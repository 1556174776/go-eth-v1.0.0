// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"math/rand"
	"time"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/core/types"
	"GO_Demo/go-ethereum/logger"
	"GO_Demo/go-ethereum/logger/glog"
	"GO_Demo/go-ethereum/p2p/discover"
)

const (
	forceSyncCycle      = 10 * time.Second // Time interval to force syncs, even if few peers are available
	minDesiredPeerCount = 5                // Amount of peers desired to start syncing

	// This is the target size for the packs of transactions sent by txsyncLoop.
	// A pack can get larger than this if a single transactions exceeds this size.
	txsyncPackSize = 100 * 1024
)

type txsync struct {
	p   *peer
	txs []*types.Transaction
}

// syncTransactions starts sending all currently pending transactions to the given peer.
// 为传入的peer对象绑定当前交易池中所有可供处理的交易,产生一条txsync消息
func (pm *ProtocolManager) syncTransactions(p *peer) {
	txs := pm.txpool.GetTransactions() //获取当前交易池可处理的交易集合(切片)
	if len(txs) == 0 {
		return
	}
	select {
	case pm.txsyncCh <- &txsync{p, txs}: //将交易集合与对等peer关联,产生一条txsync消息
	case <-pm.quitSync:
	}
}

// txsyncLoop takes care of the initial transaction sync for each new
// connection. When a new peer appears, we relay all currently pending
// transactions. In order to minimise egress bandwidth usage, we send
// the transactions in small packs to one peer at a time.
// 循环等待pm.txsyncCh管道产生的txsync消息(包含需要发送给对端peer的交易集合),然后调用send()向对端peer发送此txsync消息
func (pm *ProtocolManager) txsyncLoop() {
	var (
		pending = make(map[discover.NodeID]*txsync) //记录与其他对等peer的txsync对象
		sending = false                             // whether a send is active
		pack    = new(txsync)                       // the pack that is being sent   包含需要发送的交易集合
		done    = make(chan error, 1)               // result of the send
	)

	//将传入的txsync消息中的交易集合发送给指定的peer对象,同时将对应的txsync从pending队列中删除
	send := func(s *txsync) {
		size := common.StorageSize(0) //记录形参给定的txsync消息中的交易条数
		pack.p = s.p                  //设置目标peer节点
		pack.txs = pack.txs[:0]
		for i := 0; i < len(s.txs) && size < txsyncPackSize; i++ {
			pack.txs = append(pack.txs, s.txs[i]) //填充packet包的交易集合(将形参指定的txsync消息中的全部交易加入)
			size += s.txs[i].Size()
		}
		s.txs = s.txs[:copy(s.txs, s.txs[len(pack.txs):])] //删除已经被加入到packet包中的交易
		if len(s.txs) == 0 {
			delete(pending, s.p.ID()) //如果指定txsync对象中的全部交易都已被发送，将其从pending队列中删除
		}
		// Send the pack in the background.
		glog.V(logger.Detail).Infof("%v: sending %d transactions (%v)", s.p.Peer, len(pack.txs), size)
		sending = true
		go func() { done <- pack.p.SendTransactions(pack.txs) }() //将对应的packet包发送给相应的peer对象
	}

	// 随机的从当前pending队列中获取一个txsync消息作为返回值
	pick := func() *txsync {
		if len(pending) == 0 {
			return nil
		}
		n := rand.Intn(len(pending)) + 1
		for _, s := range pending {
			if n--; n == 0 {
				return s
			}
		}
		return nil
	}

	//循环办理业务：
	//1.第一次发送packet,需要从pm.txsyncCh管道中获取一个txsync消息,调用send()函数向txsync消息指定的peer节点发送交易集合
	//2.后续发送packet.由于每次发送packet都会向done管道发送信号,因此后续每当检测到done管道有信号则进行下一次packet发送
	for {
		select {
		case s := <-pm.txsyncCh: //从管道txsyncCh获取一个txsync消息
			pending[s.p.ID()] = s //将txsync对象加入到当前函数的pending队列
			if !sending {         //如果目前尚未发送packet
				send(s) //则将上述txsync消息包含的交易集合发送指定的peer
			}
		case err := <-done: //上次发送已完成(SendTransactions执行完毕)
			sending = false //重置sending标志位
			// Stop tracking peers that cause send failures.
			if err != nil {
				glog.V(logger.Debug).Infof("%v: tx send failed: %v", pack.p.Peer, err)
				delete(pending, pack.p.ID())
			}
			// Schedule the next send.
			if s := pick(); s != nil { //从pending队列中再次随机获取一个txsync消息
				send(s) //发送packet
			}
		case <-pm.quitSync:
			return
		}
	}
}

// syncer is responsible for periodically synchronising with the network, both
// downloading hashes and blocks as well as handling the announcement handler.
func (pm *ProtocolManager) syncer() {
	// Start and ensure cleanup of sync mechanisms
	pm.fetcher.Start() //启动fetcher
	defer pm.fetcher.Stop()
	defer pm.downloader.Terminate()

	// Wait for different events to fire synchronisation operations
	forceSync := time.Tick(forceSyncCycle)
	for {
		select {
		case <-pm.newPeerCh: //完成了与新peer节点的协议handshake
			// Make sure we have peers to select from, then sync
			if pm.peers.Len() < minDesiredPeerCount { //确保有足够多的已连接peer可供选择,然后才能开始同步
				break
			}
			go pm.synchronise(pm.peers.BestPeer()) //尝试将本地区块链与远程对等peer同步
		case <-forceSync: //当forceSync计时器到达计时周期,即使没有足够的对等点，也强制同步
			go pm.synchronise(pm.peers.BestPeer()) //尝试将本地区块链与远程对等peer同步

		case <-pm.quitSync:
			return
		}
	}
}

// 尝试将本地区块链与远程对等peer同步
func (pm *ProtocolManager) synchronise(peer *peer) {
	// Short circuit if no peers are available
	if peer == nil {
		return
	}
	// Make sure the peer's TD is higher than our own. If not drop.
	// 要确保对端peer的TD难度累计值 > 当前本地区块链的td难度累计值(否则证明对端peer没有挖出新的区块)
	if peer.Td().Cmp(pm.chainman.Td()) <= 0 {
		return //小于等于,直接退出,不进行同步(如果相等表示没有变化,因此不需要进行同步;小于则需要由对方peer节点向本节点申请进行同步)
	}
	// Otherwise try to sync with the downloader
	// 利用downloader完成同步
	pm.downloader.Synchronise(peer.id, peer.Head(), peer.Td())
}
