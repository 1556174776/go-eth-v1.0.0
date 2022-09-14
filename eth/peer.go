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
	"errors"
	"fmt"
	"math/big"
	"sync"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/core/types"
	"GO_Demo/go-ethereum/eth/downloader"
	"GO_Demo/go-ethereum/logger"
	"GO_Demo/go-ethereum/logger/glog"
	"GO_Demo/go-ethereum/p2p"
	"GO_Demo/go-ethereum/set"
)

var (
	errAlreadyRegistered = errors.New("peer is already registered")
	errNotRegistered     = errors.New("peer is not registered")
)

const (
	maxKnownTxs    = 32768 // Maximum transactions hashes to keep in the known list (prevent DOS)
	maxKnownBlocks = 1024  // Maximum block hashes to keep in the known list (prevent DOS)
)

type peer struct {
	*p2p.Peer

	rw p2p.MsgReadWriter

	version int // Protocol version negotiated
	network int // Network ID being on

	id string //对端peer的标识符

	head common.Hash //
	td   *big.Int    //当前peer通过发布区块总共累计的难度值
	lock sync.RWMutex

	knownTxs    *set.Set //此对等peer已知的交易哈希集((总是保存最新的maxKnownTxs笔交易)
	knownBlocks *set.Set //此对等方已知的区块集合(总是保存最新的maxKnownBlocks个区块)
}

func newPeer(version, network int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	id := p.ID()

	return &peer{
		Peer:        p,
		rw:          rw,
		version:     version,
		network:     network,
		id:          fmt.Sprintf("%x", id[:8]),
		knownTxs:    set.New(),
		knownBlocks: set.New(),
	}
}

// Head retrieves a copy of the current head (most recent) hash of the peer.
// 本方法负责检索对端peer当前的头部(最新的区块)哈希
func (p *peer) Head() (hash common.Hash) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	copy(hash[:], p.head[:])
	return hash
}

// SetHead updates the head (most recent) hash of the peer.
// 更新头部哈希
func (p *peer) SetHead(hash common.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()

	copy(p.head[:], hash[:])
}

// Td retrieves the current total difficulty of a peer.
// 检索当前peer节点总共的难度(通过挖矿累计的难度)
func (p *peer) Td() *big.Int {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return new(big.Int).Set(p.td)
}

// SetTd updates the current total difficulty of a peer.
// 更新td难度累计值
func (p *peer) SetTd(td *big.Int) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.td.Set(td)
}

// MarkBlock marks a block as known for the peer, ensuring that the block will
// never be propagated to this particular peer.
// 更新knownBlocks(已知区块集合),将新的区块哈希添加(保证总是存储最新的maxKnownBlocks个区块)
func (p *peer) MarkBlock(hash common.Hash) {
	// If we reached the memory allowance, drop a previously known block hash
	for p.knownBlocks.Size() >= maxKnownBlocks {
		p.knownBlocks.Pop()
	}
	p.knownBlocks.Add(hash)
}

// MarkTransaction marks a transaction as known for the peer, ensuring that it
// will never be propagated to this particular peer.
// 更新对端peer节点的knownTxs集合,保证总是存储最新的maxKnownTxs笔交易
func (p *peer) MarkTransaction(hash common.Hash) {
	// If we reached the memory allowance, drop a previously known transaction hash
	for p.knownTxs.Size() >= maxKnownTxs {
		p.knownTxs.Pop()
	}
	p.knownTxs.Add(hash)
}

// SendTransactions sends transactions to the peer and includes the hashes
// in its transaction hash set for future reference.
// 完成交易Transactions消息的发送(调用P2P Server完成,TxMsg消息),并将对应的交易消息的hash添加到对端peer的knownTxs集合中(认为此交易对与对端peer已经是已知的了)
func (p *peer) SendTransactions(txs types.Transactions) error {
	propTxnOutPacketsMeter.Mark(1)
	for _, tx := range txs {
		propTxnOutTrafficMeter.Mark(tx.Size().Int64())
		p.knownTxs.Add(tx.Hash()) //将其添加到该peer的knownTxs集合中
	}
	return p2p.Send(p.rw, TxMsg, txs) //调用P2P server模块完成交易消息的发送
}

// SendBlockHashes sends a batch of known hashes to the remote peer.
// 向对方peer节点发送区块哈希(BlockHashesMsg消息)
func (p *peer) SendBlockHashes(hashes []common.Hash) error {
	reqHashOutPacketsMeter.Mark(1)
	reqHashOutTrafficMeter.Mark(int64(32 * len(hashes)))

	return p2p.Send(p.rw, BlockHashesMsg, hashes)
}

// SendBlocks sends a batch of blocks to the remote peer.
// 向对方peer节点发送区块实体(BlocksMsg消息)
func (p *peer) SendBlocks(blocks []*types.Block) error {
	reqBlockOutPacketsMeter.Mark(1)
	for _, block := range blocks {
		reqBlockOutTrafficMeter.Mark(block.Size().Int64())
	}
	return p2p.Send(p.rw, BlocksMsg, blocks)
}

// SendNewBlockHashes announces the availability of a number of blocks through
// a hash notification.
// SendNewBlockHashes函数通过 hash值 通知宣布多个区块的可用性(向对端peer发送NewBlockHashesMsg消息)
func (p *peer) SendNewBlockHashes(hashes []common.Hash) error {
	propHashOutPacketsMeter.Mark(1)
	propHashOutTrafficMeter.Mark(int64(32 * len(hashes)))

	for _, hash := range hashes { //在knownBlocks集合中更新形参指定的区块哈希(在与对方节点的peer中标记)
		p.knownBlocks.Add(hash)
	}
	return p2p.Send(p.rw, NewBlockHashesMsg, hashes) //向对方发送NewBlockHashesMsg消息,包含新的可用区块的哈希值
}

// SendNewBlock propagates an entire block to a remote peer.
// SendNewBlock将整个区块实体以及节点累计的难度值发送到对端peer(发送NewBlockMsg消息)
func (p *peer) SendNewBlock(block *types.Block, td *big.Int) error {
	propBlockOutPacketsMeter.Mark(1)
	propBlockOutTrafficMeter.Mark(block.Size().Int64())

	p.knownBlocks.Add(block.Hash())                              //将区块哈希添加到knownBlocks集合
	return p2p.Send(p.rw, NewBlockMsg, []interface{}{block, td}) //发送区块与对应td到对端peer
}

// RequestHashes fetches a batch of hashes from a peer, starting at from, going
// towards the genesis block.
// 申请让对端节点检索指定的区块哈希值(发送GetBlockHashesMsg消息)
func (p *peer) RequestHashes(from common.Hash) error {
	glog.V(logger.Debug).Infof("Peer [%s] fetching hashes (%d) from %x...\n", p.id, downloader.MaxHashFetch, from[:4])
	return p2p.Send(p.rw, GetBlockHashesMsg, getBlockHashesData{from, uint64(downloader.MaxHashFetch)})
}

// RequestHashesFromNumber fetches a batch of hashes from a peer, starting at the
// requested block number, going upwards towards the genesis block.
// 申请从对端peer获取一批区块hash值,从请求的区块号from开始,向后累计count个(发送GetBlockHashesFromNumberMsg消息)
func (p *peer) RequestHashesFromNumber(from uint64, count int) error {
	glog.V(logger.Debug).Infof("Peer [%s] fetching hashes (%d) from #%d...\n", p.id, count, from)
	return p2p.Send(p.rw, GetBlockHashesFromNumberMsg, getBlockHashesFromNumberData{from, uint64(count)})
}

// RequestBlocks fetches a batch of blocks corresponding to the specified hashes.
// 向对方节点申请获取形参指定hash值的一批区块(发送GetBlocksMsg消息)
func (p *peer) RequestBlocks(hashes []common.Hash) error {
	glog.V(logger.Debug).Infof("[%s] fetching %v blocks\n", p.id, len(hashes))
	return p2p.Send(p.rw, GetBlocksMsg, hashes)
}

// Handshake executes the eth protocol handshake, negotiating version number,
// network IDs, difficulties, head and genesis blocks.
// 完成与对端peer的协议handshake
func (p *peer) Handshake(td *big.Int, head common.Hash, genesis common.Hash) error {
	// Send out own handshake in a new thread
	errc := make(chan error, 1)
	go func() {
		errc <- p2p.Send(p.rw, StatusMsg, &statusData{ //发送StatusMsg
			ProtocolVersion: uint32(p.version),
			NetworkId:       uint32(p.network),
			TD:              td,      //当前节点累计的td难度累计值
			CurrentBlock:    head,    //本地区块链的最新区块(即当前区块)哈希值
			GenesisBlock:    genesis, //本地区块链的创世区块哈希值
		})
	}()
	// In the mean time retrieve the remote status message
	msg, err := p.rw.ReadMsg() //等待接收对端peer的handshake回复
	if err != nil {
		return err
	}
	//验证回复消息的正确性
	if msg.Code != StatusMsg {
		return errResp(ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, StatusMsg)
	}
	if msg.Size > ProtocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	var status statusData
	if err := msg.Decode(&status); err != nil {
		return errResp(ErrDecode, "msg %v: %v", msg, err)
	}
	if status.GenesisBlock != genesis { //创世区块必须为同一个
		return errResp(ErrGenesisBlockMismatch, "%x (!= %x)", status.GenesisBlock, genesis)
	}
	if int(status.NetworkId) != p.network {
		return errResp(ErrNetworkIdMismatch, "%d (!= %d)", status.NetworkId, p.network)
	}
	if int(status.ProtocolVersion) != p.version { //子类协议版本必须相同
		return errResp(ErrProtocolVersionMismatch, "%d (!= %d)", status.ProtocolVersion, p.version)
	}
	// Configure the remote peer, and sanity check out handshake too
	p.td, p.head = status.TD, status.CurrentBlock //从回复消息中获取td难度累计值和最新区块哈希,用于更新peer对象
	return <-errc
}

// String implements fmt.Stringer.
func (p *peer) String() string {
	return fmt.Sprintf("Peer %s [%s]", p.id,
		fmt.Sprintf("eth/%2d", p.version),
	)
}

// peerSet represents the collection of active peers currently participating in
// the Ethereum sub-protocol.
// peerSet存储着当前参与以太坊子协议的活动对等体peer的集合。
type peerSet struct {
	peers map[string]*peer //key为对端peer的id(p.id)
	lock  sync.RWMutex
}

// newPeerSet creates a new peer set to track the active participants.
// 创建一个新的peerSet对象
func newPeerSet() *peerSet {
	return &peerSet{
		peers: make(map[string]*peer),
	}
}

// Register injects a new peer into the working set, or returns an error if the
// peer is already known.
// 向peerSet集合中注册一个新的peer节点(key == p.id , value == p)
func (ps *peerSet) Register(p *peer) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if _, ok := ps.peers[p.id]; ok {
		return errAlreadyRegistered
	}
	ps.peers[p.id] = p
	return nil
}

// Unregister removes a remote peer from the active set, disabling any further
// actions to/from that particular entity.
// 从peerSet集合删除一个指定id的peer节点
func (ps *peerSet) Unregister(id string) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if _, ok := ps.peers[id]; !ok {
		return errNotRegistered
	}
	delete(ps.peers, id)
	return nil
}

// Peer retrieves the registered peer with the given id.
// 根据给定的id检索出peer节点对象
func (ps *peerSet) Peer(id string) *peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return ps.peers[id]
}

// Len returns if the current number of peers in the set.
// 计算peerSet集合容纳的peer节点数
func (ps *peerSet) Len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.peers)
}

// PeersWithoutBlock retrieves a list of peers that do not have a given block in
// their set of known hashes.
// 检索出不包含给定区块哈希值的已连接peer组成的集合
func (ps *peerSet) PeersWithoutBlock(hash common.Hash) []*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		if !p.knownBlocks.Has(hash) {
			list = append(list, p)
		}
	}
	return list
}

// PeersWithoutTx retrieves a list of peers that do not have a given transaction
// in their set of known hashes.
// 检索出不包含给定交易哈希值的已连接pee组成的集合
func (ps *peerSet) PeersWithoutTx(hash common.Hash) []*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		if !p.knownTxs.Has(hash) { //如果当前peer确实不包含指定的交易哈希值,将其添加到list列表中
			list = append(list, p)
		}
	}
	return list //list列表包含所有不含形参指定交易的所有已连接peer节点
}

// BestPeer retrieves the known peer with the currently highest total difficulty.
// 在所有的已连接peer集合中检索出具有最大td(难度累计值)的peer节点作为返回值
func (ps *peerSet) BestPeer() *peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var (
		bestPeer *peer
		bestTd   *big.Int
	)
	for _, p := range ps.peers { //遍历所有与本节点存在以太坊子类协议连接的peer节点对象
		if td := p.Td(); bestPeer == nil || td.Cmp(bestTd) > 0 { //找出具有最大td(难度累计值)的peer节点对象
			bestPeer, bestTd = p, td
		}
	}
	return bestPeer
}
