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
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/core"
	"GO_Demo/go-ethereum/core/types"
	"GO_Demo/go-ethereum/eth/downloader"
	"GO_Demo/go-ethereum/eth/fetcher"
	"GO_Demo/go-ethereum/event"
	"GO_Demo/go-ethereum/logger"
	"GO_Demo/go-ethereum/logger/glog"
	"GO_Demo/go-ethereum/p2p"
	"GO_Demo/go-ethereum/pow"
	"GO_Demo/go-ethereum/rlp"
)

// This is the target maximum size of returned blocks for the
// getBlocks message. The reply message may exceed it
// if a single block is larger than the limit.
// 这是getBlocks函数返回区块的最大大小.如果单个块大于限制则回复消息可能超过该限制
const maxBlockRespSize = 2 * 1024 * 1024

func errResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

type hashFetcherFn func(common.Hash) error
type blockFetcherFn func([]common.Hash) error

// extProt is an interface which is passed around so we can expose GetHashes and GetBlock without exposing it to the rest of the protocol
// extProt is passed around to peers which require to GetHashes and GetBlocks
type extProt struct {
	getHashes hashFetcherFn  //函数变量
	getBlocks blockFetcherFn //函数变量
}

func (ep extProt) GetHashes(hash common.Hash) error    { return ep.getHashes(hash) }
func (ep extProt) GetBlock(hashes []common.Hash) error { return ep.getBlocks(hashes) }

type ProtocolManager struct {
	protVer, netId int
	txpool         txPool             //交易池处理接口
	chainman       *core.ChainManager //本地保存的区块链管理对象
	downloader     *downloader.Downloader
	fetcher        *fetcher.Fetcher
	peers          *peerSet //保存所有与本节点完成handshake的peer节点

	SubProtocols []p2p.Protocol //存放所有运行于p2p层之上的子类协议的Protocal对象

	eventMux      *event.TypeMux
	txSub         event.Subscription //交易事件
	minedBlockSub event.Subscription //矿工挖到区块事件

	// channels for fetcher, syncer, txsyncLoop
	newPeerCh chan *peer
	txsyncCh  chan *txsync
	quitSync  chan struct{}

	// wait group is used for graceful shutdowns during downloading
	// and processing
	wg   sync.WaitGroup
	quit bool
}

// NewProtocolManager returns a new ethereum sub protocol manager. The Ethereum sub protocol manages peers capable
// with the ethereum network.
func NewProtocolManager(networkId int, mux *event.TypeMux, txpool txPool, pow pow.PoW, chainman *core.ChainManager) *ProtocolManager {
	// Create the protocol manager with the base fields
	manager := &ProtocolManager{
		eventMux:  mux,
		txpool:    txpool,
		chainman:  chainman,
		peers:     newPeerSet(),
		newPeerCh: make(chan *peer, 1),
		txsyncCh:  make(chan *txsync),
		quitSync:  make(chan struct{}),
		netId:     networkId,
	}
	// Initiate a sub-protocol for every implemented version we can handle
	manager.SubProtocols = make([]p2p.Protocol, len(ProtocolVersions))
	for i := 0; i < len(manager.SubProtocols); i++ {
		version := ProtocolVersions[i]

		manager.SubProtocols[i] = p2p.Protocol{
			Name:    "eth",
			Version: version,
			Length:  ProtocolLengths[i],
			Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error { //绑定协议的Run函数
				peer := manager.newPeer(int(version), networkId, p, rw) //创建一个新的子类协议Peer对象
				manager.newPeerCh <- peer
				return manager.handle(peer) //调用协议的handle函数
			},
		}
	}
	// Construct the different synchronisation mechanisms
	// 注册downloader对象
	manager.downloader = downloader.New(manager.eventMux, manager.chainman.HasBlock, manager.chainman.GetBlock, manager.chainman.CurrentBlock, manager.chainman.InsertChain, manager.removePeer)

	validator := func(block *types.Block, parent *types.Block) error { //用于验证区块的合法性的函数
		return core.ValidateHeader(pow, block.Header(), parent, true)
	}
	heighter := func() uint64 { //用于计算当前区块链的高度的函数
		return manager.chainman.CurrentBlock().NumberU64()
	}
	//注册fetcher对象
	manager.fetcher = fetcher.New(manager.chainman.GetBlock, validator, manager.BroadcastBlock, heighter, manager.chainman.InsertChain, manager.removePeer)

	return manager
}

// 删除指定ID的Peer
func (pm *ProtocolManager) removePeer(id string) {
	// Short circuit if the peer was already removed
	peer := pm.peers.Peer(id) //查询指定ID的子类协议peer节点
	if peer == nil {
		return
	}
	glog.V(logger.Debug).Infoln("Removing peer", id)

	// Unregister the peer from the downloader and Ethereum peer set
	pm.downloader.UnregisterPeer(id)
	if err := pm.peers.Unregister(id); err != nil {
		glog.V(logger.Error).Infoln("Removal failed:", err)
	}
	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer) //关闭与此节点的P2P连接
	}
}

// 运行ProtocolManager 对象
func (pm *ProtocolManager) Start() {
	// broadcast transactions
	pm.txSub = pm.eventMux.Subscribe(core.TxPreEvent{})
	go pm.txBroadcastLoop() //检索txSub交易事件集合,完成交易消息的广播
	// broadcast mined blocks
	pm.minedBlockSub = pm.eventMux.Subscribe(core.NewMinedBlockEvent{})
	go pm.minedBroadcastLoop() //检索minedBlockSub挖矿事件集合,完成区块的广播

	// start sync handlers
	go pm.syncer()
	go pm.txsyncLoop()
}

func (pm *ProtocolManager) Stop() {
	// Showing a log message. During download / process this could actually
	// take between 5 to 10 seconds and therefor feedback is required.
	glog.V(logger.Info).Infoln("Stopping ethereum protocol handler...")

	pm.quit = true
	pm.txSub.Unsubscribe()         // quits txBroadcastLoop
	pm.minedBlockSub.Unsubscribe() // quits blockBroadcastLoop
	close(pm.quitSync)             // quits syncer, fetcher, txsyncLoop

	// Wait for any process action
	pm.wg.Wait()

	glog.V(logger.Info).Infoln("Ethereum protocol handler stopped")
}

func (pm *ProtocolManager) newPeer(pv, nv int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	return newPeer(pv, nv, p, rw)
}

// handle is the callback invoked to manage the life cycle of an eth peer. When
// this function terminates, the peer is disconnected.
func (pm *ProtocolManager) handle(p *peer) error {
	glog.V(logger.Debug).Infof("%v: peer connected [%s]", p, p.Name())

	// Execute the Ethereum handshake
	td, head, genesis := pm.chainman.Status()              //获取当前本地区块链的状态
	if err := p.Handshake(td, head, genesis); err != nil { //完成与对方节点的协议handshake
		glog.V(logger.Debug).Infof("%v: handshake failed: %v", p, err)
		return err
	}
	// Register the peer locally
	glog.V(logger.Detail).Infof("%v: adding peer", p)
	if err := pm.peers.Register(p); err != nil { //完成了与对端peer的handshake,将其注册到本地的peers集合中
		glog.V(logger.Error).Infof("%v: addition failed: %v", p, err)
		return err
	}
	defer pm.removePeer(p.id)

	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	// 将此完成handshake的peer节点注册到downloader
	if err := pm.downloader.RegisterPeer(p.id, p.version, p.Head(), p.RequestHashes, p.RequestHashesFromNumber, p.RequestBlocks); err != nil {
		return err
	}
	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.
	pm.syncTransactions(p) //将现有的交易绑定给此peer节点

	// main loop. handle incoming messages.
	// 主循环,循环等待来自上述peer节点的message消息并完成处理
	for {
		if err := pm.handleMsg(p); err != nil {
			glog.V(logger.Debug).Infof("%v: message handling failed: %v", p, err)
			return err
		}
	}
	return nil
}

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func (pm *ProtocolManager) handleMsg(p *peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Size > ProtocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch msg.Code {
	case StatusMsg:
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")

	case GetBlockHashesMsg: //1.对方peer请求检索本地保存的指定hash值的区块哈希
		// Retrieve the number of hashes to return and from which origin hash
		var request getBlockHashesData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		if request.Amount > uint64(downloader.MaxHashFetch) {
			request.Amount = uint64(downloader.MaxHashFetch)
		}
		// Retrieve the hashes from the block chain and return them
		hashes := pm.chainman.GetBlockHashesFromHash(request.Hash, request.Amount) //在本地区块链中检索对应区块的hash值
		if len(hashes) == 0 {
			glog.V(logger.Debug).Infof("invalid block hash %x", request.Hash.Bytes()[:4])
		}
		return p.SendBlockHashes(hashes) //向对方回复BlockHashesMsg消息,包含上述检索出的区块哈希值

	case GetBlockHashesFromNumberMsg: //2.对方peer请求获取从指定区块开始的amount个区块的哈希值
		// Retrieve and decode the number of hashes to return and from which origin number
		var request getBlockHashesFromNumberData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		if request.Amount > uint64(downloader.MaxHashFetch) {
			request.Amount = uint64(downloader.MaxHashFetch)
		}
		// Calculate the last block that should be retrieved, and short circuit if unavailable
		last := pm.chainman.GetBlockByNumber(request.Number + request.Amount - 1) //获取应该检索的最后一个区块
		if last == nil {                                                          //如果最后一个区块不可用,重新设定Amount值,并设定最后一个区块为本地的当前区块
			last = pm.chainman.CurrentBlock()
			request.Amount = last.NumberU64() - request.Number + 1
		}
		if last.NumberU64() < request.Number { //请求的初始区块号Number不正确(超过当前区块链的最后一个区块)
			return p.SendBlockHashes(nil)
		}
		// Retrieve the hashes from the last block backwards, reverse and return
		hashes := []common.Hash{last.Hash()}
		hashes = append(hashes, pm.chainman.GetBlockHashesFromHash(last.Hash(), request.Amount-1)...) //获取从最后一个区块向上的amount个区块的哈希值

		for i := 0; i < len(hashes)/2; i++ { //完成上述hash集合的反转
			hashes[i], hashes[len(hashes)-1-i] = hashes[len(hashes)-1-i], hashes[i]
		}
		return p.SendBlockHashes(hashes) //向对方回复BlockHashesMsg消息,包含上述检索出的Amount个区块哈希值

	case BlockHashesMsg: //3.收到对端peer的回应,包含若干区块哈希值,将它们全部交给downloader完成排序
		// A batch of hashes arrived to one of our previous requests
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		reqHashInPacketsMeter.Mark(1)

		var hashes []common.Hash
		if err := msgStream.Decode(&hashes); err != nil {
			break
		}
		reqHashInTrafficMeter.Mark(int64(32 * len(hashes)))

		// Deliver them all to the downloader for queuing
		err := pm.downloader.DeliverHashes(p.id, hashes)
		if err != nil {
			glog.V(logger.Debug).Infoln(err)
		}

	case GetBlocksMsg: //4.对端peer请求获得指定hash的区块实体(若干区块)
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather blocks until the fetch or network limits is reached
		var (
			hash   common.Hash
			bytes  common.StorageSize //计算所有区块容量之和
			hashes []common.Hash      //存储所有待检索的区块的哈希值
			blocks []*types.Block     //存储所有检索到的区块(待发送)
		)
		for {
			err := msgStream.Decode(&hash)
			if err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			hashes = append(hashes, hash)

			// Retrieve the requested block, stopping if enough was found
			if block := pm.chainman.GetBlock(hash); block != nil { //从本地链中获取指定hash的区块
				blocks = append(blocks, block)
				bytes += block.Size()
				if len(blocks) >= downloader.MaxBlockFetch || bytes > maxBlockRespSize { //必须保证返回区块的数目和总容量处于规定值之下
					break
				}
			}
		}
		if glog.V(logger.Detail) && len(blocks) == 0 && len(hashes) > 0 {
			list := "["
			for _, hash := range hashes {
				list += fmt.Sprintf("%x, ", hash[:4])
			}
			list = list[:len(list)-2] + "]"

			glog.Infof("%v: no blocks found for requested hashes %s", p, list)
		}
		return p.SendBlocks(blocks) //将区块集合回复给对端peer(BlocksMsg消息)

	case BlocksMsg: //5.收到对端peer回复的区块集合
		// Decode the arrived block message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		reqBlockInPacketsMeter.Mark(1)

		var blocks []*types.Block //存储回复消息中包含的所有区块
		if err := msgStream.Decode(&blocks); err != nil {
			glog.V(logger.Detail).Infoln("Decode error", err)
			blocks = nil
		}
		// Update the receive timestamp of each block
		// 更新每一个区块的时间戳
		for _, block := range blocks {
			reqBlockInTrafficMeter.Mark(block.Size().Int64())
			block.ReceivedAt = msg.ReceivedAt
		}
		// Filter out any explicitly requested blocks, deliver the rest to the downloader
		// 对区块集合blocks进行过滤,将更新后的blocks集合交付downloader
		if blocks := pm.fetcher.Filter(blocks); len(blocks) > 0 {
			pm.downloader.DeliverBlocks(p.id, blocks)
		}

	case NewBlockHashesMsg: //6.收到对端节点的区块可用性announce(包含新的可用区块的哈希值)
		// Retrieve and deseralize the remote new block hashes notification
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))

		var hashes []common.Hash
		if err := msgStream.Decode(&hashes); err != nil {
			break
		}
		propHashInPacketsMeter.Mark(1)
		propHashInTrafficMeter.Mark(int64(32 * len(hashes)))

		// Mark the hashes as present at the remote node
		// 将区块哈希标记为远程节点上存在
		for _, hash := range hashes {
			p.MarkBlock(hash)
			p.SetHead(hash)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make([]common.Hash, 0, len(hashes))
		for _, hash := range hashes {
			if !pm.chainman.HasBlock(hash) { //在本地检索对应区块hash值是否存在
				unknown = append(unknown, hash) //将检索不到的hash值添加到unknown集合中
			}
		}
		for _, hash := range unknown { //遍历整个unknown集合(向其他连接的peer获取unknown集合中保存的未知hash的区块)
			pm.fetcher.Notify(p.id, hash, time.Now(), p.RequestBlocks) //p.RequestBlocks发送GetBlocksMsg消息向其他peer申请获取区块
		}

	case NewBlockMsg: //7.收到对端节点发送的区块实体
		// Retrieve and decode the propagated block
		var request newBlockData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		propBlockInPacketsMeter.Mark(1)
		propBlockInTrafficMeter.Mark(request.Block.Size().Int64())

		if err := request.Block.ValidateFields(); err != nil { //首先要验证区块的合法性
			return errResp(ErrDecode, "block validation %v: %v", msg, err)
		}
		request.Block.ReceivedAt = msg.ReceivedAt //更新区块时间戳

		// Mark the block's arrival for whatever reason
		// 在日志中记录该区块
		_, chainHead, _ := pm.chainman.Status()
		jsonlogger.LogJson(&logger.EthChainReceivedNewBlock{
			BlockHash:     request.Block.Hash().Hex(),       //获取的区块的哈希
			BlockNumber:   request.Block.Number(),           //获取的区块号
			ChainHeadHash: chainHead.Hex(),                  //本地区块链的创世区块
			BlockPrevHash: request.Block.ParentHash().Hex(), //获取的区块的父区块
			RemoteId:      p.ID().String(),                  //对端peer节点的id
		})
		// Mark the peer as owning the block and schedule it for import
		p.MarkBlock(request.Block.Hash())
		p.SetHead(request.Block.Hash())

		pm.fetcher.Enqueue(p.id, request.Block) //将对方peer的标识符(p.id)与接收的区块block组装到一起(包装为inject),传输到Fetcher的inject管道

		// TODO: Schedule a sync to cover potential gaps (this needs proto update)
		if request.TD.Cmp(p.Td()) > 0 { //Msg消息中获取的节点挖矿难度累计值 > 本地保存的peer对象存储的难度累计值
			p.SetTd(request.TD)  //更新本地peer对象的难度累计值
			go pm.synchronise(p) //利用downloader完成与远端peer的同步(远端peer的td总难度必须大于本地区块链的td总难度)
		}

	case TxMsg: //8.收到对端peer的交易消息
		// Transactions arrived, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		propTxnInPacketsMeter.Mark(1)
		for i, tx := range txs { //遍历从msg消息中获取的交易集合
			// Validate and mark the remote transaction
			if tx == nil {
				return errResp(ErrDecode, "transaction %d is nil", i)
			}
			p.MarkTransaction(tx.Hash()) //在与对端节点的peer对象中更新每笔交易(因为交易从对端peer中获取,因为也就意味着这些交易对于对端peer是已知的)

			// Log it's arrival for later analysis
			propTxnInTrafficMeter.Mark(tx.Size().Int64())
			jsonlogger.LogJson(&logger.EthTxReceived{
				TxHash:   tx.Hash().Hex(),
				RemoteId: p.ID().String(),
			})
		}
		pm.txpool.AddTransactions(txs) //将txs交易集合中的交易加入到本地交易池中

	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

// BroadcastBlock will either propagate a block to a subset of it's peers, or
// will only announce it's availability (depending what's requested).
// 分为两种处理情况:
// 1.propagate == true ,分为两步 -->
// ①将形参指定的区块发送给sqrt(N)个peer节点(N为总的连接结点)  (NewBlockMsg消息)
// ②将区块的hash值广播发送给N个peer节点 (NewBlockHashesMsg消息)

// 2.propagate == true ,仅有一步 -->
// 将区块的hash值广播发送给N个peer节点 (NewBlockHashesMsg消息)
func (pm *ProtocolManager) BroadcastBlock(block *types.Block, propagate bool) {
	hash := block.Hash()                      //计算区块的哈希值
	peers := pm.peers.PeersWithoutBlock(hash) //返回所有不包含上述区块的peer节点

	// If propagation is requested, send to a subset of the peer
	// 分析请求的类型( propagate == true 和  propagate == false 两种情况)
	if propagate { //1.propagate == true
		// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
		var td *big.Int
		if parent := pm.chainman.GetBlock(block.ParentHash()); parent != nil { //获取当前要广播的区块的父区块
			td = new(big.Int).Add(parent.Td, block.Difficulty()) //父区块的td + 当前区块的挖矿难度 = 当前节点的td(记录每一个peer累计的挖矿难度)
		} else {
			glog.V(logger.Error).Infof("propagating dangling block #%d [%x]", block.NumberU64(), hash[:4])
			return
		}
		// Send the block to a subset of our peers
		transfer := peers[:int(math.Sqrt(float64(len(peers))))] //向着sqrt(N)个peer节点发送此区块,N为总的连接结点。
		for _, peer := range transfer {
			peer.SendNewBlock(block, td) //将当前区块与其td值一起发送出去(NewBlockMsg消息)
		}
		glog.V(logger.Detail).Infof("propagated block %x to %d peers in %v", hash[:4], len(transfer), time.Since(block.ReceivedAt))
	}
	// Otherwise if the block is indeed in out own chain, announce it
	// 无论 propagate == true or propagate == false 都要进行下述步骤
	// 检查本地区块链是否包含指定区块,若包含则将该区块的hash值重新广播给peers集合中全部peer对象
	if pm.chainman.HasBlock(hash) {
		for _, peer := range peers {
			peer.SendNewBlockHashes([]common.Hash{hash}) //仅发送区块的hash值(NewBlockHashesMsg消息)
		}
		glog.V(logger.Detail).Infof("announced block %x to %d peers in %v", hash[:4], len(peers), time.Since(block.ReceivedAt))
	}
}

// BroadcastTx will propagate a transaction to all peers which are not known to
// already have the given transaction.
// 广播一条交易消息(忽略所有已经存在此交易信息的节点)
func (pm *ProtocolManager) BroadcastTx(hash common.Hash, tx *types.Transaction) {
	// Broadcast transaction to a batch of peers not knowing about it
	peers := pm.peers.PeersWithoutTx(hash) //返回所有不包含hash指定的交易的peer集合(所有peer都是与当前节点已连接的)
	//FIXME include this again: peers = peers[:int(math.Sqrt(float64(len(peers))))]
	for _, peer := range peers { //向上述检索获得的每一个peer发送此交易
		peer.SendTransactions(types.Transactions{tx})
	}
	glog.V(logger.Detail).Infoln("broadcast tx to", len(peers), "peers")
}

// Mined broadcast loop
// 挖到区块的矿工需要向其他节点广播自己挖到的区块
func (self *ProtocolManager) minedBroadcastLoop() {
	// automatically stops if unsubscribe
	for obj := range self.minedBlockSub.Chan() { //遍历minedBlockSub事件集合
		switch ev := obj.(type) {
		case core.NewMinedBlockEvent: //如果是挖到区块事件
			self.BroadcastBlock(ev.Block, true)  // First propagate block to peers  首先将刚挖到的区块实体广播给所有已连接的Peer
			self.BroadcastBlock(ev.Block, false) // Only then announce to the rest  将区块的哈希值再次广播
		}
	}
}

func (self *ProtocolManager) txBroadcastLoop() {
	// automatically stops if unsubscribe
	for obj := range self.txSub.Chan() { //检索txSub集合,取出交易事件,完成对此交易消息的广播
		event := obj.(core.TxPreEvent)
		self.BroadcastTx(event.Tx.Hash(), event.Tx)
	}
}
