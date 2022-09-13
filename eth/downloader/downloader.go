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

// Package downloader contains the manual full chain synchronisation.
package downloader

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/core"
	"GO_Demo/go-ethereum/core/types"
	"GO_Demo/go-ethereum/event"
	"GO_Demo/go-ethereum/logger"
	"GO_Demo/go-ethereum/logger/glog"

	"GO_Demo/go-ethereum/set"
)

const (
	eth60 = 60 // Constant to check for old protocol support
	eth61 = 61 // Constant to check for new protocol support
)

var (
	MinHashFetch  = 512 // Minimum amount of hashes to not consider a peer stalling
	MaxHashFetch  = 512 // Amount of hashes to be fetched per retrieval request
	MaxBlockFetch = 128 // Amount of blocks to be fetched per retrieval request

	hashTTL         = 5 * time.Second  // Time it takes for a hash request to time out
	blockSoftTTL    = 3 * time.Second  // Request completion threshold for increasing or decreasing a peer's bandwidth
	blockHardTTL    = 3 * blockSoftTTL // Maximum time allowance before a block request is considered expired
	crossCheckCycle = time.Second      // Period after which to check for expired cross checks

	maxQueuedHashes = 256 * 1024 // Maximum number of hashes to queue for import (DOS protection)
	maxBannedHashes = 4096       // Number of bannable hashes before phasing old ones out
	maxBlockProcess = 256        // Number of blocks to import at once into the chain
)

var (
	errBusy             = errors.New("busy")
	errUnknownPeer      = errors.New("peer is unknown or unhealthy")
	errBadPeer          = errors.New("action from bad peer ignored")
	errStallingPeer     = errors.New("peer is stalling")
	errBannedHead       = errors.New("peer head hash already banned")
	errNoPeers          = errors.New("no peers to keep download active")
	errPendingQueue     = errors.New("pending items in queue")
	errTimeout          = errors.New("timeout")
	errEmptyHashSet     = errors.New("empty hash set by peer")
	errPeersUnavailable = errors.New("no peers available or all peers tried for block download process")
	errAlreadyInPool    = errors.New("hash already in pool")
	errInvalidChain     = errors.New("retrieved hash chain is invalid")
	errCrossCheckFailed = errors.New("block cross-check failed")
	errCancelHashFetch  = errors.New("hash fetching canceled (requested)")
	errCancelBlockFetch = errors.New("block downloading canceled (requested)")
	errNoSyncActive     = errors.New("no sync active")
)

// hashCheckFn is a callback type for verifying a hash's presence in the local chain.
// 验证某一区块hash是否存在于本地区块链
type hashCheckFn func(common.Hash) bool

// blockRetrievalFn is a callback type for retrieving a block from the local chain.
// 在本地区块链上检索某一区块
type blockRetrievalFn func(common.Hash) *types.Block

// headRetrievalFn is a callback type for retrieving the head block from the local chain.
// 检索获取本地区块链的创世区块
type headRetrievalFn func() *types.Block

// chainInsertFn is a callback type to insert a batch of blocks into the local chain.
// 向本地区块链中插入一批新的区块
type chainInsertFn func(types.Blocks) (int, error)

// peerDropFn is a callback type for dropping a peer detected as malicious.
// 删除被检测为恶意节点的peer
type peerDropFn func(id string)

// 记录从对端peer获取的区块集合
type blockPack struct {
	peerId string         //peer节点标识符
	blocks []*types.Block //获取的区块集合
}

// 记录从对端peer获取的区块hash集合
type hashPack struct {
	peerId string        //peer节点标识符
	hashes []common.Hash //获取的区块哈希集合
}

// 包含用于验证某一区块的信息
type crossCheck struct {
	expire time.Time   //过期时间
	parent common.Hash //父区块哈希
}

type Downloader struct {
	mux *event.TypeMux

	queue  *queue                      // Scheduler for selecting the hashes to download   需下载获取的区块hash组成的有序队列
	peers  *peerSet                    // Set of active peers from which download can proceed  可以下载区块的活跃对等节点组成的集合
	checks map[common.Hash]*crossCheck // Pending cross checks to verify a hash chain  key值为需验证的区块的hash,value值是验证所需的信息
	banned *set.Set                    // Set of hashes we've received and banned   存放所有被通知禁止的区块hash

	interrupt int32 // Atomic boolean to signal termination

	// Statistics
	importStart time.Time // Instance when the last blocks were taken from the cache  从BlockCache中获取最后一个区块时的时间点
	importQueue []*Block  // Previously taken blocks to check import progress   已经从其他peer处下载获取的区块组成的集合(其中的区块可能已经被添加到本地区块链上,也可能正在等待添加)
	importDone  int       // Number of taken blocks already imported from the last batch   记录当前importQueue集合中已经被添加到本地区块链的区块数目
	importLock  sync.Mutex

	// Callbacks
	hasBlock    hashCheckFn      // Checks if a block is present in the chain   检查区块是否存在于本地区块链
	getBlock    blockRetrievalFn // Retrieves a block from the chain   从本地区块链获取指定hash的区块
	headBlock   headRetrievalFn  // Retrieves the head block from the chain   获取本地区块链的创世区块
	insertChain chainInsertFn    // Injects a batch of blocks into the chain   向本地区块链中插入一批新区块
	dropPeer    peerDropFn       // Drops a peer for misbehaving    删除指定p.id的恶意节点

	// Status
	synchroniseMock func(id string, hash common.Hash) error // Replacement for synchronise during testing
	synchronising   int32                                   //是否正在进行区块同步的标志位(==1表示正在同步)
	processing      int32                                   //是否正在将区块加入本地区块链的标志位(==1表示正在进行)
	notified        int32

	// Channels
	newPeerCh chan *peer
	hashCh    chan hashPack  // Channel receiving inbound hashes
	blockCh   chan blockPack // Channel receiving inbound blocks
	processCh chan bool      // Channel to signal the block fetcher of new or finished work

	cancelCh   chan struct{} // Channel to cancel mid-flight syncs
	cancelLock sync.RWMutex  // Lock to protect the cancel channel in delivers
}

// Block is an origin-tagged blockchain block.
type Block struct {
	RawBlock   *types.Block //区块实体
	OriginPeer string       //区块提供者的标识符
}

// New creates a new downloader to fetch hashes and blocks from remote peers.
// 创建一个新的downloader,用于从远端peer节点处获取hash和区块
func New(mux *event.TypeMux, hasBlock hashCheckFn, getBlock blockRetrievalFn, headBlock headRetrievalFn, insertChain chainInsertFn, dropPeer peerDropFn) *Downloader {
	// Create the base downloader
	downloader := &Downloader{
		mux:         mux,
		queue:       newQueue(),
		peers:       newPeerSet(),
		hasBlock:    hasBlock,
		getBlock:    getBlock,
		headBlock:   headBlock,
		insertChain: insertChain,
		dropPeer:    dropPeer,
		newPeerCh:   make(chan *peer, 1),
		hashCh:      make(chan hashPack, 1),
		blockCh:     make(chan blockPack, 1),
		processCh:   make(chan bool, 1),
	}
	// Inject all the known bad hashes
	// 向downloader.banned中注入所有已知的错误哈希
	downloader.banned = set.New()
	for hash, _ := range core.BadHashes {
		downloader.banned.Add(hash)
	}
	return downloader
}

// Stats retrieves the current status of the downloader.
// 检索获取当前downloader的各种状态变量
// 1.pending和cached为当前d.queue等待队列的长度
// 2.importing为d.importQueue区块队列中尚未加载到本地区块链的区块数目
// 3.estimate为完成本次区块链总同步所需时间的预估计值
func (d *Downloader) Stats() (pending int, cached int, importing int, estimate time.Duration) {
	// Fetch the download status
	pending, cached = d.queue.Size()

	// Figure out the import progress
	d.importLock.Lock()
	defer d.importLock.Unlock()

	// 根据当前d.importQueue中保存的区块已经在本地区块链中的存在数目获取d.importDone
	for len(d.importQueue) > 0 && d.hasBlock(d.importQueue[0].RawBlock.Hash()) {
		d.importQueue = d.importQueue[1:] //d.importQueue队列的首指针每次循环都向后移动1位
		d.importDone++                    //每当发现有一个区块存在于本地区块链时,d.importDone+1
	}
	importing = len(d.importQueue) //尚未导入到本地区块链的已下载区块数目

	// Make an estimate on the total sync
	// 对总的同步所需时间进行一个估计
	estimate = 0
	if d.importDone > 0 {
		estimate = time.Since(d.importStart) / time.Duration(d.importDone) * time.Duration(pending+cached+importing)
	}
	return
}

// Synchronising returns whether the downloader is currently retrieving blocks.
// 检查downloader是否正在检索区块中
func (d *Downloader) Synchronising() bool {
	return atomic.LoadInt32(&d.synchronising) > 0
}

// RegisterPeer injects a new download peer into the set of block source to be
// used for fetching hashes and blocks from.
// 将新的peer节点注入到peerSet集合中
func (d *Downloader) RegisterPeer(id string, version int, head common.Hash, getRelHashes relativeHashFetcherFn, getAbsHashes absoluteHashFetcherFn, getBlocks blockFetcherFn) error {
	// If the peer wants to send a banned hash, reject
	if d.banned.Has(head) {
		glog.V(logger.Debug).Infoln("Register rejected, head hash banned:", id)
		return errBannedHead
	}
	// Otherwise try to construct and register the peer
	glog.V(logger.Detail).Infoln("Registering peer", id)
	if err := d.peers.Register(newPeer(id, version, head, getRelHashes, getAbsHashes, getBlocks)); err != nil {
		glog.V(logger.Error).Infoln("Register failed:", err)
		return err
	}
	return nil
}

// UnregisterPeer remove a peer from the known list, preventing any action from
// the specified peer.
// 将指定p.id的peer节点从peerSet集合中删除
func (d *Downloader) UnregisterPeer(id string) error {
	glog.V(logger.Detail).Infoln("Unregistering peer", id)
	if err := d.peers.Unregister(id); err != nil {
		glog.V(logger.Error).Infoln("Unregister failed:", err)
		return err
	}
	return nil
}

// Synchronise tries to sync up our local block chain with a remote peer, both
// adding various sanity checks as well as wrapping it with various log entries.
func (d *Downloader) Synchronise(id string, head common.Hash, td *big.Int) {
	glog.V(logger.Detail).Infof("Attempting synchronisation: %v, head 0x%x, TD %v", id, head[:4], td)

	switch err := d.synchronise(id, head, td); err {
	case nil:
		glog.V(logger.Detail).Infof("Synchronisation completed")

	case errBusy:
		glog.V(logger.Detail).Infof("Synchronisation already in progress")

	case errTimeout, errBadPeer, errStallingPeer, errBannedHead, errEmptyHashSet, errPeersUnavailable, errInvalidChain, errCrossCheckFailed:
		glog.V(logger.Debug).Infof("Removing peer %v: %v", id, err)
		d.dropPeer(id)

	case errPendingQueue:
		glog.V(logger.Debug).Infoln("Synchronisation aborted:", err)

	default:
		glog.V(logger.Warn).Infof("Synchronisation failed: %v", err)
	}
}

// synchronise will select the peer and use it for synchronising. If an empty string is given
// it will use the best peer possible and synchronize if it's TD is higher than our own. If any of the
// checks fail an error will be returned. This method is synchronous
func (d *Downloader) synchronise(id string, hash common.Hash, td *big.Int) error {
	// Mock out the synchonisation if testing
	if d.synchroniseMock != nil { //如果设置了模拟同步方法,则调用
		return d.synchroniseMock(id, hash)
	}
	// Make sure only one goroutine is ever allowed past this point at once
	if !atomic.CompareAndSwapInt32(&d.synchronising, 0, 1) { //确保一次最多只有一个goroutine在进行区块同步
		return errBusy
	}
	defer atomic.StoreInt32(&d.synchronising, 0) //完成同步后,将d.synchronising重新设置为0

	// If the head hash is banned, terminate immediately
	if d.banned.Has(hash) { //如果检测到要进行同步的区块hash是被禁止的,立即停止并退出
		return errBannedHead
	}
	// Post a user notification of the sync (only once per session)
	if atomic.CompareAndSwapInt32(&d.notified, 0, 1) { //发布用户同步通知(每个会话仅一次)
		glog.V(logger.Info).Infoln("Block synchronisation started")
	}
	// Abort if the queue still contains some leftover data
	if _, cached := d.queue.Size(); cached > 0 && d.queue.GetHeadBlock() != nil { //如果在q.blockPool缓存中尚存在一些剩余为提取的区块,则立即终止本次同步
		return errPendingQueue
	}
	// Reset the queue and peer set to clean any internal leftover state
	d.queue.Reset() //初始化本地保存的本次同步所用到的区块信息
	d.peers.Reset() //初始化peerSet节点集合中各个peer对象的状态(1.置位闲置状态  2.区块下载容量置位1)
	d.checks = make(map[common.Hash]*crossCheck)

	// Create cancel channel for aborting mid-flight
	d.cancelLock.Lock()
	d.cancelCh = make(chan struct{})
	d.cancelLock.Unlock()

	// Retrieve the origin peer and initiate the downloading process
	p := d.peers.Peer(id) //在peerSet集合中检索目标peer节点
	if p == nil {
		return errUnknownPeer
	}
	return d.syncWithPeer(p, hash, td) //与该peer节点完成区块链同步
}

// Has checks if the downloader knows about a particular hash, meaning that its
// either already downloaded of pending retrieval.
func (d *Downloader) Has(hash common.Hash) bool {
	return d.queue.Has(hash)
}

// syncWithPeer starts a block synchronization based on the hash chain from the
// specified peer and head hash.
func (d *Downloader) syncWithPeer(p *peer, hash common.Hash, td *big.Int) (err error) {
	d.mux.Post(StartEvent{}) //产生一个同步开始事件
	defer func() {
		// reset on error
		if err != nil {
			d.cancel()
			d.mux.Post(FailedEvent{err}) //若同步过程发生错误,产生一个错误事件
		} else {
			d.mux.Post(DoneEvent{}) //若同步完成,产生一个结束事件
		}
	}()

	glog.V(logger.Debug).Infof("Synchronizing with the network using: %s, eth/%d", p.id, p.version)
	switch p.version { //判断对方使用的子类协议版本号
	case eth60:
		// Old eth/60 version, use reverse hash retrieval algorithm
		// eth60版本,使用反向哈希检索算法
		if err = d.fetchHashes60(p, hash); err != nil { //向对端申请获取区块hash值,并申请获取相应区块
			return err
		}
		if err = d.fetchBlocks60(); err != nil { //等待对端回复的区块集合
			return err
		}
	case eth61:
		// New eth/61, use forward, concurrent hash and block retrieval algorithm
		// 新的eth/61版本,使用前向、并发哈希和区块检索算法
		number, err := d.findAncestor(p)
		if err != nil {
			return err
		}
		errc := make(chan error, 2)
		go func() { errc <- d.fetchHashes(p, td, number+1) }()
		go func() { errc <- d.fetchBlocks(number + 1) }()

		// If any fetcher fails, cancel the other
		if err := <-errc; err != nil {
			d.cancel()
			<-errc
			return err
		}
		return <-errc

	default:
		// Something very wrong, stop right here
		glog.V(logger.Error).Infof("Unsupported eth protocol: %d", p.version)
		return errBadPeer
	}
	glog.V(logger.Debug).Infoln("Synchronization completed")

	return nil
}

// cancel cancels all of the operations and resets the queue. It returns true
// if the cancel operation was completed.
// 关闭d.cancelCh管道，停止downloader的所有操作,同时重置downloader的queue队列
func (d *Downloader) cancel() {
	// Close the current cancel channel
	d.cancelLock.Lock()
	if d.cancelCh != nil {
		select {
		case <-d.cancelCh:
			// Channel was already closed
		default:
			close(d.cancelCh)
		}
	}
	d.cancelLock.Unlock()

	// Reset the queue
	d.queue.Reset()
}

// Terminate interrupts the downloader, canceling all pending operations.
func (d *Downloader) Terminate() {
	atomic.StoreInt32(&d.interrupt, 1)
	d.cancel()
}

// fetchHashes60 starts retrieving hashes backwards from a specific peer and hash,
// up until it finds a common ancestor. If the source peer times out, alternative
// ones are tried for continuation.
// 向目标peer节点详情获取形参指定hash相关的一系列区块hash,然后for循环等待对端peer的回复
// 1.接收到对端peer的BlockHashesMsg消息(对上述的getHashes(h)的回应),然后向对端peer申请获取这些区块(active.getBlocks([]common.Hash{origin}))
// 2.接收到对端peer的BlocksMsg消息(对上述的active.getBlocks([]common.Hash{origin}))的回应,完成了在步骤1中区块的的cross check
// 3.在crossTicker计时器计时周期结束时,必须完成对所有申请区块的cross check(如果存在任何一个区块未接收到或者合理性验证失败,则代表整个验证失败)
// 4.如果对端peer没能在timeout计时器计时结束前,完成对active.getRelHashes(from)操作的回复,那么会选择一个新的peer节点作为申请目标发起getHashes操作
func (d *Downloader) fetchHashes60(p *peer, h common.Hash) error {
	var (
		start  = time.Now()
		active = p             // active peer will help determine the current active peer
		head   = common.Hash{} // common and last hash

		timeout     = time.NewTimer(0)                // timer to dump a non-responsive active peer  active节点必须在timeout定时周期内返回对区块hash申请的回复,否则active节点将会被更换
		attempted   = make(map[string]bool)           // attempted peers will help with retries  所有曾经作为active节点的peer的p.id都会被记录
		crossTicker = time.NewTicker(crossCheckCycle) // ticker to periodically check expired cross checks  在crossTicker计时器计时周期内,必须完成本次d.checks中所有的区块检查操作
	)
	defer crossTicker.Stop()
	defer timeout.Stop()

	glog.V(logger.Debug).Infof("Downloading hashes (%x) from %s", h[:4], p.id)
	<-timeout.C // timeout channel should be initially empty.

	getHashes := func(from common.Hash) { //负责向活跃节点active申请获得有关from的一批区块哈希
		go active.getRelHashes(from)
		timeout.Reset(hashTTL) //更改timeout定时器的定时周期为hashTTL
	}

	// Add the hash to the queue, and start hash retrieval.
	d.queue.Insert([]common.Hash{h}, false) //将形参给定的区块hash插入到q.hashQueue(区块申请等待队列)相应位置处
	getHashes(h)                            //调用函数变量,向目标节点申请获取此区块hash

	attempted[p.id] = true
	for finished := false; !finished; {
		select {
		case <-d.cancelCh:
			return errCancelHashFetch

		case hashPack := <-d.hashCh: //接收到对端peer的BlockHashesMsg消息,获取了getHashes(h)的回应区块hash集合
			// Make sure the active peer is giving us the hashes
			if hashPack.peerId != active.id { //确保BlockHashesMsg消息的发送方peer是我们请求的对象
				glog.V(logger.Debug).Infof("Received hashes from incorrect peer(%s)", hashPack.peerId)
				break
			}
			timeout.Stop() //暂时停止timeout定时器

			// Make sure the peer actually gave something valid
			if len(hashPack.hashes) == 0 { //确保BlockHashesMsg消息中包含了有效hash
				glog.V(logger.Debug).Infof("Peer (%s) responded with empty hash set", active.id)
				return errEmptyHashSet
			}
			for index, hash := range hashPack.hashes { //遍历hashPack包中的区块hash(负责更新Downloader对象的banned集合)
				if d.banned.Has(hash) { //判断该hash是否是被禁止的区块的hash
					glog.V(logger.Debug).Infof("Peer (%s) sent a known invalid chain", active.id)

					d.queue.Insert(hashPack.hashes[:index+1], false) //将集合中被ban的hash前的hash插入到q.hashQueue中(由于区块链的性质,父区块出错,后方的所有区块必定是错误的,因此必须重新获取)
					if err := d.banBlocks(active.id, hash); err != nil {
						glog.V(logger.Debug).Infof("Failed to ban batch of blocks: %v", err)
					}
					return errInvalidChain
				}
			}
			// Determine if we're done fetching hashes (queue up all pending), and continue if not done
			done, index := false, 0
			for index, head = range hashPack.hashes { //hashPack.hashes中存储的区块hash的顺序是逆向的(父区块hash在本区块hash的后面)
				if d.hasBlock(head) || d.queue.GetBlock(head) != nil { //如果本地区块链中已经获取该hash的区块,或者q.blockCache已经存储该区块
					glog.V(logger.Debug).Infof("Found common hash %x", head[:4])
					hashPack.hashes = hashPack.hashes[:index] //hashPack.hashes保存已存在区块之前的所有hash
					done = true
					break
				}
			}
			// Insert all the new hashes, but only continue if got something useful
			inserts := d.queue.Insert(hashPack.hashes, false) //将hashPack.hashes保存的区块hash插入到q.hashQueue
			if len(inserts) == 0 && !done {                   //说明本次收到的对端peer的BlockHashesMsg消息是过时的(len(inserts) == 0说明没有任何新hash)
				glog.V(logger.Debug).Infof("Peer (%s) responded with stale hashes", active.id)
				return errBadPeer
			}
			if !done { //保证接收到的BlockHashesMsg消息中保存的区块hash在本地区块链和q.blockCache中不存在区块实体的记录(否则没有再次获取的必要)
				// Check that the peer is not stalling the sync
				if len(inserts) < MinHashFetch {
					return errStallingPeer
				}
				// Try and fetch a random block to verify the hash batch
				// Skip the last hash as the cross check races with the next hash fetch
				cross := rand.Intn(len(inserts) - 1)
				origin, parent := inserts[cross], inserts[cross+1] //从insert集合中随机选择两个连续的区块哈希(区块与其父区块)
				glog.V(logger.Detail).Infof("Cross checking (%s) with %x/%x", active.id, origin, parent)

				d.checks[origin] = &crossCheck{ //组装一个crossCheck区块检查消息(用于对后续收到的对端peer回复的区块进行区块合法性验证)
					expire: time.Now().Add(blockSoftTTL),
					parent: parent,
				}
				go active.getBlocks([]common.Hash{origin}) //调用协程向对端peer申请获取该区块实体

				// Also fetch a fresh batch of hashes
				getHashes(head) //重新获取一批区块hash值(此处的head为for index, head = range hashPack.hashes循环结束时的区块hash,之后的区块都是已经存在于本地区块链或者q.blockCache中)
				continue        //重新等待对端peer的回复
			}
			// We're done, prepare the download cache and proceed pulling the blocks
			// 成功从对方peer获取了全部或部分区块(done == true)
			offset := uint64(0)
			if block := d.getBlock(head); block != nil { //从本地区块链中提取获取区块集合中的最后一个区块
				offset = block.NumberU64() + 1 //变更区块链偏移值
			}
			d.queue.Prepare(offset) //重新配置q.blockOffset偏移量
			finished = true

		case blockPack := <-d.blockCh: //接收到对端peer的BlocksMsg消息,获取了active.getBlocks([]common.Hash{origin})的回复
			// Cross check the block with the random verifications
			if blockPack.peerId != active.id || len(blockPack.blocks) != 1 { //检查blockPack来源是否正确,同时每条blockPack包必须包含两个区块(一对父子区块)
				continue
			}
			block := blockPack.blocks[0]
			if check, ok := d.checks[block.Hash()]; ok { //对所有获取到的区块进行验证(验证前后连续两区块是否为父子区块)
				if block.ParentHash() != check.parent {
					return errCrossCheckFailed
				}
				delete(d.checks, block.Hash()) //完成本次验证,将对应子区块的哈希从d.checks中删除(父区块不删除,因为父区块还没有通过父父区块完成合法性验证)
			}

		case <-crossTicker.C: //在crossTicker计时器计时周期内,必须完成本次d.checks中所有的区块检查操作
			// Iterate over all the cross checks and fail the hash chain if they're not verified
			for hash, check := range d.checks { //遍历 d.checks 所有待检查的区块
				if time.Now().After(check.expire) { //查看此检查操作是否已经过期
					glog.V(logger.Debug).Infof("Cross check timeout for %x", hash)
					return errCrossCheckFailed //一旦存在过期未完成的区块检查,则认为整个过程都失败了
				}
			}

		case <-timeout.C: //对端peer必须在timeout计时器计时周期内,返回对active.getRelHashes(from)操作的回复,否则将执行以下行为(重新获取区块hash)
			glog.V(logger.Debug).Infof("Peer (%s) didn't respond in time for hash request", p.id)

			var p *peer // p will be set if a peer can be found
			// Attempt to find a new peer by checking inclusion of peers best hash in our
			// already fetched hash list. This can't guarantee 100% correctness but does
			// a fair job. This is always either correct or false incorrect.
			for _, peer := range d.peers.AllPeers() { //遍历peerSet中存储的全部peer对象
				//检查当前downloader的queue队列是否存在此peer节点的最新区块hash,同时检查是否已经向该节点发送给区块hash获取申请
				if d.queue.Has(peer.head) && !attempted[peer.id] {
					p = peer //将p设定为符合上述条件的新的peer节点
					break
				}
			}
			// if all peers have been tried, abort the process entirely or if the hash is
			// the zero hash.
			if p == nil || (head == common.Hash{}) {
				return errTimeout
			}
			// set p to the active peer. this will invalidate any hashes that may be returned
			// by our previous (delayed) peer.
			active = p      //将上述获取的新peer节点设置为新的活动节点,这将使上一个（延迟的）对等方可能返回的任何哈希无效化。
			getHashes(head) //向新peer节点申请获取其最新区块的hash
			glog.V(logger.Debug).Infof("Hash fetching switched to new peer(%s)", p.id)
		}
	}
	glog.V(logger.Debug).Infof("Downloaded hashes (%d) in %v", d.queue.Pending(), time.Since(start))

	return nil
}

// fetchBlocks60 iteratively downloads the entire schedules block-chain, taking
// any available peers, reserving a chunk of blocks for each, wait for delivery
// and periodically checking for timeouts.
// 1.持续等待对端peer节点的BlockMsg消息,将获取的结果区块注入到q.blockCache队列(并根据获取的结果重新评判对端peer的信誉值p.req)
//   然后调用d.process()将q.blockCache队列中的区块添加到本地区块链
// 2.当ticker计时器到达计数周期时,检查是否有请求超时的fetchRequest查询包(一直未能获取对方回复),将对应peer节点的信誉p.rep降级
//   从q.hashQueue中取出剩余的待获取的区块hash值，组装成fetchRequest查询包,发送给其他信誉值高的空闲peer
// 3.当q.pendPool池为空,意味着已经完成了本轮所有的fetchRequest查询,进入下一次循环
func (d *Downloader) fetchBlocks60() error {
	glog.V(logger.Debug).Infoln("Downloading", d.queue.Pending(), "block(s)")
	start := time.Now()

	// Start a ticker to continue throttled downloads and check for bad peers
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

out:
	for {
		select {
		case <-d.cancelCh:
			return errCancelBlockFetch

		case <-d.hashCh:
			// Out of bounds hashes received, ignore them

		case blockPack := <-d.blockCh: //接收到对端peer的BlockMsg消息,blockPack包含一对父子区块
			// Short circuit if it's a stale cross check
			if len(blockPack.blocks) == 1 { //检查本次是否为过时的区块cross check
				block := blockPack.blocks[0]
				if _, ok := d.checks[block.Hash()]; ok {
					delete(d.checks, block.Hash())
					break
				}
			}
			// If the peer was previously banned and failed to deliver it's pack
			// in a reasonable time frame, ignore it's message.
			if peer := d.peers.Peer(blockPack.peerId); peer != nil { //从peerSet中检索目标p.id的peer节点
				// Deliver the received chunk of blocks, and demote in case of errors
				err := d.queue.Deliver(blockPack.peerId, blockPack.blocks) //将获取的结果区块注入到q.blockCache队列
				switch err {
				case nil: //注入过程没有发生任何错误
					// If no blocks were delivered, demote the peer (need the delivery above)
					if len(blockPack.blocks) == 0 { //如果blockPack回复中未包含任何区块
						peer.Demote()  //对应节点的信誉p.rep减半
						peer.SetIdle() //重新设置p.capacity,节点重回闲置状态
						glog.V(logger.Detail).Infof("%s: no blocks delivered", peer)
						break
					}
					// 对方peer节点回应的区块(非零个)成功注入到q.blockCache队列
					peer.Promote() //对应节点的信誉p.rep + 1
					peer.SetIdle() //重新设置p.capacity,节点重回闲置状态
					glog.V(logger.Detail).Infof("%s: delivered %d blocks", peer, len(blockPack.blocks))
					go d.process() //从blockCache中取出缓存区块,将其导入本地区块链

				case errInvalidChain: //从对端peer获取的区块是不连续的,视为无效
					// The hash chain is invalid (blocks are not ordered properly), abort
					return err

				case errNoFetchesPending: //在q.pendPool中已经不存在对此peer节点发送fetcherRequest的记录(意味着此对等peer的回复超时了)
					// Peer probably timed out with its delivery but came through
					// in the end, demote, but allow to to pull from this peer.
					peer.Demote()  //对应节点的信誉p.rep减半
					peer.SetIdle() //重新设置p.capacity,节点重回闲置状态
					glog.V(logger.Detail).Infof("%s: out of bound delivery", peer)

				case errStaleDelivery: //对端peer回复的区块与本地向其请求的区块完全不一样,通常是由新旧同步周期中的超时和异步交付引起的。不能将其设置为空闲，因为原始请求仍在运行中。
					// Delivered something completely else than requested, usually
					// caused by a timeout and delivery during a new sync cycle.
					// Don't set it to idle as the original request should still be
					// in flight.
					peer.Demote() //仅对应节点的信誉p.rep减半
					glog.V(logger.Detail).Infof("%s: stale delivery", peer)

				default:
					// Peer did something semi-useful, demote but keep it around
					peer.Demote()
					peer.SetIdle()
					glog.V(logger.Detail).Infof("%s: delivery partially failed: %v", peer, err)
					go d.process()
				}
			}

		case <-ticker.C:
			// Short circuit if we lost all our peers
			if d.peers.Len() == 0 { //检查是否与其他节点失去了连接
				return errNoPeers
			}
			// Check for block request timeouts and demote the responsible peers
			// 检查请求超时的fetchRequest查询包(一直未能获取对方回复),将对应peer节点的信誉p.rep降级
			badPeers := d.queue.Expire(blockHardTTL) //清除掉q.pendPool中所有的过期fetchRequest查询包,获取这些查询包的目标节点的p.id
			for _, pid := range badPeers {           //将所有不能及时回复的peer节点的信誉值p.rep减半
				if peer := d.peers.Peer(pid); peer != nil {
					peer.Demote()
					glog.V(logger.Detail).Infof("%s: block delivery timeout", peer)
				}
			}
			// If there are unrequested hashes left start fetching from the available peers
			// 如果还剩有待请求的区块检索
			if d.queue.Pending() > 0 {
				// Throttle the download if block cache is full and waiting processing
				if d.queue.Throttle() { //如果区块缓存池q.blockPool已满并等待处理，则限制下载
					break
				}
				// 向所有空闲对等点发送下载请求，直到达到区块缓存池q.blockPool限制
				idlePeers := d.peers.IdlePeers() //获取所有的空闲peer节点(按照信誉值从大到小排序)
				for _, peer := range idlePeers {
					// Short circuit if throttling activated since above
					if d.queue.Throttle() {
						break
					}
					request := d.queue.Reserve(peer, peer.Capacity()) //从q.hashQueue中取出待获取的区块hash值，组装成fetchRequest查询包
					if request == nil {
						continue
					}
					if glog.V(logger.Detail) {
						glog.Infof("%s: requesting %d blocks", peer, len(request.Hashes))
					}
					// Fetch the chunk and check for error. If the peer was somehow
					// already fetching a chunk due to a bug, it will be returned to
					// the queue
					if err := peer.Fetch(request); err != nil { //向此空闲peer发送此fetchRequest查询包
						glog.V(logger.Error).Infof("Peer %s received double work", peer.id)
						d.queue.Cancel(request) //如果发送出错,则丢弃该fetchRequest查询包,将包中所有需查询的区块hash重新加入到q.hashQueue集合
					}
				}
				// Make sure that we have peers available for fetching. If all peers have been tried
				// and all failed throw an error
				if d.queue.InFlight() == 0 { //已经没有可用的fetchRequest查询包(q.pendPool池为空),也就是所有节点的peer.Fetch(request)都发生了错误
					return errPeersUnavailable
				}

			} else if d.queue.InFlight() == 0 { //q.pendPool池为空,已经完成了本次所有的fetchRequest查询(需要进行下一次循环,等待从q.hashQueue中取出待获取的区块hash值，组装成fetchRequest查询包,加入到q.pendPool池)
				// When there are no more queue and no more in flight, We can
				// safely assume we're done. Another part of the process will  check
				// for parent errors and will re-request anything that's missing
				break out
			}
		}
	}
	glog.V(logger.Detail).Infoln("Downloaded block(s) in", time.Since(start))
	return nil
}

// findAncestor tries to locate the common ancestor block of the local chain and
// a remote peers blockchain. In the general case when our node was in sync and
// on the correct chain, checking the top N blocks should already get us a match.
// In the rare scenario when we ended up on a long soft fork (i.e. none of the
// head blocks match), we do a binary search to find the common ancestor.
func (d *Downloader) findAncestor(p *peer) (uint64, error) {
	glog.V(logger.Debug).Infof("%v: looking for common ancestor", p)

	// Request out head blocks to short circuit ancestor location
	head := d.headBlock().NumberU64()
	from := int64(head) - int64(MaxHashFetch)
	if from < 0 {
		from = 0
	}
	go p.getAbsHashes(uint64(from), MaxHashFetch)

	// Wait for the remote response to the head fetch
	number, hash := uint64(0), common.Hash{}
	timeout := time.After(hashTTL)

	for finished := false; !finished; {
		select {
		case <-d.cancelCh:
			return 0, errCancelHashFetch

		case hashPack := <-d.hashCh:
			// Discard anything not from the origin peer
			if hashPack.peerId != p.id {
				glog.V(logger.Debug).Infof("Received hashes from incorrect peer(%s)", hashPack.peerId)
				break
			}
			// Make sure the peer actually gave something valid
			hashes := hashPack.hashes
			if len(hashes) == 0 {
				glog.V(logger.Debug).Infof("%v: empty head hash set", p)
				return 0, errEmptyHashSet
			}
			// Check if a common ancestor was found
			finished = true
			for i := len(hashes) - 1; i >= 0; i-- {
				if d.hasBlock(hashes[i]) {
					number, hash = uint64(from)+uint64(i), hashes[i]
					break
				}
			}

		case <-d.blockCh:
			// Out of bounds blocks received, ignore them

		case <-timeout:
			glog.V(logger.Debug).Infof("%v: head hash timeout", p)
			return 0, errTimeout
		}
	}
	// If the head fetch already found an ancestor, return
	if !common.EmptyHash(hash) {
		glog.V(logger.Debug).Infof("%v: common ancestor: #%d [%x]", p, number, hash[:4])
		return number, nil
	}
	// Ancestor not found, we need to binary search over our chain
	start, end := uint64(0), head
	for start+1 < end {
		// Split our chain interval in two, and request the hash to cross check
		check := (start + end) / 2

		timeout := time.After(hashTTL)
		go p.getAbsHashes(uint64(check), 1)

		// Wait until a reply arrives to this request
		for arrived := false; !arrived; {
			select {
			case <-d.cancelCh:
				return 0, errCancelHashFetch

			case hashPack := <-d.hashCh:
				// Discard anything not from the origin peer
				if hashPack.peerId != p.id {
					glog.V(logger.Debug).Infof("Received hashes from incorrect peer(%s)", hashPack.peerId)
					break
				}
				// Make sure the peer actually gave something valid
				hashes := hashPack.hashes
				if len(hashes) != 1 {
					glog.V(logger.Debug).Infof("%v: invalid search hash set (%d)", p, len(hashes))
					return 0, errBadPeer
				}
				arrived = true

				// Modify the search interval based on the response
				block := d.getBlock(hashes[0])
				if block == nil {
					end = check
					break
				}
				if block.NumberU64() != check {
					glog.V(logger.Debug).Infof("%v: non requested hash #%d [%x], instead of #%d", p, block.NumberU64(), block.Hash().Bytes()[:4], check)
					return 0, errBadPeer
				}
				start = check

			case <-d.blockCh:
				// Out of bounds blocks received, ignore them

			case <-timeout:
				glog.V(logger.Debug).Infof("%v: search hash timeout", p)
				return 0, errTimeout
			}
		}
	}
	return start, nil
}

// fetchHashes keeps retrieving hashes from the requested number, until no more
// are returned, potentially throttling on the way.
func (d *Downloader) fetchHashes(p *peer, td *big.Int, from uint64) error {
	glog.V(logger.Debug).Infof("%v: downloading hashes from #%d", p, from)

	// Create a timeout timer, and the associated hash fetcher
	timeout := time.NewTimer(0) // timer to dump a non-responsive active peer
	<-timeout.C                 // timeout channel should be initially empty
	defer timeout.Stop()

	getHashes := func(from uint64) {
		glog.V(logger.Detail).Infof("%v: fetching %d hashes from #%d", p, MaxHashFetch, from)

		go p.getAbsHashes(from, MaxHashFetch)
		timeout.Reset(hashTTL)
	}
	// Start pulling hashes, until all are exhausted
	getHashes(from)
	gotHashes := false

	for {
		select {
		case <-d.cancelCh:
			return errCancelHashFetch

		case hashPack := <-d.hashCh:
			// Make sure the active peer is giving us the hashes
			if hashPack.peerId != p.id {
				glog.V(logger.Debug).Infof("Received hashes from incorrect peer(%s)", hashPack.peerId)
				break
			}
			timeout.Stop()

			// If no more hashes are inbound, notify the block fetcher and return
			if len(hashPack.hashes) == 0 {
				glog.V(logger.Debug).Infof("%v: no available hashes", p)

				select {
				case d.processCh <- false:
				case <-d.cancelCh:
				}
				// If no hashes were retrieved at all, the peer violated it's TD promise that it had a
				// better chain compared to ours. The only exception is if it's promised blocks were
				// already imported by other means (e.g. fecher):
				//
				// R <remote peer>, L <local node>: Both at block 10
				// R: Mine block 11, and propagate it to L
				// L: Queue block 11 for import
				// L: Notice that R's head and TD increased compared to ours, start sync
				// L: Import of block 11 finishes
				// L: Sync begins, and finds common ancestor at 11
				// L: Request new hashes up from 11 (R's TD was higher, it must have something)
				// R: Nothing to give
				if !gotHashes && td.Cmp(d.headBlock().Td) > 0 {
					return errStallingPeer
				}
				return nil
			}
			gotHashes = true

			// Otherwise insert all the new hashes, aborting in case of junk
			glog.V(logger.Detail).Infof("%v: inserting %d hashes from #%d", p, len(hashPack.hashes), from)

			inserts := d.queue.Insert(hashPack.hashes, true)
			if len(inserts) != len(hashPack.hashes) {
				glog.V(logger.Debug).Infof("%v: stale hashes", p)
				return errBadPeer
			}
			// Notify the block fetcher of new hashes, but stop if queue is full
			cont := d.queue.Pending() < maxQueuedHashes
			select {
			case d.processCh <- cont:
			default:
			}
			if !cont {
				return nil
			}
			// Queue not yet full, fetch the next batch
			from += uint64(len(hashPack.hashes))
			getHashes(from)

		case <-timeout.C:
			glog.V(logger.Debug).Infof("%v: hash request timed out", p)
			return errTimeout
		}
	}
}

// fetchBlocks iteratively downloads the scheduled hashes, taking any available
// peers, reserving a chunk of blocks for each, waiting for delivery and also
// periodically checking for timeouts.
func (d *Downloader) fetchBlocks(from uint64) error {
	glog.V(logger.Debug).Infof("Downloading blocks from #%d", from)
	defer glog.V(logger.Debug).Infof("Block download terminated")

	// Create a timeout timer for scheduling expiration tasks
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	update := make(chan struct{}, 1)

	// Prepare the queue and fetch blocks until the hash fetcher's done
	d.queue.Prepare(from)
	finished := false

	for {
		select {
		case <-d.cancelCh:
			return errCancelBlockFetch

		case blockPack := <-d.blockCh:
			// If the peer was previously banned and failed to deliver it's pack
			// in a reasonable time frame, ignore it's message.
			if peer := d.peers.Peer(blockPack.peerId); peer != nil {
				// Deliver the received chunk of blocks, and demote in case of errors
				err := d.queue.Deliver(blockPack.peerId, blockPack.blocks)
				switch err {
				case nil:
					// If no blocks were delivered, demote the peer (need the delivery above)
					if len(blockPack.blocks) == 0 {
						peer.Demote()
						peer.SetIdle()
						glog.V(logger.Detail).Infof("%s: no blocks delivered", peer)
						break
					}
					// All was successful, promote the peer and potentially start processing
					peer.Promote()
					peer.SetIdle()
					glog.V(logger.Detail).Infof("%s: delivered %d blocks", peer, len(blockPack.blocks))
					go d.process()

				case errInvalidChain:
					// The hash chain is invalid (blocks are not ordered properly), abort
					return err

				case errNoFetchesPending:
					// Peer probably timed out with its delivery but came through
					// in the end, demote, but allow to to pull from this peer.
					peer.Demote()
					peer.SetIdle()
					glog.V(logger.Detail).Infof("%s: out of bound delivery", peer)

				case errStaleDelivery:
					// Delivered something completely else than requested, usually
					// caused by a timeout and delivery during a new sync cycle.
					// Don't set it to idle as the original request should still be
					// in flight.
					peer.Demote()
					glog.V(logger.Detail).Infof("%s: stale delivery", peer)

				default:
					// Peer did something semi-useful, demote but keep it around
					peer.Demote()
					peer.SetIdle()
					glog.V(logger.Detail).Infof("%s: delivery partially failed: %v", peer, err)
					go d.process()
				}
			}
			// Blocks arrived, try to update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		case cont := <-d.processCh:
			// The hash fetcher sent a continuation flag, check if it's done
			if !cont {
				finished = true
			}
			// Hashes arrive, try to update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		case <-ticker.C:
			// Sanity check update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		case <-update:
			// Short circuit if we lost all our peers
			if d.peers.Len() == 0 {
				return errNoPeers
			}
			// Check for block request timeouts and demote the responsible peers
			for _, pid := range d.queue.Expire(blockHardTTL) {
				if peer := d.peers.Peer(pid); peer != nil {
					peer.Demote()
					glog.V(logger.Detail).Infof("%s: block delivery timeout", peer)
				}
			}
			// If there's noting more to fetch, wait or terminate
			if d.queue.Pending() == 0 {
				if d.queue.InFlight() == 0 && finished {
					glog.V(logger.Debug).Infof("Block fetching completed")
					return nil
				}
				break
			}
			// Send a download request to all idle peers, until throttled
			for _, peer := range d.peers.IdlePeers() {
				// Short circuit if throttling activated
				if d.queue.Throttle() {
					break
				}
				// Reserve a chunk of hashes for a peer. A nil can mean either that
				// no more hashes are available, or that the peer is known not to
				// have them.
				request := d.queue.Reserve(peer, peer.Capacity())
				if request == nil {
					continue
				}
				if glog.V(logger.Detail) {
					glog.Infof("%s: requesting %d blocks", peer, len(request.Hashes))
				}
				// Fetch the chunk and make sure any errors return the hashes to the queue
				if err := peer.Fetch(request); err != nil {
					glog.V(logger.Error).Infof("%v: fetch failed, rescheduling", peer)
					d.queue.Cancel(request)
				}
			}
			// Make sure that we have peers available for fetching. If all peers have been tried
			// and all failed throw an error
			if !d.queue.Throttle() && d.queue.InFlight() == 0 {
				return errPeersUnavailable
			}
		}
	}
}

// banBlocks retrieves a batch of blocks from a peer feeding us invalid hashes,
// and bans the head of the retrieved batch.

// This method only fetches one single batch as the goal is not ban an entire
// (potentially long) invalid chain - wasting a lot of time in the meanwhile -,
// but rather to gradually build up a blacklist if the peer keeps reconnecting.
// 作用是更新当前Downloader对象的banned集合,将最新的被禁止区块的哈希添加(必定是位于上一个被ban区块的后方的区块哈希)
func (d *Downloader) banBlocks(peerId string, head common.Hash) error {
	glog.V(logger.Debug).Infof("Banning a batch out of %d blocks from %s", d.queue.Pending(), peerId)

	// Ask the peer being banned for a batch of blocks from the banning point
	peer := d.peers.Peer(peerId) //从ps.peers集合检索并返回指定p.id的peer节点实体
	if peer == nil {
		return nil
	}
	request := d.queue.Reserve(peer, MaxBlockFetch) //从q.hashQueue中取出待获取的区块hash值(也就是之前insert插入的区块hash)，组装成fetchRequest查询包记录到q.pendPool队列中
	if request == nil {
		return nil
	}
	if err := peer.Fetch(request); err != nil { // 向远端peer发送此区块检索请求
		return err
	}
	// Wait a bit for the reply to arrive, and ban if done so
	timeout := time.After(blockHardTTL) //等待目标peer节点的回复
	for {
		select {
		case <-d.cancelCh:
			return errCancelBlockFetch

		case <-timeout:
			return errTimeout

		case <-d.hashCh:
			// Out of bounds hashes received, ignore them

		case blockPack := <-d.blockCh: //收到了对端peer节点回复的区块集合blocks
			blocks := blockPack.blocks //从blockPack中提取出获取的若干区块

			// Short circuit if it's a stale cross check
			if len(blocks) == 1 { //如果只包含一个区块
				block := blocks[0]
				if _, ok := d.checks[block.Hash()]; ok { //检查此区块是否是因为进行区块检查(crossCheck包)而获得
					delete(d.checks, block.Hash()) //如是,认为此区块的检查通过,将其从d.checks中删除,然后退出本次循环
					break
				}
			}
			// Short circuit if it's not from the peer being banned
			if blockPack.peerId != peerId { //检查接收的blockPack是否来源于形参指定的peer节点处(收发是否为同一目标peer)
				break
			}
			// Short circuit if no blocks were returned
			if len(blocks) == 0 {
				return errors.New("no blocks returned to ban")
			}
			// Reconstruct the original chain order and ensure we're banning the correct blocks
			types.BlockBy(types.Number).Sort(blocks)                        //将获取的区块按照区块编号进行排序
			if bytes.Compare(blocks[0].Hash().Bytes(), head.Bytes()) != 0 { //检查排序后的获取区块链首区块是否为形参给定的被ban区块
				return errors.New("head block not the banned one")
			}
			index := 0
			for _, block := range blocks[1:] { //检验排序后的区块链是否确实是连续的
				if bytes.Compare(block.ParentHash().Bytes(), blocks[index].Hash().Bytes()) != 0 { //判断当前区块的父区块与排序后的上一个区块是否为同一个
					break
				}
				index++
			}
			// Ban the head hash and phase out any excess
			d.banned.Add(blocks[index].Hash())      //将排序区块链的最后一个区块加入ban名单(这样从上一个被ban区块到当前新的被ban区块之间的所有区块都是不合法的)
			for d.banned.Size() > maxBannedHashes { //d.banned容量超过限制
				var evacuate common.Hash //选出一个需要撤离出d.banned的区块哈希

				d.banned.Each(func(item interface{}) bool { //根据hash值是否存在于core.BadHashes中选择是否作为evacuate从d.banned中删除
					// Skip any hard coded bans
					if core.BadHashes[item.(common.Hash)] { //存在
						return true
					}
					evacuate = item.(common.Hash) //不存在
					return false
				})
				d.banned.Remove(evacuate)
			}
			glog.V(logger.Debug).Infof("Banned %d blocks from: %s", index+1, peerId)
			return nil
		}
	}
}

// process takes blocks from the queue and tries to import them into the chain.
// 从queue队列中取出区块,并尝试将其导入本地区块链
//
// The algorithmic flow is as follows:
//  - The `processing` flag is swapped to 1 to ensure singleton access  1,`processing`标志位被置为1,确保只有一个协程正在进行process
//  - The current `cancel` channel is retrieved to detect sync abortions  2.检索`cancel`管道；来检测downloader的整个同步是否已被终止
//  - Blocks are iteratively taken from the cache and inserted into the chain  3.持续将缓存中的区块插入到本地链,直到缓存为空
//  - When the cache becomes empty, insertion stops
//  - The `processing` flag is swapped back to 0     4.处理完成后, `processing` 标志位重新置为0
//  - A post-exit check is made whether new blocks became available   5.在退出process方法之前,检查是否有新的区块可用
//     - This step is important: it handles a potential race condition between
//       checking for no more work, and releasing the processing "mutex". In
//       between these state changes, a block may have arrived, but a processing
//       attempt denied, so we need to re-enter to ensure the block isn't left
//       to idle in the cache.
func (d *Downloader) process() {
	// Make sure only one goroutine is ever allowed to process blocks at once
	if !atomic.CompareAndSwapInt32(&d.processing, 0, 1) { //确保一次只有一个协程在进行process操作
		return
	}
	// If the processor just exited, but there are freshly pending items, try to
	// reenter. This is needed because the goroutine spinned up for processing
	// the fresh blocks might have been rejected entry to to this present thread
	// not yet releasing the `processing` state.
	defer func() {
		if atomic.LoadInt32(&d.interrupt) == 0 && d.queue.GetHeadBlock() != nil {
			d.process() //如果又有新的区块需要加入本地链,重复进行process()
		}
	}()
	// Release the lock upon exit (note, before checking for reentry!), and set
	// the import statistics to zero.
	defer func() {
		d.importLock.Lock()
		d.importQueue = nil
		d.importDone = 0
		d.importLock.Unlock()

		atomic.StoreInt32(&d.processing, 0) //将processing标志位重新设置为0
	}()
	for { //只要有需要进行导入的区块,就重复循环
		// Fetch the next batch of blocks
		blocks := d.queue.TakeBlocks() //取出q.blockCache中缓存的所有区块
		if len(blocks) == 0 {          //只要区块数目不为零,就重复循环
			return
		}
		// Reset the import statistics
		// 重置所有与导入相关的统计量
		d.importLock.Lock()
		d.importStart = time.Now()
		d.importQueue = blocks
		d.importDone = 0
		d.importLock.Unlock()

		// Actually import the blocks
		glog.V(logger.Debug).Infof("Inserting chain with %d blocks (#%v - #%v)\n", len(blocks), blocks[0].RawBlock.Number(), blocks[len(blocks)-1].RawBlock.Number())
		for len(blocks) != 0 {
			// Check for any termination requests
			if atomic.LoadInt32(&d.interrupt) == 1 { //检查是否有终止请求产生(是否调用func (d *Downloader) Terminate())
				return
			}
			// Retrieve the first batch of blocks to insert
			max := int(math.Min(float64(len(blocks)), float64(maxBlockProcess))) //本次要插入的区块的个数(不能超过maxBlockProcess)
			raw := make(types.Blocks, 0, max)
			for _, block := range blocks[:max] { //遍历所有要插入的区块,将其加入raw队列
				raw = append(raw, block.RawBlock)
			}
			// Try to inset the blocks, drop the originating peer if there's an error
			index, err := d.insertChain(raw) //向本地区块链中插入这一批区块
			if err != nil {                  //如果插入过程中出现了错误
				glog.V(logger.Debug).Infof("Block #%d import failed: %v", raw[index].NumberU64(), err)
				d.dropPeer(blocks[index].OriginPeer) //将错误区块的发布者认定为恶意节点
				d.cancel()                           //停止downloader的所有操作,重置downloader的queue队列
				return
			}
			blocks = blocks[max:]
		}
	}
}

// DeliverBlocks injects a new batch of blocks received from a remote node.
// This is usually invoked through the BlocksMsg by the protocol handler.
// 将从远端peer处获取的区块block集合与对端节点的p.id一起,组装成 blockPack(每次只选取两个区块进行组装,也就是一组父子区块),输入到d.blockCh管道中
// 通常是在收到对端peer节点的BlocksMsg消息后,由本地的由协议处理程序调用
func (d *Downloader) DeliverBlocks(id string, blocks []*types.Block) error {
	// Make sure the downloader is active
	if atomic.LoadInt32(&d.synchronising) == 0 { //确保downloader正处于活跃状态(正在与其他peer节点进行区块同步)
		return errNoSyncActive
	}
	// Deliver or abort if the sync is canceled while queuing
	d.cancelLock.RLock()
	cancel := d.cancelCh
	d.cancelLock.RUnlock()

	select {
	case d.blockCh <- blockPack{id, blocks}: //将从对端peer处获取的区块实体集合连同对端节点的p.id共同打包成blockPack输入到d.blockCh管道
		return nil

	case <-cancel:
		return errNoSyncActive
	}
}

// DeliverHashes injects a new batch of hashes received from a remote node into
// the download schedule. This is usually invoked through the BlockHashesMsg by
// the protocol handler.
// 将从远端peer处获取的区块hash集合与对端节点的p.id一起,组装成 hashPack,输入到d.hashCh管道中
// 通常是在收到对端peer节点的BlockHashesMsg消息后,由本地的由协议处理程序调用
func (d *Downloader) DeliverHashes(id string, hashes []common.Hash) error {
	// Make sure the downloader is active
	if atomic.LoadInt32(&d.synchronising) == 0 { //确保downloader正处于活跃状态(也就是正处于与其他peer的同步状态中)
		return errNoSyncActive
	}
	// Deliver or abort if the sync is canceled while queuing
	d.cancelLock.RLock()
	cancel := d.cancelCh
	d.cancelLock.RUnlock()

	select {
	case d.hashCh <- hashPack{id, hashes}:
		return nil

	case <-cancel:
		return errNoSyncActive
	}
}
