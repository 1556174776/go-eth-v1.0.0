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

// Package fetcher contains the block announcement based synchonisation.
package fetcher

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"gopkg.in/karalabe/cookiejar.v2/collections/prque"
)

const (
	arriveTimeout = 500 * time.Millisecond // Time allowance before an announced block is explicitly requested
	gatherSlack   = 100 * time.Millisecond // Interval used to collate almost-expired announces with fetches
	fetchTimeout  = 5 * time.Second        // Maximum alloted time to return an explicitly requested block
	maxUncleDist  = 7                      // Maximum allowed backward distance from the chain head
	maxQueueDist  = 32                     // Maximum allowed distance from the chain head to queue
	hashLimit     = 256                    // Maximum number of unique blocks a peer may have announced
	blockLimit    = 64                     // Maximum number of unique blocks a per may have delivered
)

var (
	errTerminated = errors.New("terminated")
)

// blockRetrievalFn is a callback type for retrieving a block from the local chain.
//在本地区块链上检索并返回一个目标哈希的区块
type blockRetrievalFn func(common.Hash) *types.Block

// blockRequesterFn is a callback type for sending a block retrieval request.
// 发起一个区块检索请求(向其他peer节点发送区块获取申请)
type blockRequesterFn func([]common.Hash) error

// blockValidatorFn is a callback type to verify a block's header for fast propagation.
// 验证一个区块块头的合法性
type blockValidatorFn func(block *types.Block, parent *types.Block) error

// blockBroadcasterFn is a callback type for broadcasting a block to connected peers.
// 向所有已连接的peer广播一个区块
type blockBroadcasterFn func(block *types.Block, propagate bool)

// chainHeightFn is a callback type to retrieve the current chain height.
// 返回当前区块链的高度
type chainHeightFn func() uint64

// chainInsertFn is a callback type to insert a batch of blocks into the local chain.
// 向本地区块链插入一批区块
type chainInsertFn func(types.Blocks) (int, error)

// peerDropFn is a callback type for dropping a peer detected as malicious.
// 根据p.id删除恶意节点
type peerDropFn func(id string)

// announce is the hash notification of the availability of a new block in the
// network.
// announce类用于向其他peer节点通知某一区块的可用性(收到此announce的peer节点可向已连接的其他peer获取此区块实体)
// 当前节点收到的announce宣告会被注入到notify管道中
type announce struct {
	hash common.Hash // Hash of the block being announced   被宣告的区块的哈希
	time time.Time   // Timestamp of the announcement       宣布时刻的时间戳

	origin string           // Identifier of the peer originating the notification   发起announce的对等peer的身份标识
	fetch  blockRequesterFn // Fetcher function to retrieve   用于检索的提取函数
}

// inject represents a schedules import operation.
// inject类用于记录一个收到的区块实体(一般收到区块时，会将其注入到inject管道，完成区块排序并验证后再添加到本地区块链)
type inject struct {
	origin string       //区块提供者的身份标识
	block  *types.Block //区块
}

// Fetcher is responsible for accumulating block announcements from various peers
// and scheduling them for retrieval.
// Fetcher类
type Fetcher struct {
	// Various event channels
	notify chan *announce           //存放所有announce宣告(announce用于向其他连接的peer获取本地区块链不存在的被宣告的区块)
	inject chan *inject             //存放所有从其他peer节点获取的block区块实体以及发送此区块的peer节点的id
	filter chan chan []*types.Block //过滤区块集合使用的管道
	done   chan common.Hash         //传递已提交的区块的哈希值
	quit   chan struct{}

	// Announce states
	announces map[string]int              // 记录每一个peer的announce的数量
	announced map[common.Hash][]*announce // key值为区块哈希,value为所有针对该区块哈希的announce宣告(这些announce宣告针对的区块哈希相同,但是来源p.id不同)
	fetching  map[common.Hash]*announce   // key值为区块哈希,value为某一个针对该区块哈希的announce宣告(从对应组announced中随机选出一个announce宣告即可)

	// Block cache
	queue  *prque.Prque            // 存放来自于其他节点inject(按照区块号进行了排序)
	queues map[string]int          // 记录每一个peer的inject数目(区块提交数)
	queued map[common.Hash]*inject // key值为区块哈希,value为对应区块哈希的inject(也就是区块实体)。需注意,queued已经完成了排序(按照区块号顺序)

	// Callbacks
	getBlock       blockRetrievalFn   // Retrieves a block from the local chain
	validateBlock  blockValidatorFn   // Checks if a block's headers have a valid proof of work
	broadcastBlock blockBroadcasterFn // Broadcasts a block to connected peers
	chainHeight    chainHeightFn      // Retrieves the current chain's height
	insertChain    chainInsertFn      // Injects a batch of blocks into the chain
	dropPeer       peerDropFn         // Drops a peer for misbehaving

	// Testing hooks
	fetchingHook func([]common.Hash) // Method to call upon starting a block fetch
	importedHook func(*types.Block)  // Method to call upon successful block import
}

// New creates a block fetcher to retrieve blocks based on hash announcements.
func New(getBlock blockRetrievalFn, validateBlock blockValidatorFn, broadcastBlock blockBroadcasterFn, chainHeight chainHeightFn, insertChain chainInsertFn, dropPeer peerDropFn) *Fetcher {
	return &Fetcher{
		notify:         make(chan *announce),
		inject:         make(chan *inject),
		filter:         make(chan chan []*types.Block),
		done:           make(chan common.Hash),
		quit:           make(chan struct{}),
		announces:      make(map[string]int),
		announced:      make(map[common.Hash][]*announce),
		fetching:       make(map[common.Hash]*announce),
		queue:          prque.New(),
		queues:         make(map[string]int),
		queued:         make(map[common.Hash]*inject),
		getBlock:       getBlock,
		validateBlock:  validateBlock,
		broadcastBlock: broadcastBlock,
		chainHeight:    chainHeight,
		insertChain:    insertChain,
		dropPeer:       dropPeer,
	}
}

// Start boots up the announcement based synchoniser, accepting and processing
// hash notifications and block fetches until termination requested.
//
func (f *Fetcher) Start() {
	go f.loop()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (f *Fetcher) Stop() {
	close(f.quit)
}

// Notify announces the fetcher of the potential availability of a new block in
// the network.
// 根据区块哈希值组装announce宣告(peer和hash一般从NewBlockHashesMsg消息中提取)。将组装好的announce宣告输入到f.notify管道
func (f *Fetcher) Notify(peer string, hash common.Hash, time time.Time, fetcher blockRequesterFn) error {
	block := &announce{
		hash:   hash,
		time:   time,
		origin: peer,    //对方节点的标识符(p.id)
		fetch:  fetcher, //调用此函数可向对方节点申请获得指定hash值的一批区块
	}
	select {
	case f.notify <- block:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Enqueue tries to fill gaps the the fetcher's future import queue.
// 将接收到的区块实体组装为inject对象(peer和block从BlocksMsg消息中获取).将组装好的inject对象输入到f.inject管道中
func (f *Fetcher) Enqueue(peer string, block *types.Block) error {
	op := &inject{
		origin: peer,
		block:  block,
	}
	select {
	case f.inject <- op:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Filter extracts all the blocks that were explicitly requested by the fetcher,
// returning those that should be handled differently.
// 对形参传入的区块集合进行过滤,返回不符合过滤条件的区块
func (f *Fetcher) Filter(blocks types.Blocks) types.Blocks {
	// Send the filter channel to the fetcher
	filter := make(chan []*types.Block)

	// 2.将filter管道中的区块集合输入到f.filter管道中(触发loop函数所在协程的case filter := <-f.filter)
	select {
	case f.filter <- filter:
	case <-f.quit:
		return nil
	}
	// Request the filtering of the block list
	// 1.将形参给定的区块集合输入到函数局部变量filter管道中
	select {
	case filter <- blocks:
	case <-f.quit:
		return nil
	}
	// Retrieve the blocks remaining after filtering
	// 3.获取被过滤的区块(没能通过filter过滤条件的区块)
	select {
	case blocks := <-filter:
		return blocks
	case <-f.quit:
		return nil
	}
}

// Loop is the main fetcher loop, checking and processing various notification
// events.
func (f *Fetcher) loop() {
	// Iterate the block fetching until a quit is requested
	fetch := time.NewTimer(0) //创建一个定时器,当实参指定的定时周期到达后,在其自身所有信道上发送当前时间
	for {
		// 清除过期的区块announce宣告
		for hash, announce := range f.fetching { //遍历Fetcher对象的fetching队列(每一个announce宣告都是针对不同区块的)
			if time.Since(announce.time) > fetchTimeout { //检查此announce宣告是否过期
				f.forgetHash(hash) //过期的announce宣告不再向下执行(不再执行fetcher操作),将其有关记录从Fetcher中删除
			}
		}
		// Import any queued blocks that could potentially fit
		height := f.chainHeight() //获取本地区块链高度
		//从queue队列中获取一个inject(包含一个区块实体)
		for !f.queue.Empty() { //如果queue非空
			op := f.queue.PopItem().(*inject) //出队一个inject对象(出队一个区块实体)
			// 1.必须要保证此区块编号小于本地区块链高度
			number := op.block.NumberU64() //获取区块的编号
			if number > height+1 {         //区块编号大于区块高度
				f.queue.Push(op, -float32(op.block.NumberU64())) //什么操作？？
				break
			}
			// 2.必须保证此区块并不存在于本地区块链中(如果存在我们不需要将其再插入到本地区块链上)
			hash := op.block.Hash()                                      //获取包含在inject中的提交区块的哈希
			if number+maxUncleDist < height || f.getBlock(hash) != nil { //在本地检索此区块(第一条判断条件不太理解？？)
				f.forgetBlock(hash) //不符合上述两条件的区块,不再执行区块插入本地区块链操作
				continue
			}
			f.insert(op.origin, op.block) //将获取的区块插入到本地区块链中(当然插入前还需要验证区块的合法性)
		}
		// 必须在处理完queue队列中残存的inject后(完成区块的插入),才能进行下述select接收操作
		select {
		case <-f.quit:
			// Fetcher terminating, abort all operations
			return

		case notification := <-f.notify: //1.收到来自于其他peer节点的区块有效性announce宣告
			// A block was announced, make sure the peer isn't DOSing us
			announceMeter.Mark(1)

			count := f.announces[notification.origin] + 1 //对应peer节点的announce数+1
			if count > hashLimit {                        //挂起队列的长度超过最大容量,跳出本次处理,直到不再拥塞
				glog.V(logger.Debug).Infof("Peer %s: exceeded outstanding announces (%d)", notification.origin, hashLimit)
				break
			}
			// All is well, schedule the announce if block's not yet downloading
			if _, ok := f.fetching[notification.hash]; ok { //如果此区块announce已经存在于fetcher队列中,不再进行处理
				break
			}
			f.announces[notification.origin] = count
			f.announced[notification.hash] = append(f.announced[notification.hash], notification) //将此announce宣告添加到对应hash值的announced中
			if len(f.announced) == 1 {                                                            //如果只有关于一个区块的announce宣告,重新设定fetcher定时器的计时周期
				f.reschedule(fetch)
			}

		case op := <-f.inject: //2.收到其他peer节点发布的区块(inject对象)
			broadcastMeter.Mark(1)
			f.enqueue(op.origin, op.block) //将区块按照顺序添加到f.queue队列中

		case hash := <-f.done: //3.某区块完成了提交(已经插入到了本地区块链上),删除该区块在本地的所有相关记录
			// A pending import finished, remove all traces of the notification
			f.forgetHash(hash)
			f.forgetBlock(hash)

		case <-fetch.C: //4.fetch计时器到达计时周期
			// At least one block's timer ran out, check for needing retrieval
			request := make(map[string][]common.Hash) //key值为announce宣告发布者的id,value值为此发布者宣告的所有区块hash值集合

			//从announced的每组针对不同区块的announce宣告随机选出一条announce加入到fetching队列中(同时要将对应announce在本地记录中删除)
			for hash, announces := range f.announced { //遍历每一组针对不同区块的announce宣告
				if time.Since(announces[0].time) > arriveTimeout-gatherSlack {
					// Pick a random peer to retrieve from, reset all others
					announce := announces[rand.Intn(len(announces))] //从每一组announce宣告中随机选取出一条announce
					f.forgetHash(hash)                               //将该组announce区块哈希对应的记录从本地删除
					if f.getBlock(hash) == nil {                     //检索此区块哈希是否存在于本地区块链中,如果不存在的话将其加入到fetching队列中
						request[announce.origin] = append(request[announce.origin], hash)
						f.fetching[hash] = announce
					}
				}
			}
			// Send out all block requests
			// 根据announce宣告从其他节点处获取相应的区块实体(调用fetcher函数)
			for peer, hashes := range request { //遍历每一个announce发布者发布的区块hash集合
				if glog.V(logger.Detail) && len(hashes) > 0 {
					list := "["
					for _, hash := range hashes {
						list += fmt.Sprintf("%x, ", hash[:4])
					}
					list = list[:len(list)-2] + "]"

					glog.V(logger.Detail).Infof("Peer %s: fetching %s", peer, list) //日志保留该发布者发布的所有区块hash集合
				}
				// Create a closure of the fetch and schedule in on a new thread
				fetcher, hashes := f.fetching[hashes[0]].fetch, hashes //获取区块检索函数
				go func() {
					if f.fetchingHook != nil {
						f.fetchingHook(hashes) //如果有预处理,则执行
					}
					fetcher(hashes) //用区块检索函数从其他peer节点处获取hashes集合中所指的所有区块
				}()
			}
			// Schedule the next fetch if blocks are still pending
			f.reschedule(fetch) //重新设定fetch计时器计时周期

		case filter := <-f.filter: //5.对需要进行过滤的区块集合blocks进行过滤和排序
			// Blocks arrived, extract any explicit fetches, return all else
			var blocks types.Blocks
			select {
			case blocks = <-filter:
			case <-f.quit:
				return
			}

			explicit, download := []*types.Block{}, []*types.Block{} //explicit队列存放本地区块链没有的,来自于其他peer节点对于fetcher检索函数回应的区块
			for _, block := range blocks {                           //遍历区块集合中的所有区块
				hash := block.Hash()

				// Filter explicitly requested blocks from hash announcements
				if f.fetching[hash] != nil && f.queued[hash] == nil { //确保该区块存在于fetching队列，但不存在于queued队列(也就说明此区块作为fetcher检索函数的回应)
					// Discard if already imported by other means
					if f.getBlock(hash) == nil { //确保本地区块链不存在此区块,将其加入explicit队列
						explicit = append(explicit, block)
					} else { //如果已存在,需要将对应hash值从announce相关本地记录中删除(不能再向其他peer申请获得此哈希的区块)
						f.forgetHash(hash)
					}
				} else { //不符合条件的区块需要加入到download队列
					download = append(download, block)
				}
			}

			// 将download中存放的区块集合重新输入到filter管道
			select {
			case filter <- download:
			case <-f.quit:
				return
			}
			// Schedule the retrieved blocks for ordered import
			for _, block := range explicit { //遍历explicit集合中的所有区块,将其按照区块顺序添加到本地区块链上
				if announce := f.fetching[block.Hash()]; announce != nil {
					f.enqueue(announce.origin, block)
				}
			}
		}
	}
}

// reschedule resets the specified fetch timer to the next announce timeout.
func (f *Fetcher) reschedule(fetch *time.Timer) {
	// Short circuit if no blocks are announced
	if len(f.announced) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, announces := range f.announced { //遍历每一组针对不同区块的announce宣告
		if earliest.After(announces[0].time) {
			earliest = announces[0].time
		}
	}
	fetch.Reset(arriveTimeout - time.Since(earliest))
}

// enqueue schedules a new future import operation, if the block to be imported
// has not yet been seen.
// 将区块按照顺序添加到f.queue队列中(同时也添加到f.queues和f.queued),但前提是要满足:
// 1.对应peer节点的inject数不能超过上限(防止对方peer节点的DDos)
// 2.任何太旧或者太新的区块(相对于当前本地区块链高度而言)不会被添加
func (f *Fetcher) enqueue(peer string, block *types.Block) {
	hash := block.Hash()

	count := f.queues[peer] + 1 //对应peer节点的inject数+1
	if count > blockLimit {     //挂起队列超过最大上限,不再进行任何操作,退出本方法(确保没有受到DDos)
		glog.V(logger.Debug).Infof("Peer %s: discarded block #%d [%x], exceeded allowance (%d)", peer, block.NumberU64(), hash.Bytes()[:4], blockLimit)
		return
	}
	// Discard any past or too distant blocks
	// 丢弃任何太旧或者太新的区块(相对于当前本地区块链高度而言)--丢弃就是不对其进行处理,退出本方法
	if dist := int64(block.NumberU64()) - int64(f.chainHeight()); dist < -maxUncleDist || dist > maxQueueDist {
		glog.V(logger.Debug).Infof("Peer %s: discarded block #%d [%x], distance %d", peer, block.NumberU64(), hash.Bytes()[:4], dist)
		discardMeter.Mark(1)
		return
	}
	// Schedule the block for future importing
	if _, ok := f.queued[hash]; !ok { //确保指定hash的inject对象并不存在于f.queued队列中
		op := &inject{ //根据形参创建一个inject对象
			origin: peer,
			block:  block,
		}
		// 更新该inject对象到f.queues , f.queued , 并按照区块顺序加入到f.queue中
		f.queues[peer] = count
		f.queued[hash] = op
		f.queue.Push(op, -float32(block.NumberU64()))

		if glog.V(logger.Debug) {
			glog.Infof("Peer %s: queued block #%d [%x], total %v", peer, block.NumberU64(), hash.Bytes()[:4], f.queue.Size())
		}
	}
}

// insert spawns a new goroutine to run a block insertion into the chain. If the
// block's number is at the same height as the current import phase, if updates
// the phase states accordingly.
// 检验传入区块的合法性并完成对合法区块的本地插入与广播
// 1.区块合法,将区块广播给其他peer节点,同时将区块插入到本地链上
// 2.区块不合法,标记区块提交者为恶意节点
func (f *Fetcher) insert(peer string, block *types.Block) {
	hash := block.Hash() //获取区块的哈希值

	// Run the import on a new thread
	glog.V(logger.Debug).Infof("Peer %s: importing block #%d [%x]", peer, block.NumberU64(), hash[:4])
	go func() {
		defer func() { f.done <- hash }()

		// If the parent's unknown, abort insertion
		parent := f.getBlock(block.ParentHash()) //在本地检索当前区块的上一区块(父区块)
		if parent == nil {
			return
		}
		// Quickly validate the header and propagate the block if it passes
		switch err := f.validateBlock(block, parent); err { //验证当前区块的合法性
		case nil:
			// All ok, quickly propagate to our peers
			// 检验合格,快速将此区块广播给其他peer节点
			broadcastTimer.UpdateSince(block.ReceivedAt)
			go f.broadcastBlock(block, true) //广播该区块实体给其他peer节点

		case core.BlockFutureErr: //预测未来可能出错，不报错，但也不提交区块
			futureMeter.Mark(1)
			// Weird future block, don't fail, but neither propagate

		default: //区块验证失败,认为该区块的提交者为恶意节点
			// Something went very wrong, drop the peer
			glog.V(logger.Debug).Infof("Peer %s: block #%d [%x] verification failed: %v", peer, block.NumberU64(), hash[:4], err)
			f.dropPeer(peer) //标记区块提交者为恶意节点
			return
		}
		// Run the actual import and log any issues
		// 在本地区块链中插入该区块
		if _, err := f.insertChain(types.Blocks{block}); err != nil {
			glog.V(logger.Warn).Infof("Peer %s: block #%d [%x] import failed: %v", peer, block.NumberU64(), hash[:4], err)
			return
		}
		// If import succeeded, broadcast the block
		announceTimer.UpdateSince(block.ReceivedAt)
		go f.broadcastBlock(block, false) //仅向其它节点广播此区块的哈希值

		// Invoke the testing hook if needed
		if f.importedHook != nil {
			f.importedHook(block)
		}
	}()
}

// forgetHash removes all traces of a block announcement from the fetcher's
// internal state.
// 将形参指定的区块哈希涉及的announce记录从Fetcher中删除
func (f *Fetcher) forgetHash(hash common.Hash) {
	// Remove all pending announces and decrement DOS counters
	for _, announce := range f.announced[hash] { //遍历所有具有相同key值(区块哈希)的announce宣告(区块哈希相同,但是来源peer不同，也就是p.id不同)
		f.announces[announce.origin]--         //对应来源peer节点的announce宣告数目-1
		if f.announces[announce.origin] == 0 { //如果对应peer节点的announce宣告数目 == 0,将此p.id对应的peer节点删除
			delete(f.announces, announce.origin)
		}
	}
	delete(f.announced, hash) //从announced集合中删除有关该区块哈希的全部announce宣告

	// Remove any pending fetches and decrement the DOS counters
	if announce := f.fetching[hash]; announce != nil { //如果对应区块哈希的announce宣告存在于fetching队列中
		f.announces[announce.origin]-- //对应来源peer节点的announce宣告数目-1
		if f.announces[announce.origin] == 0 {
			delete(f.announces, announce.origin)
		}
		delete(f.fetching, hash) //从fetcher集合中删除该区块哈希对应的announce宣告
	}
}

// forgetBlock removes all traces of a queued block frmo the fetcher's internal
// state.
// 将形参指定的区块涉及的inject记录从Fetcher中删除
func (f *Fetcher) forgetBlock(hash common.Hash) {
	if insert := f.queued[hash]; insert != nil { //获取指定区块hash的inject
		f.queues[insert.origin]--         //对应区块发布peer的inject数目-1
		if f.queues[insert.origin] == 0 { //当inject数为0时,从queues队列中删除此节点
			delete(f.queues, insert.origin)
		}
		delete(f.queued, hash) //从queued队列中删除上述指定hash的inject
	}
}
