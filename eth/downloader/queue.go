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

// Contains the block download scheduler to collect download tasks and schedule
// them in an ordered, and throttled way.

package downloader

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/cookiejar.v2/collections/prque"
	"GO_Demo/go-ethereum/core/types"
	"GO_Demo/go-ethereum/logger"
	"GO_Demo/go-ethereum/logger/glog"
)

var (
	blockCacheLimit = 8 * MaxBlockFetch // Maximum number of blocks to cache before throttling the download
)

var (
	errNoFetchesPending = errors.New("no fetches pending")
	errStaleDelivery    = errors.New("stale delivery")
)

// fetchRequest is a currently running block retrieval operation.
// 每一个fetchRequest对象包含了当前节点对另一个peer节点的若干区块的索引
type fetchRequest struct {
	Peer   *peer               // Peer to which the request was sent   区块检索的目标peer
	Hashes map[common.Hash]int // Requested hashes with their insertion index (priority)  请求检索的区块哈希以及该区块插入索引优先级
	Time   time.Time           // Time when the request was made  提出检索请求的时间
}

// queue represents hashes that are either need fetching or are being fetched
type queue struct {
	hashPool    map[common.Hash]int // Pending hashes, mapping to their insertion index (priority)  包含所有需要获取的区块的哈希及其下标(就是hashCounter)
	hashQueue   *prque.Prque        // Priority queue of the block hashes to fetch   需要获取的区块哈希的按优先级排列的队列(插入队列时的位置以hashCounter决定)
	hashCounter int                 // Counter indexing the added hashes to ensure retrieval order  累计所有新加入区块hash在hashQueue的索引值

	pendPool map[string]*fetchRequest // Currently pending block retrieval operations   当前挂起的块检索操作(根据hashQueue中的区块hash请求组装获取fetchRequest)

	blockPool   map[common.Hash]uint64 // Hash-set of the downloaded data blocks, mapping to cache indexes  已经被下载的区块的哈希集合(key值为区块哈希,value值区块的编号)
	blockCache  []*Block               // Downloaded but not yet delivered blocks  已从其他peer处下载的但尚未被交付的区块集合(对区块是连续保存的)
	blockOffset uint64                 // Offset of the first cached block in the block-chain  blockCache集合中第一个区块的编号在本地区块链中的偏移量(相对于创世区块)

	lock sync.RWMutex
}

// newQueue creates a new download queue for scheduling block retrieval.
func newQueue() *queue {
	return &queue{
		hashPool:   make(map[common.Hash]int),
		hashQueue:  prque.New(),
		pendPool:   make(map[string]*fetchRequest),
		blockPool:  make(map[common.Hash]uint64),
		blockCache: make([]*Block, blockCacheLimit),
	}
}

// Reset clears out the queue contents.
// 清空整个queue队列对象
func (q *queue) Reset() {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.hashPool = make(map[common.Hash]int)
	q.hashQueue.Reset()
	q.hashCounter = 0

	q.pendPool = make(map[string]*fetchRequest)

	q.blockPool = make(map[common.Hash]uint64)
	q.blockOffset = 0
	q.blockCache = make([]*Block, blockCacheLimit)
}

// Size retrieves the number of hashes in the queue, returning separately for
// pending and already downloaded.
// 分别检索返回 区块检索请求池中挂起的请求数目 和 已下载的区块的数目
func (q *queue) Size() (int, int) {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return len(q.hashPool), len(q.blockPool)
}

// Pending retrieves the number of hashes pending for retrieval.
// 返回区块检索请求队列(q.hashQueue)中请求的数目
func (q *queue) Pending() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.hashQueue.Size()
}

// InFlight retrieves the number of fetch requests currently in flight.
// 返回当前正在运行的提取请求数(具体来说是目标节点的数目,因为针对于每一个peer节点通常包含对若干区块hash的提取)
func (q *queue) InFlight() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return len(q.pendPool)
}

// Throttle checks if the download should be throttled (active block fetches
// exceed block cache).
// 检查是否需要限制区块的下载(区块检索请求过多,超过了本地的区块缓存池q.blockPool容量上限)
func (q *queue) Throttle() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()

	// Calculate the currently in-flight block requests
	pending := 0
	for _, request := range q.pendPool { //遍历针对每一个peer节点的fetchRequest请求包
		pending += len(request.Hashes) //累计总共需要对多少个区块进行检索(每个fetchRequest请求包包含对若干区块的检索请求)
	}
	// Throttle if more blocks are in-flight than free space in the cache
	return pending >= len(q.blockCache)-len(q.blockPool)
}

// Has checks if a hash is within the download queue or not.
// 检查区块的hash值是否已经在下载队列中
func (q *queue) Has(hash common.Hash) bool {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if _, ok := q.hashPool[hash]; ok { //检查区块是否已经在申请获取的检索挂起队列中
		return true
	}
	if _, ok := q.blockPool[hash]; ok { //检查区块是否在已下载区块池中
		return true
	}
	return false
}

// Insert adds a set of hashes for the download queue for scheduling, returning
// the new hashes encountered.
// 负责将形参指定的区块hash集合中的新区块hash插入到q.hashQueue相应位置处
// 1.忽略所有已经存在于q.hashPool挂起池中的所有区块hash
// 2.q.hashCounter作为插入q.hashQueue时的位置基准(根据fifo的bool值不同,分为两种插入方式？？？)
// 3.返回形参hash集合中所有未见的新区块(之前不存在于q.hashPool)的hash值(区块hash的顺序是逆向的,父区块hash在本区块hash的后面)
func (q *queue) Insert(hashes []common.Hash, fifo bool) []common.Hash {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Insert all the hashes prioritized in the arrival order
	inserts := make([]common.Hash, 0, len(hashes))
	for _, hash := range hashes {
		if old, ok := q.hashPool[hash]; ok { //忽略所有已经存在于q.hashPool挂起池中的所有区块hash
			glog.V(logger.Warn).Infof("Hash %x already scheduled at index %v", hash, old)
			continue
		}
		// Update the counters and insert the hash
		q.hashCounter = q.hashCounter + 1 //更新计数器
		inserts = append(inserts, hash)   //插入未见的新区块hash

		q.hashPool[hash] = q.hashCounter //将此新区块hash添加到q.hashPool挂起池(且对应的value值就是上述的q.hashCounter计数器)
		if fifo {                        //将此新区块hash插入到q.hashQueue相应位置处
			q.hashQueue.Push(hash, -float32(q.hashCounter)) // Lowest gets schedules first
		} else {
			q.hashQueue.Push(hash, float32(q.hashCounter)) // Highest gets schedules first
		}
	}
	return inserts //包含所有未见的新区块(之前不存在于q.hashPool)的hash值
}

// GetHeadBlock retrieves the first block from the cache, or nil if it hasn't
// been downloaded yet (or simply non existent).
// 返回q.blockCache集合中的第一个区块
func (q *queue) GetHeadBlock() *Block {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if len(q.blockCache) == 0 {
		return nil
	}
	return q.blockCache[0]
}

// GetBlock retrieves a downloaded block, or nil if non-existent.
// 从q.blockCache中获取并返回形参指定hash的区块
func (q *queue) GetBlock(hash common.Hash) *Block {
	q.lock.RLock()
	defer q.lock.RUnlock()

	// Short circuit if the block hasn't been downloaded yet
	index, ok := q.blockPool[hash] //检索对应hash的区块是否已经被下载,并获取其下标值
	if !ok {
		return nil
	}
	// Return the block if it's still available in the cache
	if q.blockOffset <= index && index < q.blockOffset+uint64(len(q.blockCache)) {
		return q.blockCache[index-q.blockOffset] //根据上述获取的index下标从q.blockCache集合中获取该区块
	}
	return nil
}

// TakeBlocks retrieves and permanently removes a batch of blocks from the cache.
// 将q.blockCache中保存的所有区块的记录从q.blockCache和q.blockPool中删除,同时更新q.blockOffset偏移量
// 返回值是本次从q.blockCache删除的所有区块组成的集合
func (q *queue) TakeBlocks() []*Block {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Accumulate all available blocks
	blocks := []*Block{}                 //存储q.blockCache中所有的区块
	for _, block := range q.blockCache { //遍历q.blockCache中所有存储的区块,将其hash值从q.blockPool集合中删除
		if block == nil {
			break
		}
		blocks = append(blocks, block)
		delete(q.blockPool, block.RawBlock.Hash())
	}
	// Delete the blocks from the slice and let them be garbage collected
	// without this slice trick the blocks would stay in memory until nil
	// would be assigned to q.blocks
	copy(q.blockCache, q.blockCache[len(blocks):]) //不太理解？？
	for k, n := len(q.blockCache)-len(blocks), len(q.blockCache); k < n; k++ {
		q.blockCache[k] = nil //清空本次q.blockCache队列中的所有区块
	}
	q.blockOffset += uint64(len(blocks)) //更新偏移量

	return blocks
}

// Reserve reserves a set of hashes for the given peer, skipping any previously
// failed download.
// 从q.hashQueue中取出待获取的区块hash值，组装成fetchRequest查询包记录到q.pendPool队列中
// 1.必须保证q.pendPool队列当前不存在针对对端peer的fetchRequest查询包
// 2.fetchRequest查询包具有最大限制数量,不能超过当前q.blockPool的容纳上限
// 3.查看此区块hash是否存在于p.ignored集合,根据结果分别加入skip集合或send集合-->
// 3.1 若存在于p.ignored集合,说明无法从对端peer获取此hash对应的区块,加入skip集合
// 3.2 不存在,说明可以从对端peer获取此hash对应的区块,加入send集合
// 4.将所有位于skip集合中的区块hash重新合并回q.hashQueue;利用send集合中的区块hash创建新的fetchRequest查询包,加入到q.pendPool中
func (q *queue) Reserve(p *peer, count int) *fetchRequest {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Short circuit if the pool has been depleted, or if the peer's already
	// downloading something (sanity check not to corrupt state)
	if q.hashQueue.Empty() {
		return nil
	}
	if _, ok := q.pendPool[p.id]; ok { //若当前节点存在向形参指定的peer节点发起过区块检索请求,则退出
		return nil
	}
	// Calculate an upper limit on the hashes we might fetch (i.e. throttling)
	//计算能额外申请获取的区块的上限
	space := len(q.blockCache) - len(q.blockPool) //计算出q.blockPool还可以容纳的区块哈希数
	for _, request := range q.pendPool {
		space -= len(request.Hashes) //向每一个peer发起fetchRequest都会请求获得多个区块,再剩余的就是可以额外申请获取的区块上限
	}
	// Retrieve a batch of hashes, skipping previously failed ones
	send := make(map[common.Hash]int)
	skip := make(map[common.Hash]int)

	for proc := 0; proc < space && len(send) < count && !q.hashQueue.Empty(); proc++ {
		hash, priority := q.hashQueue.Pop() //从hashQueue中取出一个需要获取的区块的hash值以及优先度
		if p.ignored.Has(hash) {            //查看此区块hash是否存在于p.ignored集合,根据结果分别加入skip集合或send集合
			skip[hash.(common.Hash)] = int(priority) //存在于p.ignored集合,说明无法从对端peer获取此hash对应的区块
		} else {
			send[hash.(common.Hash)] = int(priority) //不存在,说明可以从对端peer获取此hash对应的区块
		}
	}
	// Merge all the skipped hashes back
	for hash, index := range skip { //将所有位于skip集合中的区块hash重新合并回q.hashQueue
		q.hashQueue.Push(hash, float32(index))
	}
	// Assemble and return the block download request
	if len(send) == 0 {
		return nil
	}
	// 创建新的fetchRequest查询包,向对端peer申请获得send集合包含的所有区块
	request := &fetchRequest{
		Peer:   p,
		Hashes: send,
		Time:   time.Now(),
	}
	q.pendPool[p.id] = request //将查询包加入到q.pendPool中

	return request
}

// Cancel aborts a fetch request, returning all pending hashes to the queue.
// 丢弃一个fetchRequest查询包,将包中所有需查询的区块hash重新加入到q.hashQueue集合
func (q *queue) Cancel(request *fetchRequest) {
	q.lock.Lock()
	defer q.lock.Unlock()

	for hash, index := range request.Hashes { //将fetchRequest查询包中的区块hash重新返回到q.hashQueue集合
		q.hashQueue.Push(hash, float32(index))
	}
	delete(q.pendPool, request.Peer.id) //将此fetchRequest查询包从q.pendPool中删除
}

// Expire checks for in flight requests that exceeded a timeout allowance,
// canceling them and returning the responsible peers for penalization.
// 清除掉q.pendPool中所有的过期fetchRequest查询包,并将过期的查询包中的区块hash重新返回到q.hashQueue集合中
func (q *queue) Expire(timeout time.Duration) []string {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Iterate over the expired requests and return each to the queue
	peers := []string{}
	for id, request := range q.pendPool { //遍历每一条fetchRequest查询包
		if time.Since(request.Time) > timeout { //查看此查询包是否已经过期
			for hash, index := range request.Hashes { //将过期的查询包中的区块hash重新返回到q.hashQueue集合中
				q.hashQueue.Push(hash, float32(index))
			}
			peers = append(peers, id) //记录下过期查询包的目标peer的标识符
		}
	}
	// Remove the expired requests from the pending pool
	for _, id := range peers { //将过期的fetchRequest查询包从q.pendPool中删除
		delete(q.pendPool, id)
	}
	return peers
}

// Deliver injects a block retrieval response into the download queue.
// 将区块检索申请(fetchRequest查询包)获取的响应结果(从其他peer节点获取的block集合)注入到q.blockCache队列
// 1.从q.pendPool取出形参指定p.id的fetchRequest请求包request,接着在从q.pendPool删除该fetchRequest查询包
// 2.检查形参获取的响应结果(区块实体集合)是否为空,若为空说明此peer对象的本地区块链不包含这些指定hash的区块,需要在与此对等节点的peer对象中将这些区块哈希标注为不可获取(添加到p.ignored)
// 3.遍历所有获取的区块,检查其是否是fetchRequest查询包request的查询结果,合格后将该区块加入到q.blockCache队列(插入时必须保证是q.blockCache队列对区块是连续存储的)
// 4.将request查询包中剩余的未成功获取区块的hash重新加入到q.hashQueue队列中
func (q *queue) Deliver(id string, blocks []*types.Block) (err error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Short circuit if the blocks were never requested
	request := q.pendPool[id] //从q.pendPool取出形参指定p.id的fetchRequest请求包
	if request == nil {
		return errNoFetchesPending
	}
	delete(q.pendPool, id) //从q.pendPool删除对应的fetchRequest查询包

	// If no blocks were retrieved, mark them as unavailable for the origin peer
	if len(blocks) == 0 { //如果返回的结果block集合中无任何区块实体,则说明此peer对象的本地区块链不包含这些指定hash的区块
		for hash, _ := range request.Hashes {
			request.Peer.ignored.Add(hash) //需要在与此对等节点的peer对象中将这些区块哈希标注为不可获取
		}
	}
	// Iterate over the downloaded blocks and add each of them
	errs := make([]error, 0)       //负责存储所有不明区块的hash
	for _, block := range blocks { //遍历所有获取的区块,检查其是否是fetchRequest查询包request的查询结果,合格后将该区块加入到q.blockCache队列(必须保证是q.blockCache队列对区块是连续存储的)
		// Skip any blocks that were not requested
		hash := block.Hash()
		if _, ok := request.Hashes[hash]; !ok { //在记录的fetchRequest查询包中查询是否存在对该区块的查询行为
			errs = append(errs, fmt.Errorf("non-requested block %x", hash)) //将不明区块的hash值记录
			continue                                                        //如果不存在对此区块的查询记录,则进行下一区块的检查
		}
		// If a requested block falls out of the range, the hash chain is invalid
		index := int(int64(block.NumberU64()) - int64(q.blockOffset)) //获取该区块在q.blockCache队列中的下标
		if index >= len(q.blockCache) || index < 0 {                  //必须保证q.blockCache中区块是连续保存的
			return errInvalidChain
		}
		// Otherwise merge the block and mark the hash block
		q.blockCache[index] = &Block{ //将此区块加入到q.blockCache队列中
			RawBlock:   block,
			OriginPeer: id,
		}
		delete(request.Hashes, hash)
		delete(q.hashPool, hash)              //区块已经获取,将对应请求从请求池中删除
		q.blockPool[hash] = block.NumberU64() //在q.blockPool中保留此区块的哈希值,并记录该区块的编号作为value值
	}
	// Return all failed or missing fetches to the queue
	// 将request查询包中剩余的未成功获取区块的hash重新加入到q.hashQueue队列中
	for hash, index := range request.Hashes {
		q.hashQueue.Push(hash, float32(index))
	}
	// If none of the blocks were good, it's a stale delivery
	if len(errs) != 0 {
		if len(errs) == len(blocks) { //没有一个区块是正常的(来源不明)
			return errStaleDelivery
		}
		return fmt.Errorf("multiple failures: %v", errs)
	}
	return nil
}

// Prepare configures the block cache offset to allow accepting inbound blocks.
// 重新配置q.blockOffset偏移量(仅在 q.blockOffset < 形参给定的偏移量 时才会进行重新配置)
func (q *queue) Prepare(offset uint64) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.blockOffset < offset {
		q.blockOffset = offset
	}
}
