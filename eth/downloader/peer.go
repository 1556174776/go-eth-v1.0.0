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

// Contains the active peer-set of the downloader, maintaining both failures
// as well as reputation metrics to prioritize the block retrievals.

package downloader

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"GO_Demo/go-ethereum/common"
	"GO_Demo/go-ethereum/set"
)

type relativeHashFetcherFn func(common.Hash) error
type absoluteHashFetcherFn func(uint64, int) error
type blockFetcherFn func([]common.Hash) error

var (
	errAlreadyFetching   = errors.New("already fetching blocks from peer")
	errAlreadyRegistered = errors.New("peer is already registered")
	errNotRegistered     = errors.New("peer is not registered")
)

// peer represents an active peer from which hashes and blocks are retrieved.
type peer struct {
	id   string      // Unique identifier of the peer   对等节点的标识符
	head common.Hash // Hash of the peers latest known block  对等节点最新已知块的哈希

	idle int32 // Current activity state of the peer (idle = 0, active = 1)  对等节点的当前活跃状态(若闲置(尚未向其发送request)==0,活跃(正在等待对方对于request的回应)==1)
	rep  int32 // Simple peer reputation   用来衡量一个peer节点的对区块检索请求的响应能力,响应越慢该值越小

	capacity int32     // Number of blocks allowed to fetch per request  每个FetcheRequest请求包中可包含的区块hash个数
	started  time.Time // Time instance when the last fetch was started  发出上一个fetch区块请求的时间点

	ignored *set.Set // Set of hashes not to request (didn't have previously)  存储所有不可从此对等节点获取的区块的hash值

	getRelHashes relativeHashFetcherFn // Method to retrieve a batch of hashes from an origin hash   根据给定的原始区块hash检索获取一批相关哈希值
	getAbsHashes absoluteHashFetcherFn // Method to retrieve a batch of hashes from an absolute position  根据给定的绝对位置检索获取一批区块哈希值
	getBlocks    blockFetcherFn        // Method to retrieve a batch of blocks   检索获取一批区块哈希值

	version int // Eth protocol version number to switch strategies
}

// newPeer create a new downloader peer, with specific hash and block retrieval
// mechanisms.
// 创建一个新的downloader peer对象,伴随有特殊的区块哈希和区块检索机制
func newPeer(id string, version int, head common.Hash, getRelHashes relativeHashFetcherFn, getAbsHashes absoluteHashFetcherFn, getBlocks blockFetcherFn) *peer {
	return &peer{
		id:           id,
		head:         head,
		capacity:     1,
		getRelHashes: getRelHashes,
		getAbsHashes: getAbsHashes,
		getBlocks:    getBlocks,
		ignored:      set.New(),
		version:      version,
	}
}

// Reset clears the internal state of a peer entity.
// 清除对等peer的内部状态(p.idle == 0,p.capacity == 1,清除整个p.ignored记录的哈希值)
func (p *peer) Reset() {
	atomic.StoreInt32(&p.idle, 0)
	atomic.StoreInt32(&p.capacity, 1)
	p.ignored.Clear()
}

// Fetch sends a block retrieval request to the remote peer.
// 向远端peer发送一个区块检索请求
func (p *peer) Fetch(request *fetchRequest) error {
	// Short circuit if the peer is already fetching
	if !atomic.CompareAndSwapInt32(&p.idle, 0, 1) { //p.idle必须为0或1中的一种状态
		return errAlreadyFetching
	}
	p.started = time.Now()

	// Convert the hash set to a retrievable slice
	hashes := make([]common.Hash, 0, len(request.Hashes))
	for hash, _ := range request.Hashes { //将request请求包中的所有区块hash记录保存
		hashes = append(hashes, hash)
	}
	go p.getBlocks(hashes) //向对端peer申请获取这些区块

	return nil
}

// SetIdle sets the peer to idle, allowing it to execute new retrieval requests.
// Its block retrieval allowance will also be updated either up- or downwards,
// depending on whether the previous fetch completed in time or not.
// 根据节点上次的响应速度重新设置peer节点的p.capacity,同时将p.idle设置为0(peer节点重回闲置状态)
func (p *peer) SetIdle() {
	// Update the peer's download allowance based on previous performance
	// 根据上次的响应速度更新对此peer节点的request请求包能够容纳的请求上限数
	scale := 2.0
	if time.Since(p.started) > blockSoftTTL { //检查距离上次发送request请求包是否已经超过了规定的 blockSoftTTL
		scale = 0.5
		if time.Since(p.started) > blockHardTTL { //检查距离上次发送request请求包是否已经超过了规定的 blockHardTTL
			scale = 1 / float64(MaxBlockFetch) // reduces capacity to 1
		}
	}
	for {
		// Calculate the new download bandwidth allowance
		prev := atomic.LoadInt32(&p.capacity)                                             //获取当前使用的request请求包能够容纳的请求上限数
		next := int32(math.Max(1, math.Min(float64(MaxBlockFetch), float64(prev)*scale))) //根据响应速度获取未来应该使用的上限数

		// Try to update the old value
		if atomic.CompareAndSwapInt32(&p.capacity, prev, next) { //重新设定p.capacity
			// If we're having problems at 1 capacity, try to find better peers
			if next == 1 { //若发现计算获得的next==1
				p.Demote() //将将对应peer节点的p.rep(衡量响应速度的标志位)减半
			}
			break
		}
	}
	// Set the peer to idle to allow further block requests
	atomic.StoreInt32(&p.idle, 0) //将节点状态设置为闲置状态
}

// Capacity retrieves the peers block download allowance based on its previously
// discovered bandwidth capacity.
// 检索返回peer节点一次能够检索的区块数量(检索能力)
func (p *peer) Capacity() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Promote increases the peer's reputation.
// 将对应peer节点的p.rep + 1
func (p *peer) Promote() {
	atomic.AddInt32(&p.rep, 1)
}

// Demote decreases the peer's reputation or leaves it at 0.
// 将对应peer节点的p.rep减半
func (p *peer) Demote() {
	for {
		// Calculate the new reputation value
		prev := atomic.LoadInt32(&p.rep)
		next := prev / 2

		// Try to update the old value
		if atomic.CompareAndSwapInt32(&p.rep, prev, next) {
			return
		}
	}
}

// String implements fmt.Stringer.
func (p *peer) String() string {
	return fmt.Sprintf("Peer %s [%s]", p.id,
		fmt.Sprintf("reputation %3d, ", atomic.LoadInt32(&p.rep))+
			fmt.Sprintf("capacity %3d, ", atomic.LoadInt32(&p.capacity))+
			fmt.Sprintf("ignored %4d", p.ignored.Size()),
	)
}

// peerSet represents the collection of active peer participating in the block
// download procedure.
// 包含参与块下载过程的活动对等peer的集合。
type peerSet struct {
	peers map[string]*peer
	lock  sync.RWMutex
}

// newPeerSet creates a new peer set top track the active download sources.
// 创建一个新的peerSet集合对象
func newPeerSet() *peerSet {
	return &peerSet{
		peers: make(map[string]*peer),
	}
}

// Reset iterates over the current peer set, and resets each of the known peers
// to prepare for a next batch of block retrieval.
// 重置peerSet集合中所有peer节点,以准备下一批区块检索
func (ps *peerSet) Reset() {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	for _, peer := range ps.peers {
		peer.Reset()
	}
}

// Register injects a new peer into the working set, or returns an error if the
// peer is already known.
// 将新的对等peer节点加入到本地ps.peers集合中(key为p.id,value为peer对象)
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
// 将指定p.id的peer节点从ps.peers集合中删除
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
// 从ps.peers集合检索并返回指定p.id的peer节点实体
func (ps *peerSet) Peer(id string) *peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return ps.peers[id]
}

// Len returns if the current number of peers in the set.
// 返回ps.peers集合保存的peer节点的个数
func (ps *peerSet) Len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.peers)
}

// AllPeers retrieves a flat list of all the peers within the set.
// 返回ps.peers集合中保存的所有peer节点对象
func (ps *peerSet) AllPeers() []*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		list = append(list, p)
	}
	return list
}

// IdlePeers retrieves a flat list of all the currently idle peers within the
// active peer set, ordered by their reputation.
// 检索活动对等peer集合中所有当前空闲(p.idle==0)对等peer，按其信誉(p.req)从大到小排序,最终返回排序后的peer集合
func (ps *peerSet) IdlePeers() []*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*peer, 0, len(ps.peers))
	for _, p := range ps.peers { //遍历peerSet集合中的每一个peer节点
		if atomic.LoadInt32(&p.idle) == 0 { //筛选出p.idle==0(即处于闲置状态)的节点,添加到list集合
			list = append(list, p)
		}
	}
	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ { //按照p.req的大小对list集合中的peer节点进行从大到小的排序
			if atomic.LoadInt32(&list[i].rep) < atomic.LoadInt32(&list[j].rep) {
				list[i], list[j] = list[j], list[i]
			}
		}
	}
	return list
}
