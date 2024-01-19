package consistenthash

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/aliexpressru/gomemcached/utils"
)

const (
	// TopWeight is the top weight that one entry might set.
	TopWeight = 100

	minReplicas = 256
	prime       = 18245165
)

var _ ConsistentHash = (*HashRing)(nil)

type (
	ConsistentHash interface {
		Add(node any)
		AddWithReplicas(node any, replicas int)
		AddWithWeight(node any, weight int)
		Get(v any) (any, bool)
		GetAllNodes() []any
		Remove(node any)
		GetNodesCount() int
	}

	// Func defines the hash method.
	Func func(data []byte) uint64

	// A HashRing is implementation of consistent hash.
	HashRing struct {
		hashFunc Func
		replicas int
		keys     []uint64
		ring     map[uint64][]any
		nodes    map[string]struct{}
		lock     sync.RWMutex
	}
)

// NewHashRing returns a HashRing.
func NewHashRing() *HashRing {
	return NewCustomHashRing(minReplicas, Hash)
}

// NewCustomHashRing returns a HashRing with given replicas and hash func.
func NewCustomHashRing(replicas int, fn Func) *HashRing {
	if replicas < minReplicas {
		replicas = minReplicas
	}

	if fn == nil {
		fn = Hash
	}

	return &HashRing{
		hashFunc: fn,
		replicas: replicas,
		ring:     make(map[uint64][]any),
		nodes:    make(map[string]struct{}),
	}
}

// Add adds the node with the number of h.replicas,
// the later call will overwrite the replicas of the former calls.
func (h *HashRing) Add(node any) {
	h.AddWithReplicas(node, h.replicas)
}

// AddWithReplicas adds the node with the number of replicas,
// replicas will be truncated to h.replicas if it's larger than h.replicas,
// the later call will overwrite the replicas of the former calls.
func (h *HashRing) AddWithReplicas(node any, replicas int) {
	h.Remove(node)

	if replicas > h.replicas {
		replicas = h.replicas
	}

	nodeRepr := repr(node)
	h.lock.Lock()
	defer h.lock.Unlock()
	h.addNode(nodeRepr)

	for i := 0; i < replicas; i++ {
		hash := h.hashFunc([]byte(replicaRepr(nodeRepr, i)))
		h.keys = append(h.keys, hash)
		h.ring[hash] = append(h.ring[hash], node)
	}

	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

// AddWithWeight adds the node with weight, the weight can be 1 to 100, indicates the percent,
// the later call will overwrite the replicas of the former calls.
func (h *HashRing) AddWithWeight(node any, weight int) {
	// don't need to make sure weight not larger than TopWeight,
	// because AddWithReplicas makes sure replicas cannot be larger than h.replicas
	replicas := h.replicas * weight / TopWeight
	h.AddWithReplicas(node, replicas)
}

// Get returns the corresponding node from h base on the given v.
func (h *HashRing) Get(v any) (any, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	if len(h.ring) == 0 {
		return nil, false
	}

	hash := h.hashFunc([]byte(repr(v)))
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)

	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		return nil, false
	case 1:
		return nodes[0], true
	default:
		innerIndex := h.hashFunc([]byte(innerRepr(v)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

// GetAllNodes returns all nodes used in hash ring
//
//	return a slice with a string representation of the nodes
func (h *HashRing) GetAllNodes() []any {
	h.lock.RLock()
	defer h.lock.RUnlock()

	if len(h.ring) == 0 {
		return nil
	}

	var (
		allNodes = make([]any, 0, len(h.nodes))
		uqNodes  = make(map[any]struct{}, len(h.nodes))
	)

	for _, nodes := range h.ring {
		for _, node := range nodes {
			if _, ok := uqNodes[node]; !ok {
				allNodes = append(allNodes, node)
				uqNodes[node] = struct{}{}
			}
		}
	}

	return allNodes
}

// Remove removes the given node from h.
func (h *HashRing) Remove(node any) {
	nodeRepr := repr(node)

	h.lock.Lock()
	defer h.lock.Unlock()

	if !h.containsNode(nodeRepr) {
		return
	}

	for i := 0; i < h.replicas; i++ {
		hash := h.hashFunc([]byte(replicaRepr(nodeRepr, i)))
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})
		if index < len(h.keys) && h.keys[index] == hash {
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		h.removeRingNode(hash, nodeRepr)
	}

	h.removeNode(nodeRepr)
}

// GetNodesCount returns the current number of nodes
func (h *HashRing) GetNodesCount() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return len(h.nodes)
}

func (h *HashRing) removeRingNode(hash uint64, nodeRepr string) {
	if nodes, ok := h.ring[hash]; ok {
		newNodes := nodes[:0]
		for _, x := range nodes {
			if repr(x) != nodeRepr {
				newNodes = append(newNodes, x)
			}
		}
		if len(newNodes) > 0 {
			h.ring[hash] = newNodes
		} else {
			delete(h.ring, hash)
		}
	}
}

func (h *HashRing) addNode(nodeRepr string) {
	h.nodes[nodeRepr] = struct{}{}
}

func (h *HashRing) containsNode(nodeRepr string) bool {
	_, ok := h.nodes[nodeRepr]
	return ok
}

func (h *HashRing) removeNode(nodeRepr string) {
	delete(h.nodes, nodeRepr)
}

func innerRepr(node any) string {
	return fmt.Sprintf("%d:%v", prime, node)
}

func repr(node any) string {
	return utils.Repr(node)
}

func replicaRepr(nodeRepr string, replicaNumber int) string {
	return fmt.Sprintf("%s_virtual%s", nodeRepr, strconv.Itoa(replicaNumber))
}
