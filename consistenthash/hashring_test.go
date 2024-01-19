package consistenthash

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aliexpressru/gomemcached/utils"
)

const (
	keySize     = 20
	requestSize = 1000
)

func BenchmarkHashRingGet(b *testing.B) {
	ch := NewHashRing()
	for i := 0; i < keySize; i++ {
		ch.Add("localhost:" + strconv.Itoa(i))
	}

	for i := 0; i < b.N; i++ {
		ch.Get(i)
	}
}

func TestHashRing_GetAllNodes(t *testing.T) {
	ch := NewHashRing()

	for i := 0; i < keySize; i++ {
		ch.Add("localhost:" + strconv.Itoa(i))
	}
	count := ch.GetNodesCount()
	assert.Equalf(t, keySize, count, "GetNodesCount: have - %d; want - %d", count, keySize)

	allNodes := ch.GetAllNodes()
	assert.Equal(t, keySize, len(allNodes))

	for i := 0; i < keySize; i++ {
		node := "localhost:" + strconv.Itoa(i)
		found := false
		for _, n := range allNodes {
			if n == node {
				found = true
				break
			}
		}
		assert.True(t, found, "Node not found in GetAllNodes: "+node)
	}
}

func TestHashRingWithEntropy(t *testing.T) {
	ch := NewCustomHashRing(0, nil)
	val, ok := ch.Get("any")
	assert.False(t, ok)
	assert.Nil(t, val)

	for i := 0; i < keySize; i++ {
		ch.AddWithReplicas("localhost:"+strconv.Itoa(i), minReplicas<<1)
	}

	keys := make(map[string]int)
	for i := 0; i < requestSize; i++ {
		key, ok := ch.Get(requestSize + i)
		assert.True(t, ok)
		keys[key.(string)]++
	}

	mi := make(map[any]int, len(keys))
	for k, v := range keys {
		mi[k] = v
	}
	entropy := utils.CalcEntropy(mi)
	assert.True(t, entropy > .95)
}

func TestHashRingIncrementalTransfer(t *testing.T) {
	prefix := "anything"
	create := func() *HashRing {
		ch := NewHashRing()
		for i := 0; i < keySize; i++ {
			ch.Add(prefix + strconv.Itoa(i))
		}
		return ch
	}

	originCh := create()
	keys := make(map[int]string, requestSize)
	for i := 0; i < requestSize; i++ {
		key, ok := originCh.Get(requestSize + i)
		assert.True(t, ok)
		assert.NotNil(t, key)
		keys[i] = key.(string)
	}

	node := fmt.Sprintf("%s%d", prefix, keySize)
	for i := 0; i < 10; i++ {
		laterCh := create()
		laterCh.AddWithWeight(node, 10*(i+1))

		for j := 0; j < requestSize; j++ {
			key, ok := laterCh.Get(requestSize + j)
			assert.True(t, ok)
			assert.NotNil(t, key)
			value := key.(string)
			assert.True(t, value == keys[j] || value == node)
		}
	}
}

func TestHashRingTransferOnFailure(t *testing.T) {
	index := 41
	keys, newKeys := getKeysBeforeAndAfterFailure(t, "localhost:", index)
	var transferred int
	for k, v := range newKeys {
		if v != keys[k] {
			transferred++
		}
	}

	ratio := float32(transferred) / float32(requestSize)
	assert.True(t, ratio < 2.5/float32(keySize), fmt.Sprintf("%d: %f", index, ratio))
}

func TestHashRingLeastTransferOnFailure(t *testing.T) {
	prefix := "localhost:"
	index := 41
	keys, newKeys := getKeysBeforeAndAfterFailure(t, prefix, index)
	for k, v := range keys {
		newV := newKeys[k]
		if v != prefix+strconv.Itoa(index) {
			assert.Equal(t, v, newV)
		}
	}
}

func TestHashRing_Remove(t *testing.T) {
	ch := NewHashRing()
	ch.Add("first")
	ch.Add("second")
	ch.Remove("first")
	for i := 0; i < 100; i++ {
		val, ok := ch.Get(i)
		assert.True(t, ok)
		assert.Equal(t, "second", val)
	}
}

func TestHashRing_RemoveInterface(t *testing.T) {
	const key = "any"
	ch := NewHashRing()
	node1 := newMockNode(key, 1)
	node2 := newMockNode(key, 2)
	ch.AddWithWeight(node1, 80)
	ch.AddWithWeight(node2, 50)
	assert.Equal(t, 1, len(ch.nodes))
	node, ok := ch.Get(1)
	assert.True(t, ok)
	assert.Equal(t, key, node.(*mockNode).addr)
	assert.Equal(t, 2, node.(*mockNode).id)
}

func getKeysBeforeAndAfterFailure(t *testing.T, prefix string, index int) (map[int]string, map[int]string) {
	ch := NewHashRing()
	for i := 0; i < keySize; i++ {
		ch.Add(prefix + strconv.Itoa(i))
	}

	keys := make(map[int]string, requestSize)
	for i := 0; i < requestSize; i++ {
		key, ok := ch.Get(requestSize + i)
		assert.True(t, ok)
		assert.NotNil(t, key)
		keys[i] = key.(string)
	}

	remove := fmt.Sprintf("%s%d", prefix, index)
	ch.Remove(remove)
	newKeys := make(map[int]string, requestSize)
	for i := 0; i < requestSize; i++ {
		key, ok := ch.Get(requestSize + i)
		assert.True(t, ok)
		assert.NotNil(t, key)
		assert.NotEqual(t, remove, key)
		newKeys[i] = key.(string)
	}

	return keys, newKeys
}

type mockNode struct {
	addr string
	id   int
}

func newMockNode(addr string, id int) *mockNode {
	return &mockNode{
		addr: addr,
		id:   id,
	}
}

func (n *mockNode) String() string {
	return n.addr
}
