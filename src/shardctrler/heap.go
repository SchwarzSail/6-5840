package shardctrler

type GidShardsMapping struct {
	Gid int
	ShardCount int
}

type Heap struct {
	array []*GidShardsMapping
}

func NewHeap() *Heap {
	return &Heap{array: make([]*GidShardsMapping, 0)}
}

func(h *Heap) less(i, j int) bool {
	if h.array[i].ShardCount != h.array[j].ShardCount {
		return h.array[i].ShardCount < h.array[j].ShardCount
	}
	return h.array[i].Gid < h.array[j].Gid
}

func(h *Heap) swap(i, j int) {
	h.array[i], h.array[j] = h.array[j], h.array[i]
}

func(h *Heap) update(gid, newShardCount int) {
	for k, v := range h.array {
		if v.Gid == gid {
			h.array[k].ShardCount = newShardCount
			h.heapifyDown(k)
			h.heapifyUp(k)
			return
		}
	}
	panic("")
}

func (h *Heap) heapifyUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if h.less(parent, index) {
			break
		}
		h.swap(index, parent)
		index = parent
	}
}

func (h *Heap) heapifyDown(index int) {
	for {
		left := 2 * index + 1
		right := 2 * index + 2
		smallest := index
		if left < len(h.array) && h.less(left, smallest) {
			smallest = left
		}
		if right < len(h.array) && h.less(right, smallest) {
			smallest = right
		}
		if smallest == index {
			break
		}
		h.swap(index, smallest)
		index = smallest
	}
}

//return the first element in the heap, it's the smallest
func (h *Heap) pop() *GidShardsMapping {
	if len(h.array) == 0 {
		panic("")
	}
	res := h.array[0]
	h.array[0] = h.array[len(h.array) - 1]
	h.array = h.array[:len(h.array) - 1]
	h.heapifyDown(0)
	return res
}

func(h *Heap) push(e *GidShardsMapping) {
	h.array = append(h.array, e)
	h.heapifyUp(len(h.array) - 1)
}