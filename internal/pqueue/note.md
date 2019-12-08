### 在 Go 中如何实现自定类型的 heap/Priority Queue 结构

1. 官网文档: [Package heap](https://golang.org/pkg/container/heap/)
2. Go 提供了 `container/heap`包实现 heap。`heap`可以为任何类型提供 head 操作，只需要该类型实现了 `heap.Interface`接口。该接口一共包含五个方法：
   1. `sort`接口包含三个方法: `Len() int`、`Less(i, j int) bool`、`Swap(i, j int)`
   2. heap 结构必须的两个操作: `Push(x interface{})`、`Pop() interface{}`
3. `heap.Interface`接口

```go
type Interface interface {
    sort.Interface
    Push(x interface{}) // add x as element Len()
    Pop() interface{}   // remove and return element Len() - 1.
}
```

其中，`sort`接口包含三个要实现的方法:

```go
type Interface interface {
    // Len is the number of elements in the collection.
    Len() int
    // Less reports whether the element with
    // index i should sort before the element with index j.
    Less(i, j int) bool
    // Swap swaps the elements with indexes i and j.
    Swap(i, j int)
}
```

4. heap 包提供的 heap 操作方法

```go
  func Fix(h Interface, i int)
  func Init(h Interface)
  func Pop(h Interface) interface{}
  func Push(h Interface, x interface{})
  func Remove(h Interface, i int) interface{}
```

5. 看提供的 example，可以发现`pop`方法似乎是在移除最后一个元素，跟我们学过的堆结构不太一样，我们认为 heap 的最小值在数组索引 0 的位置, 而且这个`pop`操作之后也没有进行堆结构调整:

```go
func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
```

实际上，之所以这样写，是因为这个`Pop`操作并不是我们进行弹出堆顶元素的唯一的操作，不要忘了我们是通过`heap`包提供的`Pop`操作，看一下`heap`包的`Pop`代码就会明白这样做的道理了:

```go
// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// Pop is equivalent to Remove(h, 0).
func Pop(h Interface) interface{} {
	n := h.Len() - 1
	h.Swap(0, n)
	down(h, 0, n)
	return h.Pop()
}
```

可以看到，`heap`包的`Pop`方法，先调整了堆的大小，然后把堆的底层数组的第一个元素和最后一个元素交换了位置，然后进行了堆结构的调整，最后调用了我们的代码.
因此，调用我们的`Pop`方法时，数组的最后一个元素已经是之前的堆顶了，只需要把它取出来，再把最后一个位置移除即可。

6. `Push` 方法同理，但是更简单一点，我们自定义的接口实现中，只需要把它放到切片最后，然后`heap`包的`Push`方法会进行堆结构调整

```go
// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func Push(h Interface, x interface{}) {
	h.Push(x)
	up(h, h.Len()-1)
}
```

7. `internal/pqueue/pqueue.go` 实现的 pqueue 和官方提供的 example 的区别， 主要就是考虑效率问题
   1. `Push`方法中考虑了扩容问题
   2. `Pop`方法中考虑了缩容问题
   3. 多了一个方法 `PeekAndShift`: `PeekAndShift` 根据 传入的 max 值返回对应的元素
      1. 如果堆为空, 返回 `nil, 0`， nil 代表没有找到元素，第二个返回值可以区分是因为堆空没找到还是因为堆中最小值都比 max 大而不满足条件
      2. 如果堆中最小值都比 max 大，返回 `nil, item.Priority - max`
      3. 否则返回堆顶元素 `item, 0`
