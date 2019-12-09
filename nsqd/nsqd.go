package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

// Client 接口
type Client interface {
	Stats() ClientStats 	// Client 统计信息
	IsProducer() bool			// 是否为生产者
}

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex

	opts atomic.Value

	dl        *dirlock.DirLock
	isLoading int32
	errValue  atomic.Value
	startTime time.Time

	topicMap map[string]*Topic

	clientLock sync.RWMutex
	clients    map[int64]Client

	lookupPeers atomic.Value

	tcpServer     *tcpServer
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int

	notifyChan           chan interface{}
	optsNotificationChan chan struct{}
	exitChan             chan int
	waitGroup            util.WaitGroupWrapper

	ci *clusterinfo.ClusterInfo
}

// New 创建一个NSQD实例
// 1. 检测配置中是否传入了DataPath， 如果没有，就获取当前程序的路径，赋值给dataPath
// 2. 检查配置中是否传入了Logger， 如果没有，生成一个默认的
// 3. 创建实例，为 启动时间 `startTime`、 目录锁 `dl` 和 相关的 channel 赋初始值
func New(opts *Options) (*NSQD, error) {
	var err error

	// 1. 检测配置中是否传入了DataPath， 如果没有，就获取当前程序的路径，赋值给dataPath
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	// 2. 检查配置中是否传入了Logger， 如果没有，生成一个默认的
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	// 3. 创建实例，为 启动时间 `startTime`、 目录锁 `dl` 和 相关的 channel 赋初始值
	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		clients:              make(map[int64]Client),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}

	// 4. 创建一个 httpClient, 并使用其初始化 NSQD的 `clusterinfo.ClusterInfo`属性 `n.ci`
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)


	// 5. 初始化三个 atomic.Value 变量
	n.lookupPeers.Store([]*lookupPeer{})
	n.swapOpts(opts) 	// n.opts.Store(opts)
	n.errValue.Store(errStore{})

	// 5. 目录锁dl锁住，进行一系列操作
	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	// 5.1  MaxDeflateLevel 参数检验，目前不知道这个参数什么意思(TODO)
	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	// 5.2 ID 参数检验，must be [0,1024)
	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}

	// 5.3 StatsdPrefix 如果不为空，需要根据前缀构建一个StatsdPrefix
	if opts.StatsdPrefix != "" {
		var port string
		_, port, err = net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
		}
		// statsdHostKey = opts.BroadcastAddress + port
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		// 参数中给定的statsdHostKey是带有占位符 `%s` 的，将占位符替换成 `opts.BroadcastAddress + port`
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	// 5.4 设置 `opts.TLSRequired`
	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	// 5.5 根据设置好的 `opts` 构建 `tlsConfig`
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	// 5.6 E2EProcessingLatencyPercentiles 参数校验，目前不知道什么意思(TODO)
	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	// 5.7 创建 Server 和 各种 Listener
	n.tcpServer = &tcpServer{}
	n.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	n.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}

	return n, nil
}

// getOpts 从 atomic.Value 变量中获取opts
func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// swapOpts 设置 opts
func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

// triggerOptsNotification 监测 n.optsNotificationChan， 获取 opts 变更信息
func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

// RealTCPAddr 返回 TCP 地址
func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	return n.tcpListener.Addr().(*net.TCPAddr)
}

// RealHTTPAddr 返回 HTTP 地址
func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	return n.httpListener.Addr().(*net.TCPAddr)
}

// RealHTTPSAddr 返回 HTTPS 地址
func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	return n.httpsListener.Addr().(*net.TCPAddr)
}

// SetHealth 设置 atomic.Value `n.errValue`, 将错误信息存进去
func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

// IsHealthy 返回 节点是否健康，具体而言，就是查看 `n.errValue`中是否存在错误值
func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

// GetError 返回 n.errValue 的值， 如果有错误，这个n.errValue中存储了包含错误信息的结构体
func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

// GetHealth 返回一个字符串表示节点的健康信息，如果正常返回"OK", 不正常返回 错误信息
func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

// GetStartTime 返回 `n.startTime`
func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// AddClient 将一个 Client 添加到 `n.clients` map中，注意：该map是并发访问的，要加锁
func (n *NSQD) AddClient(clientID int64, client Client) {
	n.clientLock.Lock()
	n.clients[clientID] = client
	n.clientLock.Unlock()
}

// RemoveClient 将一个 Client 从 `n.clients` map 移除，首先检查是否确实存在，存在的话移除。注意：该map是并发访问的，要加锁
func (n *NSQD) RemoveClient(clientID int64) {
	n.clientLock.Lock()
	_, ok := n.clients[clientID]
	if !ok {
		n.clientLock.Unlock()
		return
	}
	delete(n.clients, clientID)
	n.clientLock.Unlock()
}

// Main 应该是 NSQD 的主逻辑，主要完成了以下几点工作:
// 1. 启动了服务器：包括 TCPServer、HTTPServer/HTTPSServer
// 2. 启动 `queueScanLoop`， 队列扫描循环
// 3. 启动 `lookupLoop`，监控lookupd注册信息服务
// 4. 启动 `statsdLoop`, 监控数据收集循环
// 5. 阻塞，除非收到了退出信号
func (n *NSQD) Main() error {
	ctx := &context{n}

	exitCh := make(chan error)
	var once sync.Once
	// 如果没有错误，exitFunc传进去的应该是个nil
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// 1. 启动了服务器：包括 TCPServer、HTTPServer/HTTPSServer
	n.tcpServer.ctx = ctx
	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})

	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
	n.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
	})

	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
		httpsServer := newHTTPServer(ctx, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}

	// 2. 启动 `queueScanLoop`， 队列扫描循环
	n.waitGroup.Wrap(n.queueScanLoop)
	// 3. 启动 `lookupLoop`，监控lookupd注册信息服务
	n.waitGroup.Wrap(n.lookupLoop)
	// 4. 启动 `statsdLoop`, 监控数据收集循环
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(n.statsdLoop)
	}

	// 5. 阻塞，除非收到了退出信号
	err := <-exitCh
	return err
}

type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"`
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"`
		} `json:"channels"`
	} `json:"topics"`
}

// newMetadataFile 返回 MetadataFile 的文件路径，为：配置文件中的参数	`opts.DataPath` + "nsqd.dat"
func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

// readOrEmpty 读取文件名称为 fn 的文件，返回文件内容的 byte切片
// fn: filename
func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

// writeSyncFile 将数据 `data` 写到文件 `fn` 中，同时刷新到磁盘 `f.Sync()`
func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// LoadMetadata 加载元数据信息, 具体操作如下:
// 1. 读取元数据文件, 将 `n.isLoading` 置为 1， 然后读取，如果为空说明没有元数据信息(这种不算错误)
// 2. 反序列化文件中的内容到结构体 `meta` m 中
// 3. 遍历meta数据，结合lookupd查询topic和channel的状态，并启动topic
func (n *NSQD) LoadMetadata() error {
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)

	fn := newMetadataFile(n.getOpts())

	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, t := range m.Topics {
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		topic := n.GetTopic(t.Name)
		if t.Paused {
			topic.Pause()
		}
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
		topic.Start()
	}
	return nil
}

// PersistMetadata 将元数据进行持久化，包括topics和channels
// TODO 有一个疑问：为什么channel是一进去就lock，而topic不是
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

// Exit 退出 NSQD， 完成以下操作
// 1. 关闭服务器相关的资源: tcpListener、tcpServer、httpListener、httpsListener
// 2. 持久化元数据 (需要加锁)
// 3. 关闭exitChan，发送退出信号
// 4. 等待其他协程完成工作
// 5. 释放目录锁 dl
func (n *NSQD) Exit() {
	// 1. 关闭服务器相关的资源
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}
	if n.tcpServer != nil {
		n.tcpServer.CloseAll()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	// 2. 持久化元数据
	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	// 3. 关闭exitChan，发送退出信号
	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)

	// 4. 等待其他协程完成工作
	n.waitGroup.Wait()

	// 5. 释放目录锁 dl
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
// GetTopic 返回一个指向给定 `topicName` 的 topic 对象 (可能是一个新创建的)， 具体的执行流程如下:
// 1. 加读锁，先从本地的缓存 `topicMap` 中取, 如果取到了直接返回这个topic
// 2. 如果本地缓存没取到，准备加锁创建，但是在此之前还是先从本地的缓存中取(因为有可能释放读锁，加写锁这段时间有缓存了)
// 3. 创建一个并放到本地缓存中，NewTopic(topicName, &context{n}, deleteCallback), 释放写锁
// 4. 检查当前状态是否为正在加载元数据 `n.isLoading`， 如果是，先返回，因为加载元数据完成后topic会启动
// 5. 如果有 `lookupd` 服务，去查询对应的 `channel` 信息，把新创建的 topic 构建完整
// 6. 启动 topic
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock()

	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}

	// 3. 创建一个并放到本地缓存中
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if loading metadata at startup, no lookupd connections yet, topic started after load
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			}
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	t.Start()
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// Notify 新开协程执行持久化元数据操作
func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		// TODO 谁在监听这个通道
		case n.notifyChan <- v:
			if !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
// 遍历 `topicMap` 拿到所有 `topic`, 然后再取每个 `topic` 的所有 `channel`
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
// queueScanWorker 其实就是开个协程去处理队列中的数据，它监测两个通道：
// 1. workCh: 当有通道进入需要处理
// 2. workCh: 缩小pool of queueScanWorker时，会往closeCh发东西，收到该信号的协程就退出当前协程
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			// TODO 判断channel是否为dirty的核心方法，在 `channel.go` 文件中
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	// 如果 dirty的百分比大于定义的阈值，执行这个循环
	loop:
		// 从 len(channels) 个 channel 中随机选择 num 个加入workCh
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			// 如果 responseCh 得到的是 true，说明被标记为 dirty
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}
