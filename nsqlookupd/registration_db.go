package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RegistrationDB 保存了注册信息，继承了读写锁，包含一个map，map里面存储了注册信息
type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

// Registration 用于维护注册信息，从调用情况来看：
// Category 表示注册的类别，已经看到的类别有
// 	 	`topic` 表示注册的主题信息
// 	 	`channel` 表示注册的通道信息
// 	 	`client` 表示注册的是节点node信息
// Key SubKey都使用的情况下只有 Key用于表示topic名称， SubKey表示topic下的channel名称，是否还有其他的使用方式还需要再看源码
type Registration struct {
	Category string
	Key      string
	SubKey   string
}

// Registrations 表示 一些列Registration的切片
type Registrations []Registration

// PeerInfo 用来维护节点信息
type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// Producer 维护生产者信息，包括：生产者节点信息 peerInfo、 tombstoned状态、tombstonedAt时间
type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

// Producers 表示一系列Producer构成的切片
type Producers []*Producer

// ProducerMap 相当于维护了Producer的索引信息， 将一个字符串Key (produecer.peerInfo.id) 映射到实际的Producer，这个结构在RegistrationDB中作为一个属性用到了
type ProducerMap map[string]*Producer

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

// Tombstone 设置 Producer 的 Tombstone 状态，包括将Tombstone状态置为true， 并把成为Tombstone的时间设置为当前时间
func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

// IsTombstoned 用于判断生产者是否为Tombstone状态， 需要满足两个条件：
//	1. p.tombstoned 状态被标记为true
//	2. 当前时间与成为tombstone的时间的差值，需要小于定义的lifetime (如果大于说明这个节点已经标记为down了，lifetime表示tombstone状态允许存在的时间)
func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

// NewRegistrationDB 创建一个 RegistrationDB 实例，初始化了 RegistrationDB 中的 map结构
func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

// AddRegistration add a registration key
// 加锁从 registrationMap 中获取，如果获取不到说明不存在，向其中添加一条记录
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

// AddProducer add a producer to a registration
// 1. 首先检查registrationMap中对应的Registration记录是否存在，如果不存在创建一条
// 2. 从registrationMap中根据registration拿到ProducerMap(producers := r.registrationMap[k])
// 3. 从ProducerMap中根据key `p.peerInfo.id` 查找，如果找到了说明该生产者已经添加过了，如果没找到，以`p.peerInfo.id`作为key，要添加的 p作为 value添加一条记录
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	producers := r.registrationMap[k]
	_, found := producers[p.peerInfo.id]
	if found == false {
		producers[p.peerInfo.id] = p
	}
	return !found
}

// RemoveProducer remove a producer from a registration
// 删除给定 Registration 下给定的生产者，返回是否删除成功以及该Registration剩余的生产者数量
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}

	// Note: this leaves keys in the DB even if they have empty lists
	delete(producers, id)
	return removed, len(producers)
}

// RemoveRegistration remove a Registration and all it's producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

// needFilter 接收 key和subKey作为参数，返回是否需要过滤 (存在通配符为true，否则为false)
func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// FindRegistrations  接收 category、key、subkey， 返回匹配的所有 Registration
// 分两种情况：
// 1. 不需要过滤 (needFilter返回false)，也就是key和subKey都不存在通配符：直接根据 category、key、subkey 构造一个Registration，然后以此Registration为key在registrationMap中找。这种情况下其实至多只有一个匹配
// 2. 需要过滤 (needFilter返回true)，也就是key或subKey都存在通配符：遍历registrationMap，测试category、key、subkey中有通配符的情况下能否与遍历中的key匹配
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	results := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

// FindProducers  接收 category、key、subkey， 返回匹配的所有 Producer。与FindRegistrations类似，也是分两种情况，只是找到了Registration后还需要从registrationMap中把producer取出来，逻辑完全相同
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.registrationMap[k])
	}

	results := make(map[string]struct{})
	var retProducers Producers
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			_, found := results[producer.peerInfo.id]
			if found == false {
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

// LookupRegistrations 查找所有包含 给定id 的生产者所在的Registration
// 遍历 registrationMap，该map的value是producerMap，用给定的id作为key去取，取到了说明存在，那么registrationMap的key多对应的registration就是需要的，添加到返回结果中
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		if _, exists := producers[id]; exists {
			results = append(results, k)
		}
	}
	return results
}

// IsMatch 接收category、key、subkey，其中 key和subKey可能为通配符"*", 测试当前 Registration k能否和带输入的带通配符的查询匹配上
func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

// Filter 从Registrations中过滤满足条件的Registration
func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

// Keys 返回 Registrations 中 Registration所对应的key的切片
func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

// SubKeys 返回 Registrations 中 Registration所对应的SubKeys的切片
func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

// FilterByActive 过滤出出于active状态的Producers
// 以下两种情况任务不是active的:
// 1. now.Sub(cur) > inactivityTimeout: 当前时间与上次更新时间的差大于设定的超时时间inactivityTimeout
// 2. p.IsTombstoned(tombstoneLifetime): 出于Tombstone状态
func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

// PeerInfo 返回 Producers(Producer的切片) 的 PeerInfo 信息
// 遍历 Producers 拿到每一个Producer， 在拿到Producer.peerInfo即可
func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

// ProducerMap2Slice 将ProducerMap 转换成一个Producers(Producer的切片)
// 遍历ProducerMap把所有value拿出来就行了
func ProducerMap2Slice(pm ProducerMap) Producers {
	var producers Producers
	for _, producer := range pm {
		producers = append(producers, producer)
	}

	return producers
}
