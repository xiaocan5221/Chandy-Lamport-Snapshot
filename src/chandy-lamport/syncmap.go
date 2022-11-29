package chandy_lamport

import (
	"fmt"
	"sync"
)

const pp = false

// SyncMap An implementation of a map that synchronizes read and write accesses.
// 同步读和写访问的映射的实现。
// Note: This class intentionally adopts the interface of `sync.Map`,
// 注意：这个类有意采用接口`sync.Map`
// which is introduced in Go 1.9+ but not available before that.
// 这是在Go 1.9+中引入的，但在此之前是不可用的。
// This provides a simplified version of the same class without
// requiring the user to upgrade their Go installation.
// 这提供了同一个类的简化版本，而不需要用户升级他们的Go安装。
type SyncMap struct {
	internalMap map[interface{}]interface{}
	lock        sync.RWMutex
}

func NewSyncMap() *SyncMap {
	m := SyncMap{}
	m.internalMap = make(map[interface{}]interface{})
	return &m
}

func (m *SyncMap) Load(key interface{}) (value interface{}, ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	value, ok = m.internalMap[key]
	return
}

func (m *SyncMap) Store(key, value interface{}) {
	if dp && pp {
		fmt.Println("进入Store")
	}
	m.lock.Lock()
	if dp && pp {
		fmt.Println("上锁完成")
	}
	defer m.lock.Unlock()
	if dp && pp {
		fmt.Println("解锁完成")
	}
	m.internalMap[key] = value
	if dp && pp {
		fmt.Println("完成Store")
	}
}

func (m *SyncMap) LoadOrStore(key, value interface{}) (interface{}, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	existingValue, ok := m.internalMap[key]
	if ok {
		return existingValue, true
	}
	m.internalMap[key] = value
	return value, false
}

func (m *SyncMap) Delete(key interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.internalMap, key)
}

func (m *SyncMap) Range(f func(key, value interface{}) bool) {
	m.lock.RLock()
	for k, v := range m.internalMap {
		if !f(k, v) {
			break
		}
	}
	defer m.lock.RUnlock()
}
