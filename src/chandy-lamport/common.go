package chandy_lamport

import (
	"fmt"
	"log"
	"reflect"
	"sort"
)

const debug = false
const dp = false // 新加，用于打印追踪调试时的信息标记

// ====================================
//  Messages exchanged between servers
// ====================================

// SendMessageEvent An event that represents the sending of a message.
// 表示发送消息的事件
// This is expected to be queued in `link.events`.
// 预计这将在 `link.events` 中排队
type SendMessageEvent struct {
	src     string
	dest    string
	message interface{}
	// The message will be received by the server at or after this time step
	// 服务器将在这一步或这一步后收到这个消息
	receiveTime int
}

// TokenMessage A message sent from one server to another for token passing.
// 从一个服务器发送到另一个服务器用于令牌传递的消息
// This is expected to be encapsulated within a `sendMessageEvent`.
// 这应该被封装在一个`sendMessageEvent`中
type TokenMessage struct {
	numTokens int
}

func (m TokenMessage) String() string {
	return fmt.Sprintf("token(%v)", m.numTokens)
}

// MarkerMessage A message sent from one server to another during the chandy-lamport algorithm.
// 在 chandy-lamp 算法期间从一个服务器发送到另一个服务器的消息
// This is expected to be encapsulated within a `sendMessageEvent`.
type MarkerMessage struct {
	snapshotId int
}

func (m MarkerMessage) String() string {
	return fmt.Sprintf("marker(%v)", m.snapshotId)
}

// =======================
//  Events used by logger
// =======================

// ReceivedMessageEvent A message that signifies receiving of a message on a particular server
// 表示在特定服务器上 接收到消息 的消息
// This is used only for debugging that is not sent between servers
type ReceivedMessageEvent struct {
	src     string
	dest    string
	message interface{}
}

func (m ReceivedMessageEvent) String() string {
	switch msg := m.message.(type) {
	case TokenMessage:
		return fmt.Sprintf("%v received %v tokens from %v", m.dest, msg.numTokens, m.src)
	case MarkerMessage:
		return fmt.Sprintf("%v received marker(%v) from %v", m.dest, msg.snapshotId, m.src)
	}
	return fmt.Sprintf("Unrecognized message: %v", m.message)
}

// SentMessageEvent A message that signifies sending of a message on a particular server
// 表示在特定服务器上 发送消息 的消息
// This is used only for debugging that is not sent between servers
type SentMessageEvent struct {
	src     string
	dest    string
	message interface{}
}

func (m SentMessageEvent) String() string {
	switch msg := m.message.(type) {
	case TokenMessage:
		return fmt.Sprintf("%v sent %v tokens to %v", m.src, msg.numTokens, m.dest)
	case MarkerMessage:
		return fmt.Sprintf("%v sent marker(%v) to %v", m.src, msg.snapshotId, m.dest)
	}
	return fmt.Sprintf("Unrecognized message: %v", m.message)
}

// StartSnapshot A message that signifies the beginning of the snapshot process on a particular server.
// 表示特定服务器上快照过程开始的消息
// This is used only for debugging that is not sent between servers.
type StartSnapshot struct {
	serverId   string
	snapshotId int
}

func (m StartSnapshot) String() string {
	return fmt.Sprintf("%v startSnapshot(%v)", m.serverId, m.snapshotId)
}

// EndSnapshot A message that signifies the end of the snapshot process on a particular server.
// 表示特定服务器上的快照进程结束的消息
// This is used only for debugging that is not sent between servers.
type EndSnapshot struct {
	serverId   string
	snapshotId int
}

func (m EndSnapshot) String() string {
	return fmt.Sprintf("%v endSnapshot(%v)", m.serverId, m.snapshotId)
}

// ================================================
//  Events injected to the system by the simulator
//  模拟器注入系统的事件
// ================================================

// PassTokenEvent An event parsed from the .event files that represent the passing of tokens
// from one server to another
// 从代表令牌传递的 .event 文件中解析的事件
type PassTokenEvent struct {
	src    string
	dest   string
	tokens int
}

// SnapshotEvent An event parsed from the .event files that represent the initiation of the
// chandy-lamport snapshot algorithm
// 从 .event 文件中解析的事件，代表 chandy-lamp 快照算法的启动
type SnapshotEvent struct {
	serverId string
}

// SnapshotMessage A message recorded during the snapshot process
// 快照过程中记录的消息
type SnapshotMessage struct {
	src     string
	dest    string
	message interface{}
}

// SnapshotState State recorded during the snapshot process
// 快照过程中记录的状态
type SnapshotState struct {
	id       int
	tokens   map[string]int // key = server ID, value = num tokens
	messages []*SnapshotMessage
}

// =====================
//  Misc helper methods
// =====================

// If the error is not nil, terminate
func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Return the keys of the given map in sorted order.
// Note: The argument passed in MUST be a map, otherwise an error will be thrown.
func getSortedKeys(m interface{}) []string {
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		log.Fatal("Attempted to access sorted keys of a non-map: ", m)
	}
	keys := make([]string, 0)
	for _, k := range v.MapKeys() {
		keys = append(keys, k.String())
	}
	sort.Strings(keys)
	return keys
}
