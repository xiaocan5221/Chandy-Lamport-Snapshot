package chandy_lamport

import (
	"fmt"
	"log"
)

// Server The main participant of the distributed snapshot protocol.
// Server 是分布式快照协议的主要参与者
// Servers exchange token messages and marker messages among each other.
// 服务器之间交换令牌消息和标记消息
// Token messages represent the transfer of tokens from one server to another.
// 令牌消息代表令牌从一台服务器到另一台服务器的转移
// Marker messages represent the progress of the snapshot process. The bulk of
// 标记消息表示快照过程的进度
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
// 大部分分布式协议在“HandlePacket”和“StartSnapshot”中实现
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	SnapState *SyncMap // 新加，用于记录不同快照号的状态key=snapID,value=*ServerState
}

// ServerState 新加，用于记录服务器快照状态
type ServerState struct {
	SnapFinish       bool               // 记录此次快照是否完成，完成置true
	Tokens           int                // 记录快照发起时服务器状态，也就是令牌数量
	inboundLinkState *SyncMap           // 记录此次快照中所有入通道是否收到Marker, 收到后置true
	MessageQueen     []*SnapshotMessage // 记录快照开始后收到的所有消息
}

// Link A unidirectional communication channel between two servers
// 两个服务器之间的单向通信通道
// Each link contains an event queue (as opposed to a packet queue)
// 每个通道都包含一个事件队列（与数据包队列相反）
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		NewSyncMap(),
	}
}

// AddOutboundLink Add a unidirectional link to the destination server
// 添加到目标服务器的单向通道
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// SendToNeighbors Send a message on all the server's outbound links
// 在服务器的所有出站通道上发送消息
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// SendTokens Send a number of tokens to a neighbor attached to this server
// 向连接到此服务器的邻居发送一些令牌
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// HandlePacket Callback for when a message is received on this server.
// 在此服务器上收到消息时的回调
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
// 当此服务器上的快照算法完成时，此函数应通过调用 sim.NotifySnapshotComplete 通知模拟器
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	// 分情况处理消息
	// 令牌消息
	// 1.快照状态：记录消息到对应的快照号状态下，处理消息
	// 2.非快照状态：处理消息
	// Marker消息
	// 1.对应快照号存在：将对应快照的对应通道置否，表示此通道收到Marker消息
	// 2.对应快照号不存在：启动一个快照，将此快照号对应接收到此消息的通道置否
	switch message.(type) {
	case TokenMessage:
		snapIsFinish := true                                 // 表示快照进程完成的信号量
		nonFinishSnapID := make([]int, 0)                    // 存在多个快照进程时，用来存储未完成的快照进程号
		server.SnapState.Range(func(k, v interface{}) bool { // 遍历，判断是否所有快照进程都已完成
			if v.(ServerState).SnapFinish == false {
				snapIsFinish = false // 有未完成快照进程则将信号量置否
				if dp {
					fmt.Println("HandlePacket TokenMessage 服务器:", server.Id, "快照进程:", k, "未完成")
				}
				nonFinishSnapID = append(nonFinishSnapID, k.(int)) // 将未完成的快照进程号码保存
			}
			return true // 表示遍历所有
		})
		if snapIsFinish == false { // 如果有未完成的快照进程
			for _, k := range nonFinishSnapID { // 逐个对未完成的快照进程进行判断，此消息对应进入通道是否收到过Marker消息
				s, _ := server.SnapState.Load(k)
				t, _ := s.(ServerState).inboundLinkState.Load(src)
				if dp {
					fmt.Println("HandlePacket TokenMessage 服务器:", server.Id, "快照进程:", k, "进入通道：", src, "状态是：", t)
				}
				if t == false { // 没收到过则记录此消息，收到过则不用记录
					SnapshotMessage := SnapshotMessage{src, server.Id, message}
					newServerState := ServerState{
						s.(ServerState).SnapFinish,
						s.(ServerState).Tokens,
						s.(ServerState).inboundLinkState,
						append(s.(ServerState).MessageQueen, &SnapshotMessage),
					}
					server.SnapState.Store(k, newServerState)
					if dp {
						fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", k, "增加消息：", message)
					}
				}
			}
		}
		server.Tokens += message.(TokenMessage).numTokens // 无论是否记录都需要处理此令牌消息
		if dp {
			fmt.Println("HandlePacket 服务器:", server.Id, "处理来自服务器:", src, "消息:", message, "完成")
		}
	case MarkerMessage:
		snapIsExist := false                                 // 用于记录对应的快照号进程是否存在
		snapIsFinish := true                                 // 用于记录对应的快照号进程是否完成
		server.SnapState.Range(func(k, v interface{}) bool { // 与已存在的快照进程号逐个对比
			if k == message.(MarkerMessage).snapshotId {
				snapIsExist = true // 存在时，判断此快照进程是否完成，正常情况下应该未完成
				if v.(ServerState).SnapFinish == false {
					v.(ServerState).inboundLinkState.Store(src, true) // 将此Marker消息进入通道置完成位
					if dp {
						fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", k, "进入通道:", src, "完成")
					}
					v.(ServerState).inboundLinkState.Range(func(s, t interface{}) bool { // 判断此快照进程下所有服务器是否完成
						if t == false { // 有一个未完成就将标志信号置否，同时停止遍历
							if dp {
								fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", k, "进入通道:", s, "未完成")
							}
							snapIsFinish = false
							return false // 代表停止遍历
						}
						if dp {
							fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", k, "进入通道:", s, "已完成")
						}
						return true // 代表遍历所有
					})
				} else {
					log.Fatal("Error repeated Marker message: ", message)
				}
			}
			return true
		})
		if snapIsExist == true && snapIsFinish == true { // 快照进程存在且完成的情况下，需要将此状态标记为完成
			if dp {
				fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", message.(MarkerMessage).snapshotId, "已完成")
			}
			oldServerState, _ := server.SnapState.Load(message.(MarkerMessage).snapshotId)
			if dp {
				fmt.Println("HandlePacket 服务器:", server.Id, "快照进程", message.(MarkerMessage).snapshotId, "原始状态读取完毕")
			}
			newServerState := ServerState{
				true, // 所有服务器都完成将快照进程置完成位
				oldServerState.(ServerState).Tokens,
				oldServerState.(ServerState).inboundLinkState,
				oldServerState.(ServerState).MessageQueen,
			}
			if dp {
				fmt.Println("HandlePacket 服务器:", server.Id, "快照进程", message.(MarkerMessage).snapshotId, "状态更新完毕")
			}
			server.SnapState.Store(message.(MarkerMessage).snapshotId, newServerState) // 更新对应快照进程字段
			if dp {
				fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", message.(MarkerMessage).snapshotId, "状态储存完毕")
			}
			server.sim.NotifySnapshotComplete(server.Id, message.(MarkerMessage).snapshotId) // 通知模拟器此服务器的此快照进程完成
		}
		if snapIsExist == false { // 快照进程不存在时，开启一个快照进程
			server.StartSnapshot(message.(MarkerMessage).snapshotId)
			tempServerState, _ := server.SnapState.Load(message.(MarkerMessage).snapshotId)
			tempServerState.(ServerState).inboundLinkState.Store(src, true)
			server.SnapState.Store(message.(MarkerMessage).snapshotId, tempServerState)
			// 防止仅有一个进入通道时发生错误，需要以下进程
			tempServerState.(ServerState).inboundLinkState.Range(func(s, v interface{}) bool { // 判断此快照进程下所有服务器进入通道是否完成
				if v == false { // 有一个未完成就将标志信号置否
					if dp {
						fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", message.(MarkerMessage).snapshotId, "进入通道:", s, "未完成")
					}
					snapIsFinish = false
					return false
				}
				return true
			})
			if snapIsFinish == true {
				oldServerState, _ := server.SnapState.Load(message.(MarkerMessage).snapshotId)
				if dp {
					fmt.Println("HandlePacket 服务器:", server.Id, "快照进程", message.(MarkerMessage).snapshotId, "原始状态读取完毕")
				}
				newServerState := ServerState{
					true, // 所有服务器都完成将快照进程置完成位
					oldServerState.(ServerState).Tokens,
					oldServerState.(ServerState).inboundLinkState,
					oldServerState.(ServerState).MessageQueen,
				}
				if dp {
					fmt.Println("HandlePacket 服务器:", server.Id, "快照进程", message.(MarkerMessage).snapshotId, "状态更新完毕")
				}
				server.SnapState.Store(message.(MarkerMessage).snapshotId, newServerState) // 更新对应快照状态字段
				if dp {
					fmt.Println("HandlePacket 服务器:", server.Id, "快照进程:", message.(MarkerMessage).snapshotId, "状态储存完毕")
				}
				server.sim.NotifySnapshotComplete(server.Id, message.(MarkerMessage).snapshotId) // 通知模拟器此服务器的此快照进程完成
			}
		}
	default:
		log.Fatal("Error unknown message: ", message)
	}
}

// StartSnapshot Start the chandy-lamport snapshot algorithm on this server.
// 在此服务器上启动 chandy-lamp 快照算法
// This should be called only once per server.
// 每个服务器只应调用一次
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	// 完成两件事
	// 1.初始化服务器状态字段
	// 2.向所有邻居发送Marker消息
	serverState := ServerState{
		false,
		server.Tokens,
		NewSyncMap(),
		make([]*SnapshotMessage, 0),
	}
	for k := range server.inboundLinks {
		serverState.inboundLinkState.Store(k, false)
	}
	server.SnapState.Store(snapshotId, serverState)
	message := MarkerMessage{snapshotId}
	server.SendToNeighbors(message)
	if dp {
		fmt.Println("Server StartSnapshot 服务器：", server.Id, "启动快照：", snapshotId, "向所有邻居发送消息:", message)
	}
}
