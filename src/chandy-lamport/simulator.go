package chandy_lamport

import (
	"fmt"
	"log"
	"math/rand"
)

// Max random delay added to packet delivery
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.
// Simulator 是分布式快照应用程序的入口点
// It is a discrete time simulator, i.e. events that happen at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// 它是一个离散时间模拟器，即在时间 t + 1 发生的事件严格在时间 t 发生的事件之后发生
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
// 在每个时间步，模拟器检查系统中所有链路上排队的消息，并决定将哪些消息传递到目的地
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.
// 模拟器负责启动快照进程，诱导服务器相互传递令牌，并在进程终止后收集快照状态
type Simulator struct {
	time           int
	nextSnapshotId int
	servers        map[string]*Server // key = server ID
	logger         *Logger
	// TODO: ADD MORE FIELDS HERE
	snapState *SyncMap // 记录快照目前状态，key = snapShotID,value 是snapState类型
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		NewSyncMap(),
	}
}

// snapState 新加，用来记录某个快照目前的状态
type snapState struct {
	finish       bool            // 此快照是否完成，true 代表完成，当且仅当serverFinish中所有value都为true时才置true
	serverFinish map[string]bool // 各服务器是否完成此轮快照，key = sim.serverID，true 代表完成
}

// GetReceiveTime Return the received time of a message after adding a random delay.
// 添加随机延迟后返回消息的接收时间
// Note: since we only deliver one message to a given server at each time step,
// 注意：由于我们在每个时间步只向给定服务器发送一条消息，
// the message may be received *after* the time step returned in this function.
// 该消息可能会在此函数中返回的时间步之后收到
func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(5)
}

// AddServer Add a server to this simulator with the specified number of starting tokens
// 将具有指定数量的起始令牌的服务器添加到此模拟器
func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim)
	sim.servers[id] = server
}

// AddForwardLink Add a unidirectional link between two servers
// 在两台服务器之间添加单向通道
func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// InjectEvent Run an event in the system
// 在系统中运行事件
func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Tick Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
// 将模拟器时间提前一步，处理在新时间步到期的所有发送消息事件（如果有）。
func (sim *Simulator) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// 注意：为了确保跨服务器的数据包传递的确定性排序，
	// we must also iterate through the servers and the links in a deterministic way
	// 我们还必须以确定的方式遍历服务器和通道
	// 一次Tick使每个服务器最多处理一个事件
	/*if dp {
		fmt.Println("tick and sim time is:", sim.time)
	}*/
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			// 在每个时间步，每个服务器最多发送一个数据包，以建立向每个服务器发送数据包的总顺序
			if !link.events.Empty() {
				e := link.events.Peek().(SendMessageEvent)
				if e.receiveTime <= sim.time {
					link.events.Pop()
					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					if dp {
						fmt.Println("Tick 服务器:", e.dest, "消息:", e.message, "来自于：", e.src)
					}
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// StartSnapshot Start a new snapshot process at the specified server
// 在指定的服务器上启动一个新的快照进程
// 模拟器还要初始化快照状态字段
func (sim *Simulator) StartSnapshot(serverId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME
	// 新建并初始化快照状态字段
	snapState := snapState{false, make(map[string]bool)}
	for s := range sim.servers {
		snapState.serverFinish[s] = false
	}
	sim.snapState.Store(snapshotId, snapState)
	if dp {
		fmt.Println("Sim StartSnapshot 服务器:", serverId, "快照进程:", snapshotId, "开始")
	}
	sim.servers[serverId].StartSnapshot(snapshotId)
}

// NotifySnapshotComplete Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
// 服务器回调以通知模拟器快照过程已在特定服务器上完成
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME
	if dp {
		fmt.Println("Notify 服务器:", serverId, "快照进程:", snapshotId, "快照完成。")
	}
	tempSnapState, _ := sim.snapState.Load(snapshotId)         // 读取指定快照进程目前状态字段
	tempSnapState.(snapState).serverFinish[serverId] = true    // 服务器通知模拟器后，模拟器将对应快照进程的服务器状态置完成位
	for k, v := range tempSnapState.(snapState).serverFinish { // 判断此快照进程下是否所有服务器都完成
		if v == false { // 有一个未完成就表示快照进程未完成，则返回
			if dp {
				fmt.Println("Notify 快照进程:", snapshotId, "服务器：", k, "快照未完成")
			}
			return
		}
	}
	newSnapState := snapState{ // 防止并发读写，新建状态字，使用原状态字的服务器状态字段
		true, // 执行到此表示该快照进程下所有服务器都已完成快照，将此快照进程标记完成状态
		tempSnapState.(snapState).serverFinish,
	}
	sim.snapState.Store(snapshotId, newSnapState) // 更新模拟器对应快照进程的快照状态
	if dp {
		fmt.Println("Notify 快照进程:", snapshotId, "整体快照完毕")
	}
}

// CollectSnapshot Collect and merge snapshot state from all the servers.
// 从所有服务器收集和合并快照状态
// This function blocks until the snapshot process has completed on all servers.
// 此功能会一直阻塞，直到所有服务器上的快照过程完成
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
	// TODO: IMPLEMENT ME
	for k, _ := sim.snapState.Load(snapshotId); k.(snapState).finish == false; k, _ = sim.snapState.Load(snapshotId) {
		// 等待此快照号代表的快照进程完成
	}
	for k := range sim.servers { // 逐个服务器收集快照状态
		temp, _ := sim.servers[k].SnapState.Load(snapshotId)
		snap.tokens[k] = temp.(ServerState).Tokens
		for _, m := range temp.(ServerState).MessageQueen {
			snap.messages = append(snap.messages, m)
		}
	}
	if dp {
		fmt.Println("Collect 快照进程:", snapshotId, "快照状态收集完毕")
	}
	return &snap
}
