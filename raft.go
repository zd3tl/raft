package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var _ Api = new(server)

var logHeader = &Log{}

type Api interface {
	HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
	LeaderCommit int

	// 4 test
	PeerEndpoint string
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

type RequestVoteRequest struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int

	// 4 test
	PeerEndpoint string
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type ClientRequest struct {
	K, V string
}
type ClientResponse struct {
	ok bool
}

type serverState int

const (
	follower serverState = iota
	candidate
	leader
)

type server struct {
	// serverId 在集群中标记自己，不特意删除，不会出现更换的情况，这块可以参考redis中的id的实现
	serverId string

	currentTerm int
	// TODO 日志可以更换开源磁盘存储
	logs []*Log
	// 被 HandleAppendEntries 和 followerLoopClusterChange 两个goroutine共享
	votedFor atomic.Value

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	// state 开始是 follower
	state serverState

	// lastAppendEntriesTime 和 electionTimeout 结合判断是否需要参与选举，看情况 follower 也需要一个goroutine loop 积极参与集群的状态
	lastAppendEntriesTime time.Time

	// peers 当前集群的所有节点信息，这块应该是初始化好的，不会变，厨房费配置变更
	peers []*peer

	lg     *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

type peer struct {
	svr *server

	endpoint   string
	nextIndex  int
	matchIndex int
	// leaderId 用于follower接收到client请求，redirect到leader
	leaderId string

	rpc Rpc
}
type KV struct {
	K string `json:"k"`
	V string `json:"v"`
}

type Log struct {
	Term int `json:"term"`
	Kv   *KV `json:"kv"`

	// 5.3
	Index int `json:"index"`
}

func (p *peer) sendAppendEntries(entries []*Log) (*AppendEntriesResponse, error) {
	// 5.3
	var (
		prevLogIndex int
		prevLogTerm  int
	)
	// entries 为空当前在心跳
	if len(entries) > 0 {
		prevLogIndex = entries[0].Index - 1
		if prevLogIndex >= 0 {
			prevLogTerm = p.svr.logs[prevLogIndex].Term
		}
	}

	req := AppendEntriesRequest{
		Term:         p.svr.currentTerm,
		LeaderId:     p.svr.serverId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: p.svr.commitIndex,

		PeerEndpoint: p.endpoint,
	}

	resp, err := p.rpc.AppendEntries(&req)
	if err != nil {
		return nil, err
	}

	// 刷新peer的信息
	if len(entries) > 0 {
		p.nextIndex = entries[len(entries)-1].Index + 1
		p.matchIndex = entries[len(entries)-1].Index
	}
	return resp, nil
}

var (
	// electionTimeout follower 启动等一段事件
	electionTimeout = 10 * time.Second
)

func newServer(peerEndpoints []string, serverId string) *server {
	// 要求必须是大于1的，且奇数
	peerLen := len(peerEndpoints)
	if peerLen%2 != 0 {
		return nil
	}

	lg, _ := zap.NewProduction()
	svr := server{
		state:                 follower,
		serverId:              serverId,
		lg:                    lg,
		lastAppendEntriesTime: time.Now(),
		votedFor:              atomic.Value{},
	}
	svr.logs = append(svr.logs, logHeader)
	svr.votedFor.Store("")

	for _, endpoint := range peerEndpoints {
		svr.peers = append(
			svr.peers,
			&peer{
				svr:        &svr,
				nextIndex:  len(svr.logs),
				matchIndex: 0,
				endpoint:   endpoint,
				rpc:        &MR,
			},
		)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	svr.ctx = ctx
	svr.cancel = cancel

	return &svr
}

func (svr *server) start() {
	go svr.followerLoopClusterChange(svr.ctx)
}

func (svr *server) stop() {
	svr.cancel()
}

// HandleAppendEntries follower 实现
func (svr *server) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	// leader 不能接受append，调整好状态再说
	if svr.state == leader {
		svr.lg.Info("rejectAppend: leader can not accept append entries request",
			zap.String("serverId", svr.serverId))

		return &AppendEntriesResponse{
			Term:    svr.currentTerm,
			Success: false,
		}
	}

	// 5.1 当前的term大，证明req中的是历史term的，reject
	if svr.currentTerm > req.Term {
		svr.lg.Info("rejectAppend: smaller term",
			zap.String("serverId", svr.serverId),
			zap.Int("req.Term", req.Term),
			zap.Int("svr.currentTerm", svr.currentTerm))

		return &AppendEntriesResponse{
			Term:    svr.currentTerm,
			Success: false,
		}
	}

	if svr.currentTerm < req.Term {
		// 需要进行term更正
		svr.lg.Info("append: term correct",
			zap.String("serverId", svr.serverId),
			zap.Int("req.Term", req.Term),
			zap.Int("svr.currentTerm", svr.currentTerm))
		svr.currentTerm = req.Term
	}

	// 5.3 不是第一条log，需要判断上一条log和本地log的index和term能match
	if req.PrevLogIndex > 0 {
		// 上一个log不存在
		if req.PrevLogIndex > len(svr.logs) {
			svr.lg.Info("rejectAppend: log entry mismatch",
				zap.String("serverId", svr.serverId),
				zap.Int("req.PrevLogIndex", req.Term),
				zap.Int("len(svr.logs)", len(svr.logs)))

			return &AppendEntriesResponse{
				Term:    svr.currentTerm,
				Success: false,
			}
		}

		// 上一个log存在，但是term不一致
		prevLog := svr.logs[req.PrevLogIndex]
		if prevLog.Term != req.PrevLogTerm {
			// 删除后续所有的log
			svr.logs = svr.logs[:prevLog.Index]

			svr.lg.Info("rejectAppend: prev log term mismatch",
				zap.String("serverId", svr.serverId),
				zap.Int("req.PrevLogTerm", prevLog.Term),
				zap.Int("prevLog.Term", prevLog.Term))

			return &AppendEntriesResponse{
				Term:    svr.currentTerm,
				Success: false,
			}
		}
	}

	// 5.2
	if svr.state == candidate {
		svr.state = follower
	}

	// TODO 下面的逻辑执行需要时间， lastAppendEntriesTime 更新延迟会导致 follower 角色的变更，应该尽量避免这种情况，否则会有很多无意义的rpc
	svr.lastAppendEntriesTime = time.Now()
	svr.votedFor.Store("")

	// 5.3
	if len(req.Entries) == 0 {
		// 心跳的场景，不涉及信息的变更，只需要刷新 lastAppendEntriesTime 就行
		svr.lg.Info("acceptAppend: heartbeat",
			zap.String("serverId", svr.serverId),
			zap.String("leaderId", req.LeaderId),
			zap.Reflect("req.Entries", req.Entries))

		return &AppendEntriesResponse{
			Term:    svr.currentTerm,
			Success: true,
		}
	}

	var maxLogIndex int
	for _, log := range req.Entries {
		if log.Index <= req.PrevLogIndex {
			svr.lg.Info("unexpected log.Index error",
				zap.String("serverId", svr.serverId),
				zap.String("leaderId", req.LeaderId),
				zap.Reflect("log", log))
			continue
		}

		// index已经被占用，那么直接overwrite，且需要对logs进行截取
		if log.Index >= len(svr.logs) {
			svr.logs = append(svr.logs, log)
		} else {
			svr.logs[log.Index] = log
		}

		if maxLogIndex < log.Index {
			maxLogIndex = log.Index
		}
	}
	svr.logs = svr.logs[:maxLogIndex+1]

	// 直接append过来的log，不能理解应用于状态机，需要根据LeaderCommit进行判断
	// 下面小于LeaderCommit的都可以应用于本地
	maxCommitIndex := req.LeaderCommit
	currentMaxLogIndex := len(svr.logs)
	if currentMaxLogIndex < maxCommitIndex {
		maxCommitIndex = currentMaxLogIndex
	}
	// TODO svr.commitIndex 到 currentMaxLogIndex 之间都可以应用于状态机器，这块可以异步，防止耗时过长

	svr.lg.Info("acceptAppend: log entries",
		zap.String("serverId", svr.serverId),
		zap.String("leaderId", req.LeaderId),
		zap.Reflect("req.Entries", req.Entries))

	// TODO 日志到状态机是个异步的过程，会让log的append很快，状态更新近实时，async会导致返回结果后仍旧client拿不到，所以应该不太对

	return &AppendEntriesResponse{
		Term:    svr.currentTerm,
		Success: true,
	}
}

// HandleRequestVote follower 实现
func (svr *server) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	if svr.state == leader {
		svr.lg.Info("rejectAppend: leader can not accept request vote request",
			zap.String("serverId", svr.serverId),
			zap.String("req.CandidateId", req.CandidateId))

		return &RequestVoteResponse{
			Term:        svr.currentTerm,
			VoteGranted: false,
		}
	}

	if req.CandidateId == svr.serverId {
		panic(fmt.Sprintf("send vote request to self %s %s", req.CandidateId, svr.serverId))
	}

	// 5.1 5.2
	if req.Term < svr.currentTerm {
		svr.lg.Info("rejectVote: smaller term",
			zap.String("serverId", svr.serverId),
			zap.Int("req.Term", req.Term),
			zap.Int("svr.currentTerm", svr.currentTerm))

		return &RequestVoteResponse{
			Term:        svr.currentTerm,
			VoteGranted: false,
		}
	}

	// 5.4
	var svrLastLogIndex int
	if len(svr.logs) > 1 {
		svrLastLogIndex = svr.logs[len(svr.logs)-1].Index
	}
	if req.LastLogIndex < svrLastLogIndex {
		svr.lg.Info("rejectVote: log not up to date",
			zap.String("serverId", svr.serverId),
			zap.Int("req.LastLogIndex", req.LastLogIndex),
			zap.Int("svrLastLogIndex", svrLastLogIndex))

		return &RequestVoteResponse{
			Term:        svr.currentTerm,
			VoteGranted: false,
		}
	}

	// 5.2 保证只能投票选择一个 candidate
	swapped := svr.votedFor.CompareAndSwap("", req.CandidateId)
	if swapped {
		svr.currentTerm = req.Term
		svr.lastAppendEntriesTime = time.Now()

		// 注意这里没有直接改变当前server的state，state由server当前的守护进程改变，目前先这么处理
	}

	svr.lg.Info("acceptVote: check swapped",
		zap.String("serverId", svr.serverId),
		zap.Bool("swapped", swapped),
		zap.String("req.CandidateId", req.CandidateId))
	return &RequestVoteResponse{
		Term:        svr.currentTerm,
		VoteGranted: swapped,
	}
}

func (svr *server) HandleClientRequest(req *ClientRequest) *ClientResponse {
	kv := KV{
		K: req.K,
		V: req.V,
	}
	log := Log{
		Term:  svr.currentTerm,
		Kv:    &kv,
		Index: len(svr.logs),
	}

	// 直接写到本地
	svr.logs = append(svr.logs, &log)

	// TODO 此处的请求，很可能部分失败，需要有异步的机制保证，未同步的log持续尝试同步给相应的peer
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		succCnt int
	)

	wg.Add(len(svr.peers))
	for _, p := range svr.peers {
		go func(p *peer) {
			defer wg.Done()

			// 设定超时，成功与否，不那么重要
			// 1 失败不会写入状态机
			// 2 成功写入状态机，并且通过 leaderLoopAppendEntries 无限重试
			resp, err := p.sendAppendEntries([]*Log{&log})
			if err != nil {
				return
			}

			if resp.Success {
				mu.Lock()
				succCnt++
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	// 5.3
	majority := len(svr.peers)/2 + 1
	if succCnt > majority {
		// TODO 写入状态机 boltdb

		// 5.3
		svr.commitIndex = len(svr.logs) - 1
	}

	return nil
}

// followerLoopClusterChange 对于follower来说，需要监测 electionTimeout
func (svr *server) followerLoopClusterChange(ctx context.Context) {
	ticker := time.Tick(time.Second)
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			return
		}
		svr.followerClusterChange(ctx)
	}
}

func (svr *server) followerClusterChange(ctx context.Context) {
	// 角色变更，退出goroutine，不再继续探测election timeout
	if svr.state != follower {
		return
	}

	if time.Since(svr.lastAppendEntriesTime) < electionTimeout {
		return
	}
	svr.lg.Info("follower: election timeout",
		zap.String("serviceId", svr.serverId),
		zap.Time("lastAppendEntriesTime", svr.lastAppendEntriesTime),
		zap.Duration("electionTimeout", electionTimeout))

	// 在这个期间，有请求进来，把votedFor变更，下面的CompareAndSwap报错，等下一个ticker继续检查

	// 5.2 leader election
	swapped := svr.votedFor.CompareAndSwap("", svr.serverId)
	if !swapped {
		svr.lg.Info("follower: votedFor already be taken",
			zap.String("serverId", svr.serverId),
			zap.String("svr.votedFor", svr.votedFor.Load().(string)))

		// 已经投票，不能升级为 candidate，继续作为 follower 探测 lastAppendEntriesTime
		svr.lastAppendEntriesTime = time.Now()
		return
	}
	svr.state = candidate
	svr.lg.Info("follower: votedFor self succ",
		zap.String("serverId", svr.serverId))

	// 需要不断重试，直到终结状态出现
	var firstRound = true
	for {
		if !firstRound {
			svr.lg.Info("candidate: wait for another round",
				zap.String("serverId", svr.serverId),
				zap.Int("currentTerm", svr.currentTerm))
			// 等一个随机的事件继续发 150-300ms
			time.Sleep(electionTimeout + time.Duration(150+rand.Intn(150))*time.Millisecond)
		}
		firstRound = false

		svr.lg.Info("candidate: start select leader",
			zap.String("serverId", svr.serverId),
			zap.Int("currentTerm", svr.currentTerm))

		if time.Since(svr.lastAppendEntriesTime) < electionTimeout {
			svr.lg.Info("candidate: some on other became leader",
				zap.String("serverId", svr.serverId),
				zap.Int("currentTerm", svr.currentTerm))
			svr.state = follower
			break
		}

		// 每次不成功需要刷新term，并重新投票给自己
		svr.currentTerm++
		// currentVoteTerm := svr.currentTerm

		// TODO 这块可以event driven出去，让另一个goroutine干，但目前暂时放在一起
		var (
			mu sync.Mutex
			wg sync.WaitGroup

			// votedFor自己初始值就是1
			succCnt             = 1
			encounterBiggerTerm bool
		)
		wg.Add(len(svr.peers))
		for _, p := range svr.peers {
			go func(p *peer) {
				defer wg.Done()

				if p.endpoint == svr.serverId {
					panic("ddd")
				}

				lastLog := &Log{}
				if len(p.svr.logs) > 0 {
					lastLog = p.svr.logs[len(p.svr.logs)-1]
				}
				req := &RequestVoteRequest{
					Term:         svr.currentTerm,
					CandidateId:  svr.serverId,
					LastLogIndex: lastLog.Index,
					LastLogTerm:  lastLog.Term,
					PeerEndpoint: p.endpoint,
				}
				resp, err := p.rpc.RequestVote(req)
				if err != nil {
					svr.lg.Error("candidate: RequestVote error",
						zap.String("serverId", svr.serverId),
						zap.String("peerServerId", p.leaderId),
						zap.Error(err))
					return
				}

				if resp.Term > svr.currentTerm {
					svr.lg.Info("candidate: encounter bigger term when RequestVote",
						zap.String("serverId", svr.serverId),
						zap.Int("resp.Term", resp.Term),
						zap.Int("svr.currentTerm", svr.currentTerm))

					mu.Lock()
					encounterBiggerTerm = true
					mu.Unlock()
					return
				}

				if resp.VoteGranted {
					svr.lg.Info("candidate: VoteGranted",
						zap.String("serverId", svr.serverId),
						zap.Int("svr.currentTerm", svr.currentTerm),
						zap.String("peer", p.endpoint))

					mu.Lock()
					succCnt++
					mu.Unlock()
				}
			}(p)
		}
		wg.Wait()

		// Rules for Servers
		if encounterBiggerTerm {
			svr.state = follower
			svr.votedFor.CompareAndSwap(svr.serverId, "")
			// 弥补上面逻辑执行花费的时间，刷新 lastAppendEntriesTime
			svr.lastAppendEntriesTime = time.Now()

			svr.lg.Info("candidate: bigger term exist, return to follower",
				zap.String("serverId", svr.serverId),
				zap.Int("currentTerm", svr.currentTerm))
			break
		}

		// 1 查看是赢得当前的选举
		majority := (len(svr.peers)+1)/2 + 1
		if succCnt >= majority {
			svr.state = leader

			svr.lg.Info("candidate: became leader",
				zap.String("serverId", svr.serverId),
				zap.Int("currentTerm", svr.currentTerm))

			go svr.leaderLoopAppendEntries(ctx)

			return
		}

		// 确定自己没有赢得选举，清空投票，进入下一轮循环，允许 HandleRequestVote 拉投票
		swapped2 := svr.votedFor.CompareAndSwap(svr.serverId, "")
		if swapped2 {
			svr.lg.Info("candidate: release votedFor",
				zap.String("serverId", svr.serverId),
				zap.Int("currentTerm", svr.currentTerm))
		}

		svr.lg.Info("candidate: no winner try another round",
			zap.String("serverId", svr.serverId),
			zap.Int("currentTerm", svr.currentTerm))
	}
}

func (svr *server) leaderLoopAppendEntries(ctx context.Context) {

	startTime := time.Now()

	var init = true
	ticker := time.Tick(time.Second)
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			return
		}

		// 4 test 停止leader的工作，等待另一个服务接管
		if time.Since(startTime) > 30*time.Second {
			return
		}

		var (
			mu                  sync.Mutex
			wg                  sync.WaitGroup
			encounterBiggerTerm bool
		)
		wg.Add(len(svr.peers))
		for _, p := range svr.peers {
			go func(p *peer) {
				defer wg.Done()

				// 5.3
				var entries []*Log
				if !init {
					// 存在需要复制的log
					if p.nextIndex < svr.commitIndex {
						entries = append(entries, svr.logs[p.nextIndex])
					}
				}

				resp, err := p.sendAppendEntries(entries)
				if err != nil {
					svr.lg.Error("leader: failed to append entries",
						zap.String("serverId", svr.serverId),
						zap.Int("resp.Term", resp.Term),
						zap.Int("svr.currentTerm", svr.currentTerm),
						zap.Error(err))

					// 失败重试，需要减少nextIndex，follower可能面对log少的情况，需要找到leader和follower的交点
					p.nextIndex--
					return
				}
				if resp.Term > svr.currentTerm {
					svr.lg.Info("leader: encounter bigger term when RequestVote",
						zap.String("serverId", svr.serverId),
						zap.Int("resp.Term", resp.Term),
						zap.Int("svr.currentTerm", svr.currentTerm))

					mu.Lock()
					encounterBiggerTerm = true
					mu.Unlock()
					return
				}

				if !resp.Success {
					svr.lg.Info("leader: failed to append entries",
						zap.String("serverId", svr.serverId),
						zap.Int("resp.Term", resp.Term),
						zap.Int("svr.currentTerm", svr.currentTerm))
				} else {
					svr.lg.Info("leader: success to append entries",
						zap.String("serverId", svr.serverId),
						zap.Int("resp.Term", resp.Term),
						zap.Int("svr.currentTerm", svr.currentTerm))
				}
			}(p)
		}
		init = false
		wg.Wait()

		// Rules for Servers
		if encounterBiggerTerm {
			svr.state = follower
			svr.lastAppendEntriesTime = time.Now()
			svr.votedFor.Store("")

			svr.lg.Info("leader: encounter bigger term, return to follower",
				zap.String("serverId", svr.serverId),
				zap.Int("svr.currentTerm", svr.currentTerm))

			go svr.followerLoopClusterChange(ctx)

			return
		}
	}
}
