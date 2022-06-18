package raft

import (
	"go.uber.org/zap"
	"reflect"
	"testing"
)

func TestServer_HandleAppendEntries(t *testing.T) {
	lg, _ := zap.NewProduction()

	currentTerm := 100

	var tests = []struct {
		svrState serverState

		req        *AppendEntriesRequest
		expectResp *AppendEntriesResponse
	}{
		{
			svrState: leader,

			req: &AppendEntriesRequest{},
			expectResp: &AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
		},

		{
			svrState: follower,

			req: &AppendEntriesRequest{
				Term: 99,
			},
			expectResp: &AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
		},

		{
			svrState: follower,

			req: &AppendEntriesRequest{
				Term: currentTerm,

				// 超过本地logs的日志，直接reject
				PrevLogIndex: 1,
			},
			expectResp: &AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
		},

		{
			svrState: follower,

			req: &AppendEntriesRequest{
				Term: currentTerm,

				PrevLogIndex: 1,
				// 相同index不同term
				PrevLogTerm: 98,
			},
			expectResp: &AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
		},

		{
			svrState: follower,

			// 正常req，不带日志
			req: &AppendEntriesRequest{
				Term:         currentTerm,
				PrevLogIndex: 1,
				PrevLogTerm:  99,
			},
			expectResp: &AppendEntriesResponse{
				Term:    currentTerm,
				Success: true,
			},
		},

		{
			svrState: follower,

			// 正常req，带日志
			req: &AppendEntriesRequest{
				Term:         currentTerm,
				PrevLogIndex: 1,
				PrevLogTerm:  99,
				Entries: []*Log{
					{
						Term:  currentTerm,
						Index: 1,
					},
					{
						Term:  currentTerm,
						Index: 2,
					},
				},
			},
			expectResp: &AppendEntriesResponse{
				Term:    currentTerm,
				Success: true,
			},
		},
	}

	for idx, tt := range tests {
		followerSvr := server{
			lg: lg,

			currentTerm: currentTerm,

			logs: []*Log{
				// 站位
				{
					Term:  0,
					Index: 0,
					Kv:    &KV{},
				},

				// 第一条日志
				{
					Term:  99,
					Index: 1,
					Kv:    &KV{},
				},
			},
		}

		followerSvr.state = tt.svrState

		actualResp := followerSvr.HandleAppendEntries(tt.req)
		if !reflect.DeepEqual(actualResp, tt.expectResp) {
			t.Errorf("case %d not ok", idx)
			t.SkipNow()
		}
	}
}

func TestServer_HandleRequestVote(t *testing.T) {
	lg, _ := zap.NewProduction()

	currentTerm := 100
	candidateId := "foo"

	var tests = []struct {
		svrState serverState

		req        *RequestVoteRequest
		expectResp *RequestVoteResponse
	}{
		{
			svrState: leader,

			req: &RequestVoteRequest{},
			expectResp: &RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: false,
			},
		},

		{
			svrState: follower,

			req: &RequestVoteRequest{
				Term: 99,
			},
			expectResp: &RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: false,
			},
		},

		{
			svrState: follower,

			req: &RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: 0,
			},
			expectResp: &RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: false,
			},
		},

		{
			svrState: follower,

			req: &RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: 1,
			},
			expectResp: &RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: true,
			},
		},

		{
			svrState: follower,

			req: &RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: 1,
			},
			expectResp: &RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: false,
			},
		},
	}

	for idx, tt := range tests {
		followerSvr := server{
			state:       tt.svrState,
			lg:          lg,
			currentTerm: currentTerm,
			logs: []*Log{
				// 站位
				{
					Term:  0,
					Index: 0,
					Kv:    &KV{},
				},

				// 第一条日志
				{
					Term:  99,
					Index: 1,
					Kv:    &KV{},
				},
			},
		}

		if idx == 4 {
			followerSvr.votedFor.Store("bar")
		}

		actualResp := followerSvr.HandleRequestVote(tt.req)
		if !reflect.DeepEqual(actualResp, tt.expectResp) {
			t.Errorf("case %d not ok", idx)
			t.SkipNow()
		}
	}
}

func TestServer_HandleClientRequest(t *testing.T) {
	newServer(
		[]string{
			"localhost:8002",
			"localhost:8003",
		},
		"localhost:8001",
	)
	newServer(
		[]string{
			"localhost:8001",
			"localhost:8003",
		},
		"localhost:8002",
	)
	newServer(
		[]string{
			"localhost:8001",
			"localhost:8002",
		},
		"localhost:8003",
	)

	ch := make(chan struct{})
	ch <- struct{}{}
}
