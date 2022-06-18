package raft

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type Rpc interface {
	RequestVote(request *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(request *AppendEntriesRequest) (*AppendEntriesResponse, error)
}

type memoryRpc struct {
	server *server
}

func newMemoryRpc(svr *server) *memoryRpc {
	return &memoryRpc{server: svr}
}

func (r *memoryRpc) RequestVote(request *RequestVoteRequest) (*RequestVoteResponse, error) {
	resp := r.server.HandleRequestVote(request)
	return resp, nil
}

func (r *memoryRpc) AppendEntries(request *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp := r.server.HandleAppendEntries(request)
	return resp, nil
}

type httpRpc struct {
	endpoint   string
	httpClient *http.Client
}

func newHttpRpc(endpoint string) *httpRpc {
	return &httpRpc{endpoint: endpoint}
}

func (r *httpRpc) RequestVote(request *RequestVoteRequest) (*RequestVoteResponse, error) {
	b, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, "/requestvote", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	rb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var requestVoteResponse *RequestVoteResponse
	if err := json.Unmarshal(rb, requestVoteResponse); err != nil {
		return nil, err
	}

	return requestVoteResponse, nil
}

func (r *httpRpc) AppendEntries(request *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	b, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, "/appendentries", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	rb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var appendEntriesResponse *AppendEntriesResponse
	if err := json.Unmarshal(rb, appendEntriesResponse); err != nil {
		return nil, err
	}

	return appendEntriesResponse, nil
}
