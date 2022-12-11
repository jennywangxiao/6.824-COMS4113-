package paxos

import (
	"coms4113/hw5/pkg/base"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//TODO: implement it
	nodeSet := make([]base.Node, 0)
	switch message.(type) {
	case *ProposeRequest:
		n := server.copy()
		prop := message.(*ProposeRequest)
		if prop.N <= n.n_p {
			n.Response = []base.Message{
				&ProposeResponse{CoreMessage: base.MakeCoreMessage(prop.To(), prop.From()),
					Ok: false,
					N_p: n.n_p,
					SessionId: prop.SessionId},
			}
		} else {
			n.n_p = prop.N
			n.Response = []base.Message{
				&ProposeResponse{CoreMessage: base.MakeCoreMessage(prop.To(), prop.From()),
					Ok: true,
					N_p: prop.N,
					N_a: n.n_a,
					V_a: n.v_a,
					SessionId: prop.SessionId},
			}
		}
		nodeSet = append(nodeSet, n)

	case *ProposeResponse:
		resp := message.(*ProposeResponse)
		num := server.getNumber(resp.From())
		if server.proposer.Responses[num] == true {
			n:= server.copy()
			nodeSet = append(nodeSet, n)
		} else {
			if resp.Ok && server.proposer.SuccessCount+1 > len(server.peers)/2 {
				n := server.copy()
				n.proposer.Phase = "accept"
				n.proposer.SuccessCount = 0
				n.proposer.ResponseCount = 0
				n.proposer.Responses = make([]bool, len(n.peers))
				if resp.N_a > n.proposer.N_a_max {
					n.proposer.N_a_max = resp.N_a
					n.proposer.V = resp.V_a
				}
				respSet := make([]base.Message, 0)
				for _, p := range server.peers {
					r := &AcceptRequest{CoreMessage: base.MakeCoreMessage(n.Address(), p),
						N: resp.N_p,
						V: n.proposer.V,
						SessionId: resp.SessionId}
					respSet = append(respSet, r)
				}
				n.Response = respSet
				nodeSet = append(nodeSet, n)
			}
			n := server.copy()
			n.proposer.Responses[num] = true
			n.proposer.ResponseCount += 1
			if resp.Ok {
				n.proposer.SuccessCount += 1
				if resp.N_a > n.proposer.N_a_max {
					n.proposer.N_a_max = resp.N_a
					n.proposer.V = resp.V_a
				}
			}
			nodeSet = append(nodeSet, n)
			
		}

	case *AcceptRequest:
		n := server.copy()
		acc := message.(*AcceptRequest)
		if acc.N < n.n_p {
			n.Response = []base.Message{
				&AcceptResponse{CoreMessage: base.MakeCoreMessage(acc.To(),acc.From()),
					Ok: false,
					N_p: n.n_p,
					SessionId: acc.SessionId},
			
			}
		} else {
			n.Response = []base.Message{
				&AcceptResponse{CoreMessage: base.MakeCoreMessage(acc.To(),acc.From()),
					Ok: true,
					N_p: acc.N,
					SessionId: acc.SessionId},
			}
			n.n_p = acc.N
			n.n_a = acc.N
			n.v_a = acc.V
		}
		nodeSet = append(nodeSet, n)

	case *AcceptResponse:
		resp := message.(*AcceptResponse)
		num := server.getNumber(resp.From())
		if server.proposer.Responses[num] == true {
			n := server.copy()
			nodeSet = append(nodeSet, n)
		} else {
			if resp.Ok && server.proposer.SuccessCount+1 >= len(server.peers)/2+1 {
				n := server.copy()
				n.proposer.Phase = "decide"
				n.proposer.SuccessCount = 0
				n.proposer.ResponseCount = 0
				n.proposer.Responses = make([]bool, len(n.peers))
				n.agreedValue = n.proposer.V
				respSet := make([]base.Message, 0)
				for _, p := range server.peers {
					r := &DecideRequest{CoreMessage: base.MakeCoreMessage(n.Address(), p),
					V: n.proposer.V,
					SessionId: resp.SessionId,
					}
					respSet = append(respSet, r)
				}
				n.Response = respSet
				nodeSet = append(nodeSet, n)
			}
			n := server.copy()
			n.proposer.Responses[num] = true
			n.proposer.ResponseCount += 1
			if resp.Ok {
				n.proposer.SuccessCount += 1
			}
			nodeSet = append(nodeSet, n)
		}

	case *DecideRequest:
		n:= server.copy()
		dec := message.(*DecideRequest)
		n.proposer.Phase = "decide"
		n.agreedValue = dec.V
		nodeSet = append(nodeSet, n)
	}

	return nodeSet
}

func (server *Server) getNumber(address base.Address) int {
	rst := -3
	for i := range server.peers {
		if server.peers[i] == address {
			rst = i
			break
		}
	}
	return rst
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	//TODO: implement it
	sess := server.proposer.SessionId + 1
	n := server.proposer.N + 1
	respSet := make([]base.Message, 0)
	for _, p := range server.peers {
		resp := &ProposeRequest{CoreMessage: base.MakeCoreMessage(server.Address(), p),
		N: n,
		SessionId: sess,}
		respSet = append(respSet, resp)
	}
	server.Response = respSet

	server.proposer.N = n
	server.proposer.Phase = Propose
	server.proposer.V = server.proposer.InitialValue
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	server.proposer.SessionId = sess
	
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
