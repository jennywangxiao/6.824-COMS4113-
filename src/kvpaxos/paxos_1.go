package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
	min int
	max int
	log map[int]*LogItem
	local int
}

type LogItem struct {
	Np int
	Na int
	Va interface{}
	decided bool
}

type PrepareArgs struct {
	Seq int
	N int
}

type AcceptArgs struct {
	Seq int
	N int
	V interface{}
}

type DecideArgs struct {
	Seq int
	V interface{}
}

type MinArgs struct {
	Seq int
}

type FindMinArgs struct {
}
type FindMaxArgs struct {
}

type PrepareReply struct {
	N int
	Va interface{}
	Max int
	OK bool
}

type Reply struct {
	OK bool
}

type FindMinReply struct {
	Seq int
}
type FindMaxReply struct {
	Seq int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) Prepare (args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	log, ok := px.log[args.Seq]
	if args.Seq > px.max {
		px.max = args.Seq
	}
	if ok {
		if args.N <= log.Np {
			reply.OK = false
		} else {
			reply.OK = true
			log.Np = args.N
		}
	} else {
		reply.OK = true
		log = &LogItem{args.N, -1, 0, false}
		px.log[args.Seq] = log
	}
	reply.Max = log.Np
	reply.N = log.Na
	reply.Va = log.Va
	return nil
}

func (px *Paxos) preparing(seq int, no int) (n int, max int, v interface{}, majority []int, count int) {
	majority = make([]int, len(px.peers))
	count = 0
	n = -1
	max = -1
	for i:=0; i<len(px.peers);i++ {
		args := &PrepareArgs{Seq: seq, N: no}
		reply := &PrepareReply{}
		if i != px.me {
			ok := call(px.peers[i], "Paxos.Prepare", args, reply)
			if reply.Max > max {
				max = reply.Max
			}
			if ok && reply.OK {
				majority[i] = 1
				count += 1
				if reply.N > n {
					v = reply.Va
					n = reply.N
				}
			}
		} else {
			px.Prepare(args, reply)
			if reply.OK {
				majority[px.me] = 1
				count += 1
				if reply.N > n {
					v = reply.Va
					n = reply.N
				}
			}
			if reply.Max > max {
				max = reply.Max
			}
		}
	}
	return n, max, v, majority, count
}

func (px *Paxos) Accept (args *AcceptArgs, reply *Reply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	log, ok := px.log[args.Seq]
	if args.Seq > px.max {
		px.max = args.Seq
	}
	if ok {
		if args.N < log.Np {
			reply.OK = false
		} else {
			reply.OK = true
			log.Na = args.N
			log.Va = args.V
		}
	} else {
		reply.OK = false
	}
	return nil
}

func (px *Paxos) accepting(seq int, n int, v interface{}, majority []int) int {
	count := 0

	for i := 0; i < len(px.peers); i++ {
		args := &AcceptArgs{Seq: seq, N: n, V: v}
		reply := &Reply{}
		if i != px.me {
			ok := call(px.peers[i], "Paxos.Accept", args, reply)
			if reply.OK && ok {
				count += 1
			} else {
				majority[i] = 0
			}
		} else {
			px.Accept(args, reply)
			if reply.OK {
				count += 1
			} else {
				majority[px.me] = 0
			}
		}
	}
	return count
}

func (px *Paxos) Decide(args *DecideArgs, reply *Reply) error{
	px.mu.Lock()
	if args.Seq > px.max {
		px.max = args.Seq
	}
	log, ok := px.log[args.Seq]
	if !ok {
		px.mu.Unlock()
		return nil
	}
	reply.OK = true
	log.decided = true
	px.mu.Unlock()
	return nil
}

func (px *Paxos) deciding(seq int, v interface{}, majority []int) int {
	count := 0
	for i := 0; i < len(px.peers); i++ {
		if majority[i] == 0{
			continue
		}
		args := &DecideArgs{Seq: seq, V: v}
		reply := &Reply{}
		if i != px.me {
			call(px.peers[i], "Paxos.Decide", args, reply)
			if reply.OK {
				count += 1
			}
		} else {
			px.Decide(args, reply)
			if reply.OK {
				count += 1
			}
		}
	}
	return count
}

func (px *Paxos) proposePaxos(seq int, v interface{}) {
	n := px.me + 1
	for seq >= px.min {
		n = n + len(px.peers)
		n_high, max, v_high, majority, c := px.preparing(seq, n)
	
		if 2*c < len(px.peers) {
			high := len(px.peers)*(max/len(px.peers)+100)+len(px.peers)
			if n < high {
				n = high
			}
			time.Sleep(100*time.Millisecond)
			continue
		}
		if n_high < 0 {
			v_high = v
		}
		c = px.accepting(seq, n, v_high, majority)
		if 2*c < len(px.peers) {
			time.Sleep(100*time.Millisecond)
			continue
		}
		c = px.deciding(seq, v_high, majority)
		if c == len(px.peers){
			break
		}
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
	px.mu.Lock()
	if seq < px.min {
		px.mu.Unlock()
	}
	if seq > px.max {
		px.max = seq
	}
	px.mu.Unlock()
	go px.proposePaxos(seq, v)
}


//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
	if seq >= px.local {
		px.mu.Lock()
		px.local = seq + 1
		px.mu.Unlock()
		m := px.local
		for i :=0; i < len(px.peers); i++ {
			if i != px.me {
				args := &FindMinArgs{}
				reply := &FindMinReply{}
				ok := call(px.peers[i], "Paxos.FindMin", args, reply)
				if !ok {
					m = px.min
					break
				} else if ok && reply.Seq < m {
					m = reply.Seq
				}
			}
		}
		if m > px.min {
			px.mu.Lock()
			px.min = m
			px.mu.Unlock()
			px.clear(m)
			args1 := &MinArgs{Seq: m}
			reply1 := &Reply{}
			for i:=0; i < len(px.peers); i++ {
				if i != px.me {
					call(px.peers[i],"Paxos.Sync", args1, reply1)
				}
			}
		}
	}
}
func (px *Paxos) FindMin(args *FindMinArgs, reply *FindMinReply) error {
	reply.Seq = px.local
	return nil
}

func (px *Paxos) FindMax(args *FindMaxArgs, reply *FindMaxReply) error{
	reply.Seq = px.max
	return nil
}

func (px *Paxos) Sync(args *MinArgs, rep *Reply) error{
	if args.Seq > px.min {
		px.mu.Lock()
		px.min = args.Seq
		if px.min > px.local {
			px.local = px.min
		}
		px.mu.Unlock()
		px.clear(px.min)
	}
	return nil
}

func (px *Paxos) clear (seq int) {
	px.mu.Lock()
	for i,_ := range(px.log) {
		if i < seq {
			delete(px.log, i)
		}
	}
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
	m := px.max
	args := &FindMaxArgs{}
	reply := &FindMaxReply{}
	for i := 0; i < len(px.peers); i++ {
		if i != px.me {
			ok := call(px.peers[i], "Paxos.FindMax", args, reply)
			if ok && m < reply.Seq {
				m = reply.Seq
			}
		}
	}
	if m > px.max {
		px.mu.Lock()
		px.max = m
		px.mu.Unlock()
	}

  return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
	return px.min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	flag := false
	var v interface{} = nil
	if px.min > seq {
		flag = false
	}
	log, ok := px.log[seq]
	if ok && log.decided {
		flag = true
		v = log.Va
	}

  return flag, v
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
	px.min = 0
	px.max = -1
	px.log = make(map[int]*LogItem)
	px.local = 0

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // Prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}