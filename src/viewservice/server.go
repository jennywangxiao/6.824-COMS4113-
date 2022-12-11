package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  Viewnum map[string]uint
  v View
  nextV View
  ack bool
  lastHeard map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()
  vs.lastHeard[args.Me] = time.Now()

  if vs.v.Primary == args.Me {
    if vs.v.Viewnum == args.Viewnum {
      vs.ack = true
    }
    if args.Viewnum == 0 {
      vs.nextV.Viewnum = vs.v.Viewnum + 1
      vs.nextV.Primary = vs.v.Backup
      vs.nextV.Backup = args.Me
    }
  } else if vs.v.Backup == args.Me {
    if args.Viewnum == 0 {
      vs.nextV.Viewnum = vs.v.Viewnum + 1
      vs.nextV.Primary = vs.v.Primary
      vs.nextV.Backup = args.Me
    }
  } else {
    if vs.v.Primary == "" {
      vs.nextV.Viewnum = vs.v.Viewnum + 1
      vs.nextV.Primary = args.Me
      if vs.v.Viewnum == args.Viewnum {
        vs.ack = true
      }
    } else if vs.v.Backup == "" {
      vs.nextV.Viewnum = vs.v.Viewnum + 1
      vs.nextV.Backup = args.Me
    }
  }

  if vs.v != vs.nextV && vs.ack {
    vs.v = vs.nextV
    vs.ack = false
  }
  reply.View = vs.v
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()
  reply.View = vs.v

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  deadTime := PingInterval * DeadPings
  t := time.Now()
  t_primary := t.Sub(vs.lastHeard[vs.v.Primary])
  if t_primary > deadTime {
    vs.nextV.Primary = vs.nextV.Backup
    vs.nextV.Backup = ""
  }
  t_backup := t.Sub(vs.lastHeard[vs.v.Backup])
  if t_backup > deadTime {
    vs.nextV.Backup = ""
  }
  if vs.nextV.Primary != vs.v.Primary || vs.nextV.Backup != vs.v.Backup {
    vs.nextV.Viewnum = vs.v.Viewnum + 1
  }
  if vs.v != vs.nextV && vs.ack {
    vs.v = vs.nextV
    vs.ack = false
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.Viewnum = make(map[string]uint)
  vs.v = View{}
  vs.nextV = View{}
  vs.ack = true
  vs.lastHeard = make(map[string]time.Time)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
