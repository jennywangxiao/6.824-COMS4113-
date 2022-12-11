package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  mu sync.Mutex
  clients map[int64]map[int64]string
  view viewservice.View
  data map[string]string
}



func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  status := pb.ClientStatus(args.ID, args.Counter)
  if pb.view.Primary != pb.me {
    reply.Err = ErrWrongServer
  } else if pb.view.Primary == pb.me && status == true {
    reply.PreviousValue = pb.clients[args.ID][args.Counter]
    reply.Err = OK
  } else {
    if args.DoHash {
      prev, status := pb.data[args.Key]
      if status == false {
        prev = ""
      }
      args.Value = strconv.Itoa(int(hash(prev + args.Value)))
      reply.PreviousValue = prev
    }
    reply.Err = OK
    pb.data[args.Key] = args.Value
    pb.clients[args.ID][args.Counter] = reply.PreviousValue
    if pb.view.Backup != "" && status == false {
      new_args := &UpdateArgs{Key: args.Key, Value: args.Value, ID:args.ID, Counter: args.Counter}
      var reply PutUpdateReply
      res := call(pb.view.Backup, "PBServer.BackupPut", new_args, &reply)
      for reply.Err != OK || ! res {
        time.Sleep(viewservice.PingInterval)
        pb.tick()
        res = call(pb.view.Backup, "PBServer.BackupPut", new_args, &reply)
      }
    }
  }

  return nil
}

func (pb *PBServer) ClientStatus(id int64, counter int64) bool {
  c, status := pb.clients[id]
  if status == false {
    c = make(map[int64]string)
    pb.clients[id] = c
  }
  _, status = c[counter]
  return status
}

func (pb * PBServer) BackupPut(args *UpdateArgs, reply *PutUpdateReply) error {
  if pb.view.Backup != pb.me {
    reply.Err = ErrWrongServer
  } else {
    reply.Err = OK
    pb.data[args.Key] = args.Value
    pb.ClientStatus(args.ID, args.Counter)
    pb.clients[args.ID][args.Counter] = args.Value
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.view.Primary != pb.me {
    reply.Err = OK
    reply.Value = pb.data[args.Key]
  } else {
    reply.Err = OK
    status := pb.ClientStatus(args.ID, args.Counter)
    reply.Value = pb.data[args.Key]
    pb.clients[args.ID][args.Counter] = pb.data[args.Key]
    if pb.view.Backup != "" && status == false {
      new_args := &UpdateArgs{Key: args.Key, Value: reply.Value, ID:args.ID, Counter: args.Counter}
      var reply GetUpdateReply
      res := call(pb.view.Backup, "PBServer.BackupGet", new_args, &reply)
      for reply.Err != OK || ! res {
        time.Sleep(viewservice.PingInterval)
        pb.tick()
        res = call(pb.view.Backup, "PBServer.BackupGet", new_args, &reply)
      }
    }
  }

  return nil
}

func (pb * PBServer) BackupGet(args *UpdateArgs, reply *GetUpdateReply) error {
  if pb.view.Backup != pb.me {
    reply.Err = ErrWrongServer
  } else {
    reply.Err = OK
    pb.ClientStatus(args.ID, args.Counter)
    pb.clients[args.ID][args.Counter] = args.Value
  }
  return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  v, err := pb.vs.Ping(pb.view.Viewnum)
  if v != pb.view && err == nil {
    pb.view = v
    if pb.view.Primary == pb.me && pb.view.Backup != "" {
      args := &StateArgs{Data: pb.data, ClientList: pb.clients}
      var reply StateReply
      res := call(pb.view.Backup, "PBServer.StateUpdate", args, &reply)
      for (res == false || reply.Err != OK) && pb.view.Backup != "" {
        time.Sleep(viewservice.PingInterval)
        pb.tick()
        res = call(pb.view.Backup, "PBServer.StateUpdate", args, &reply)
      }
      pb.vs.Ping(pb.view.Viewnum)
    }
  }
}

func (pb *PBServer) StateUpdate(args *StateArgs, reply *StateReply) error {
  if pb.me == pb.view.Backup {
    reply.Err = OK
    pb.clients = args.ClientList
    pb.data = args.Data
  } else {
    reply.Err = ErrWrongServer
  }
  return nil
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.clients = make(map[int64]map[int64]string)
  pb.view = viewservice.View{0, "", ""}
  pb.data = make(map[string]string)
  


  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
