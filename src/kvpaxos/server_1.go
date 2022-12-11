package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  SeqNum int64
  Type string // Get, Push, Puthash, or Err
  Key string
  Value string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  keyValues map[string]string
  seqNum map[int64]string
  min int
}

func (kv *KVPaxos) GetConsensus(seq int) (op Op) {
  t := time.Millisecond*10
  for {
    done, v := kv.px.Status(seq)
    if done {
      return v.(Op)//interface
    }
    time.Sleep(t)
    if t < time.Second {
      t = 2*t
    } else {
      op.Type = "Err"
      return op
    }
  }
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  n := -1
  attempts := 7
  for i:=0;i<attempts;i++ {
    n += 1
    m := kv.px.Max()+1
    if n < m {
      n = m
    }
    op := FormGet(args)
    kv.px.Start(n,op)
    decideop := kv.GetConsensus(n)
    if op == decideop && decideop.Type != "ERR" {
      rst, ok := kv.SetLog(n)
      if ok == "OK" {
        reply.Err = "OK"
        reply.Value = rst
      } else if decideop.Type == "ERR" {
        kv.mu.Unlock()
        return nil
      }
      kv.mu.Unlock()
      return nil
    }
  }
  kv.mu.Unlock()
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  // Your code here.
  n := -1
  attempts := 7
  for i:=0;i<attempts;i++ {
    m := kv.px.Max()+1
    n = n + 1
    if n < m {
      n = m
    }
    op := FormPut(args)
    kv.px.Start(n, op)
    decideop := kv.GetConsensus(n)
    if op == decideop && decideop.Type != "ERR" {
      rst, ok := kv.SetLog(n)
      if ok == "OK" {
        reply.Err = "OK"
        reply.PreviousValue = rst
      }
      kv.mu.Unlock()
      return nil
    } else if decideop.Type == "ERR" {
      kv.mu.Unlock()
      return nil
    }
  }
  kv.mu.Unlock()
  return nil
}

func FormPut(args *PutArgs) (op Op) {
  op.SeqNum = args.SeqNum
  if args.DoHash {
    op.Type = "Puthash"
  } else {
    op.Type = "Put"
  }
  op.Key = args.Key
  op.Value = args.Value
  return op
}

func FormGet(args *GetArgs) (op Op) {
  op.SeqNum = args.SeqNum
  op.Type = "Get"
  op.Key = args.Key
  return op
}

func (kv *KVPaxos) handleOP(op Op) string {
  rst := ""
  v, ok := kv.seqNum[op.SeqNum]
  if ok {
    return v
  }
  if op.Type == "Get" {
    rst, ok = kv.keyValues[op.Key]
    if !ok {
      rst = ""
    }
  } else if op.Type == "Put" {
    kv.keyValues[op.Key] = op.Value
    kv.seqNum[op.SeqNum] = ""
  } else if op.Type == "Puthash" {
    rst, ok = kv.keyValues[op.Key]
    if !ok {
      rst = ""
    }
    kv.seqNum[op.SeqNum] = rst
    kv.keyValues[op.Key] = strconv.Itoa(int(hash(rst+op.Value)))
  } else {
    rst = ""
  }
  return rst
}


func (kv *KVPaxos) SetLog(count int) (string, string) {
  rst := ""
  for ; kv.min <= count; kv.min++ {
    done, v := kv.px.Status(kv.min)
    if done {
      rst = kv.handleOP(v.(Op))
    } else {
      op := Op{SeqNum: 0, Type: "", Value: "", Key: ""}
      kv.px.Start(kv.min, op)
      op = kv.GetConsensus(kv.min)
      if op.Type == "Err" {
        return rst, "ERR"
      } else {
        rst = kv.handleOP(op)
      }
    }
  }
  kv.px.Done(kv.min-1)
  return rst, "OK"
}



// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.keyValues = make(map[string]string)
  kv.seqNum = make(map[int64]string)
  kv.min = 0


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}