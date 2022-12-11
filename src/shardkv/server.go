package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "errors"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}



type Op struct {
  // Your definitions here.
  Operation string
  Seq int64
  Temp interface{}
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  keyValues map[string]string
  seqNum map[int64]string
  min int
  shardGroup map[string]int
  cfg shardmaster.Config
  version map[int]map[int]map[string]string
  kvlock sync.Mutex
}


func (kv *ShardKV) GetConsensus(seq int) (op Op) {
  t := time.Millisecond*10
  for {
    done, v := kv.px.Status(seq)
    if done {
      return v.(Op)
    }
    time.Sleep(t)
    if t < time.Second {
      t = 2*t
    } else {
      op.Operation = "Err"
      return op
    }
  }
}

func (kv *ShardKV) SetLog(max int) (rst interface{}, err error) {
  err = nil
  rst = ""
  for ; kv.min <= max; kv.min++ {
    done, v := kv.px.Status(kv.min)
    if done {
      rst = kv.handleOP(v.(Op))
    } else {
      op := Op{"", 0, ""}
      kv.px.Start(kv.min, op)
      op = kv.GetConsensus(kv.min)
      if op.Operation == "Err" {
        return "err", nil
      } else {
        rst = kv.handleOP(op)
      }
    }
  }
  kv.px.Done(kv.min - 1)
  return rst, err
}

func (kv *ShardKV) Shard(args *ValueArgs, reply *ValueReply) error {
  kv.kvlock.Lock()
  defer kv.kvlock.Unlock()
  view, ok := kv.version[args.VID]
  if !ok {
    return nil
  }
  sKV, okk := view[args.ShardID]
  if !okk {
    return nil
  }
  s := make(map[string]string)
  for key := range(sKV) {
    s[key] = sKV[key]
  }
  reply.KeyValue = s
  reply.Err = OK
  return nil
}

func (kv *ShardKV) handleOP(op Op) interface{} {
  value, done := kv.seqNum[op.Seq]
  if done {
    return value
  }
  if op.Operation == "Get" {
    a := op.Temp.(GetArgs)
    value, _ = kv.keyValues[a.Key]
    return value
  }
  if op.Operation == "Put" {
    a := op.Temp.(PutArgs)
    kv.keyValues[a.Key] = a.Value
    kv.shardGroup[a.Key] = a.ShardID
    kv.seqNum[op.Seq] = ""
    return ""
  }
  if op.Operation == "Puthash" {
    a := op.Temp.(PutArgs)
    kv.shardGroup[a.Key] = a.ShardID
    value, _ = kv.keyValues[a.Key]
    kv.seqNum[op.Seq] = value
    kv.keyValues[a.Key] = strconv.Itoa(int(hash(value+a.Value)))
    return value
  }
  if op.Operation == "Update" {
    conf := kv.cfg
    new_conf := op.Temp.(shardmaster.Config)
    for i:=conf.Num; i<new_conf.Num; i++ {
      c := kv.sm.Query(conf.Num+1)
      kv.kvlock.Lock()
      for k:=0; k<len(c.Shards); k++ {
        if conf.Shards[k] == kv.gid && c.Shards[k] != kv.gid  {
          _, ok := kv.version[i]
          if !ok {
            kv.version[i] = make(map[int]map[string]string)
          }
          new_log := make(map[string]string)
          for key := range(kv.keyValues) {
            if kv.shardGroup[key] == k {
              new_log[key] = kv.keyValues[key]
            }
          }
          kv.version[i][k] = new_log
        }
      }
      kv.kvlock.Unlock()
      for k := 0; k<len(c.Shards); k++ {
        if conf.Shards[k] > 0 && conf.Shards[k] != kv.gid && c.Shards[k] == kv.gid {
          flag := false
          for flag == false {
            s := conf.Groups[conf.Shards[k]]
            for j:=0; j<len(s); j++ {
              args := ValueArgs{k, i}
              reply := ValueReply{}
              ok := call(s[j], "ShardKV.Shard", &args, &reply)
              if reply.Err == OK && ok {
                for key, value := range(reply.KeyValue) {
                  kv.keyValues[key] = value
                  kv.shardGroup[key] = k
                }
                flag = true
                break
              } else {
                time.Sleep(15 * time.Millisecond)
              }
            }
          }
        }
      }
      kv.cfg = c
      conf = c
      
    }
    kv.seqNum[op.Seq] = ""
    kv.cfg = conf
    return ""
  }
  return ""
}

func (kv *ShardKV) dealOperation(op Op) (interface{}, error) {
  n := -1
  attempts := 7
  for i:=0;i<attempts;i++ {
    m := kv.px.Max() + 1
    n = n + 1
    if n < m {
      n = m
    }
    kv.px.Start(n, op)
    decideop := kv.GetConsensus(n)
    if op.Seq == decideop.Seq && decideop.Operation != "Err" {
      rst, ok := kv.SetLog(n)
      if ok == nil {
        return rst, nil
      } else {
        return "", errors.New("Err")
      }
    } else if decideop.Operation == "Err" {
      return "", errors.New("Err")
    }
  }
  return "", errors.New("Err")
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if args.VID != kv.cfg.Num {
    reply.Err = "Err"
    return nil
  }
  rst, Error := kv.dealOperation(Op{"Get", args.SeqNum, *args})
  if Error != nil {
    reply.Err = "Err"
  } else {
    reply.Value = rst.(string)
    reply.Err = OK
  }
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if args.VID != kv.cfg.Num {
    reply.Err = "Err"
    return nil
  }
  var op Op
  if args.DoHash {
    op = Op{"Puthash", args.SeqNum, *args}
  } else {
    op = Op{"Put", args.SeqNum, *args}
  }
  rst, Error := kv.dealOperation(op)
  if Error != nil {
    reply.Err = "Err"
  } else {
    reply.PreviousValue = rst.(string)
    reply.Err = OK
  }
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  new_conf := kv.sm.Query(-1)
  id := new_conf.Num
  if id > kv.cfg.Num {
    for {
      _, Error := kv.dealOperation(Op{"Update", int64(id), new_conf})
      if Error == nil {
        break
      }
    }
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(GetArgs{})
  gob.Register(GetReply{})
  gob.Register(PutArgs{})
  gob.Register(PutReply{})
  gob.Register(shardmaster.Config{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.keyValues = make(map[string]string)
  kv.seqNum = make(map[int64]string)
  kv.min = 0
  kv.shardGroup = make(map[string]int)
  kv.cfg = kv.sm.Query(-1)
  kv.version = make(map[int]map[int]map[string]string)

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}