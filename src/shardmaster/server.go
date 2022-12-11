package shardmaster

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
import "errors"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}



type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by conf num
  seqNum int
  min int
  
}

type Op struct {
  // Your data here.
  Operation string // Join, Leave, Move, Query, Err
  Seq string
  Args interface{}
}

func (sm *ShardMaster) GetConsensus(seq int) (op Op) {
  t := time.Millisecond*10
  for {
    done, v := sm.px.Status(seq)
    if done {
      return v.(Op)//interface
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

func (sm *ShardMaster) SetLog(count int) (rst interface{}, err error) {
  err = nil
  rst = ""
  for ; sm.min <= count; sm.min++ {
    done, v := sm.px.Status(sm.min)
    if done {
      rst = sm.handleOP(v.(Op))
    } else {
      op := Op{"", strconv.Itoa(sm.me) + "&" + strconv.Itoa(sm.seqNum), ""}
      sm.px.Start(sm.min, op)
      op = sm.GetConsensus(sm.min)
      if op.Operation == "Err" {
        return "err", errors.New("Err")
      } else {
        rst = sm.handleOP(op)
      }
    }
  }
  sm.px.Done(sm.min - 1)
  return rst, err
}

func (sm *ShardMaster) handleOP(op Op) interface{} {
  if op.Operation == "Join" {
    joinargs := op.Args.(JoinArgs)
    conf := sm.getConfig()
    conf.Num += 1
    server := joinargs.Servers[:]
    _, ok :=conf.Groups[joinargs.GID]
    if ok == true {
      for i:=0; i<len(server); i++ {
        j := 0
        for ; j<len(conf.Groups[joinargs.GID]); j++ {
          if conf.Groups[joinargs.GID][j] == server[i] {
            break
          }
        }
      }
    } else {
      conf = sm.moveToOneShard(joinargs.GID, conf)
      conf.Groups[joinargs.GID] = server
    }
    sm.configs = append(sm.configs, conf)
    return ""
  }
  if op.Operation == "Leave" {
    leaveargs := op.Args.(LeaveArgs)
    conf := sm.getConfig()
    conf.Num += 1
    delete(conf.Groups, leaveargs.GID)
    conf = sm.moveToShards(leaveargs.GID, conf)
    sm.configs = append(sm.configs, conf)
    return ""
  }
  if op.Operation == "Move" {
    moveargs := op.Args.(MoveArgs)
    conf := sm.getConfig()
    conf.Num += 1
    conf.Shards[moveargs.Shard] = moveargs.GID
    sm.configs = append(sm.configs, conf)
    return ""
  }
  if op.Operation == "Query" {
    n := op.Args.(QueryArgs).Num
    if n >= len(sm.configs) || n<0 {
      n = len(sm.configs)-1
    }
    return sm.configs[n]
  }
  return ""
}

func (sm *ShardMaster) moveToOneShard(GID int64, conf Config) Config {
  new_conf := make(map[int64]int)
  new_conf[GID] = 0
  for k := range(conf.Groups) {
    new_conf[k] = 0
  }
  for i:= 0; i<len(conf.Shards); i++ {
    if conf.Shards[i] > 0 {
      new_conf[conf.Shards[i]] += 1
    }
  }
  avg := NShards / (len(conf.Groups)+1)
  res := NShards % (len(conf.Groups)+1)
  count := len(conf.Groups)+1-res
  for k, v := range(new_conf) {
    if v <= avg {
      if count <= 0 {
        new_conf[k] = v-avg-1
        res -= 1
      } else {
        new_conf[k] = v-avg
        count -= 1
      }
    } else {
      if res <= 0 {
        new_conf[k] = v-avg
        count -= 1
      } else {
        new_conf[k] = v-avg-1
        res -= 1
      }
    }
  }
  for i := 0; i<len(conf.Shards); i++ {
    if new_conf[conf.Shards[i]] > 0 {
      new_conf[conf.Shards[i]] -= 1
      conf.Shards[i] = 0
    }
  }
  p := 0
  for k,v:=range(new_conf) {
    for v < 0 {
      for conf.Shards[p] != 0 {
        p += 1
      }
      conf.Shards[p] = k
      v += 1
      p += 1
    }
  }
  return conf
}

func (sm *ShardMaster) moveToShards(GID int64, conf Config) Config {
  for i:=0; i<len(conf.Shards); i++ {
    if conf.Shards[i]== GID {
      conf.Shards[i] = 0
    }
  }
  new_conf := make(map[int64]int)
  for k := range(conf.Groups) {
    new_conf[k] = 0
  }
  if len(conf.Groups) == 0 {
    return conf
  }
  avg := NShards / (len(conf.Groups))
  res := NShards % (len(conf.Groups))
  count := len(conf.Groups)-res
  for i:=0; i<len(conf.Shards); i++ {
    if conf.Shards[i] > 0 {
      new_conf[conf.Shards[i]] += 1
    }
  }
  for k, v := range(new_conf) {
    if v <= avg {
      if count <= 0 {
        new_conf[k] = v-avg-1
        res -= 1
      } else {
        new_conf[k] = v-avg
        count -= 1
      }
    } else {
      if res <= 0 {
        new_conf[k] = v-avg
        count -= 1
      } else {
        new_conf[k] = v-avg-1
        res -= 1
      }
    }
  }
  for i:=0; i<len(conf.Shards); i++ {
    if new_conf[conf.Shards[i]] > 0 {
      conf.Shards[i] = 0
      new_conf[conf.Shards[i]] -= 1
    }
  }
  p := 0
  for k,v:=range(new_conf) {
    for v < 0 {
      for conf.Shards[p] != 0 {
        p += 1
      }
      conf.Shards[p] = k
      v += 1
      p += 1
    }
  }
  return conf
}

func (sm *ShardMaster) getConfig() Config {
  c := Config{}
  c_now := sm.configs[len(sm.configs)-1]
  c.Num = c_now.Num
  c.Shards = c_now.Shards
  c.Groups = make(map[int64][]string)
  for k, v := range(c_now.Groups) {
    c.Groups[k] = v[:]
  }
  return c
}

func (sm *ShardMaster) dealOperation(op Op) (interface{}, error) {
  sm.mu.Lock()
  n := -1
  attempts := 7
  for i:=0;i<attempts;i++ {
    m := sm.px.Max() + 1
    n = n + 1
    if n < m {
      n = m
    }
    sm.px.Start(n, op)
    decideop := sm.GetConsensus(n)
    if op.Seq == decideop.Seq && decideop.Operation != "Err" {
      rst, ok := sm.SetLog(n)
      sm.mu.Unlock()
      if ok == nil {
        return rst, nil
      } else {
        return "", errors.New("Err")
      }
    } else if decideop.Operation == "Err" {
      sm.mu.Unlock()
      return "", errors.New("Err")
    }
  }
  sm.mu.Unlock()
  return "", errors.New("Err")
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  _, Error := sm.dealOperation(Op{"Join", strconv.Itoa(sm.seqNum)+"/"+strconv.Itoa(sm.me), *args})
  return Error
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  _, Error := sm.dealOperation(Op{"Leave", strconv.Itoa(sm.seqNum)+"/"+strconv.Itoa(sm.me), *args})
  return Error
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  _, Error := sm.dealOperation(Op{"Move", strconv.Itoa(sm.seqNum)+"/"+strconv.Itoa(sm.me), *args})
  return Error
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  rst, Error := sm.dealOperation(Op{"Query", strconv.Itoa(sm.seqNum)+"/"+strconv.Itoa(sm.me), *args})
  
  if Error == nil {
    reply.Config = rst.(Config)
  }
  return Error
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(JoinArgs{})
  gob.Register(JoinReply{})
  gob.Register(LeaveArgs{})
  gob.Register(LeaveReply{})
  gob.Register(MoveArgs{})
  gob.Register(MoveReply{})
  gob.Register(QueryArgs{})
  gob.Register(QueryReply{})



  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}


  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l
  sm.seqNum = 0
  sm.min = 0

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
