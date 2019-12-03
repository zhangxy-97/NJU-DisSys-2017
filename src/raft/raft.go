
package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, is"LEADER")
//   start agreement on a new log entry
// rf.GetState() (term, is"LEADER")
//   ask a Raft for its current term, and whether it thinks it is "LEADER"
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//


import "sync"
import "labrpc"
import "math/rand"
import "time"
import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	HeartbeatTime  = 100
	ElectionMinTime = 150
	ElectionMaxTime = 300
)


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	if data != nil{
		r := bytes.NewBuffer(data)
	    d := gob.NewDecoder(r)
	    d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.logs)
	}

}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntryReply struct {
	Term        int        //currentTerm, for leader to update itself
	Success     bool       //true if follower cantained entry matching prevLogIndex and prevLogTerm
	CommitIndex int
}


type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int //candidateId that received vote in current term (or null if none)
	logs        []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// votes COUNT
	votesCount int

	state   string
	applyCh chan ApplyMsg

	timer *time.Timer
}

func (rf *Raft) restartTime() {
	randst := ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime)
	timeout := time.Millisecond * time.Duration(randst)
	if rf.state == "LEADER" {
		timeout = HeartbeatTime * time.Millisecond
		randst = HeartbeatTime
	}
	if rf.timer == nil {
		rf.timer = time.NewTimer(timeout)
		go func() {
			for {
				<-rf.timer.C
				rf.Timeout()
			}

		}()
	}
	rf.timer.Reset(timeout)
}


func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == "LEADER" {
		isLeader = true
	}else{
		isLeader = false
	}
	return term, isLeader
}


type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}


type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	can_vote := true      

	if len(rf.logs)>0{
        //rf is voter, args is candidate
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm{
			can_vote = false
		}
		if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
			can_vote = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 && can_vote{
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)

		return
	}

	if args.Term > rf.currentTerm {

		rf.state = "FOLLOWER"
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if(can_vote){
			rf.votedFor = args.CandidateId
			rf.persist()
		}

		rf.restartTime()

		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)

		return
	}

}



//
// handle vote result
//
func (rf *Raft) countVote(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//节点接收到的请求包含过期任期号 直接拒绝
	if reply.Term < rf.currentTerm {
		return
	}

	//发现自己的任期号过期，则更新到最新任期号，并恢复成追随者
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1
		rf.restartTime()
		return
	}

	//当前节点处于candidate状态并接收到了投票，票数成为大多数时转换成leader状态，并更新相应数据结构
	if rf.state == "CANDIDATE" && reply.VoteGranted {
		rf.votesCount += 1
		if rf.votesCount >= (len(rf.peers))/2 + 1 {
			rf.state = "LEADER"
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = -1
			}
			rf.restartTime()
		}
		return
	}

}

//
// example AppendEntry RPC arguments structure.
//
type AppendEntryArgs struct {
	Term         int            //leader's term
	Leader_id    int            //so follower can redirect clients
	PrevLogIndex int            //index of log entry immediately preceding new ones
	PrevLogTerm  int            //term of prevLogIndex entry
	Entries      []LogEntry     //log entries to store
	LeaderCommit int            //leader's commitIndex
}


//
// receiver implementation
//
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		//Reply false, if term < currentTerm
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.state = "FOLLOWER"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = args.Term

		//add EX02
		if args.PrevLogIndex >= 0 && 
		(len(rf.logs)-1 < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm){
			//if an existing entry conflicts with a new one (missing entries || same index but different terms)
			index := len(rf.logs)-1
			for index >= 0{
				if(args.PrevLogTerm == rf.logs[index].Term){
					break
				}
				index--
			}
			reply.CommitIndex = index //获取二者达成一致的log index
			reply.Success = false
		}else if args.Entries != nil{
			//append new entries not already in the log 
			rf.logs = rf.logs[:args.PrevLogIndex + 1]
			rf.logs = append(rf.logs, args.Entries...)
			if len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commit()
			}
			rf.persist()
			reply.CommitIndex = len(rf.logs) - 1
			reply.Success = true
		}else{
			if len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commit()
			}
			reply.CommitIndex = args.PrevLogIndex  //没提交新的logs 所以仍返回原index
			reply.Success = true
		}

	}
	rf.restartTime()
}

//将lastApplied+1到commitIndex之间的command提交到rf.applyCh用于通道传输
func (rf* Raft) commit(){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	i := rf.lastApplied + 1
	for i <= rf.commitIndex{
		var args ApplyMsg
		args.Index = i+1
		args.Command = rf.logs[i].Command
		rf.applyCh <- args
		i++
	}
	rf.lastApplied = rf.commitIndex
}

//
// send AppendEtries to all follwers
//
func (rf *Raft) SendAppendEntries() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		//AppendEntryArgs is empty  -->  used as heartbeat
		var args AppendEntryArgs
		//add Ex02
		args.Term = rf.currentTerm
		args.Leader_id = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1   //发送给第i个server 的下一个entry的index-1
		if args.PrevLogIndex >= 0{
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term    //前一个LogEntry中的Term
		}
		if rf.nextIndex[i] < len(rf.logs){
			args.Entries = rf.logs[rf.nextIndex[i]:]
		}
		args.LeaderCommit = rf.commitIndex
		
		go func(server int, args AppendEntryArgs) {
			var reply AppendEntryReply
			ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

//
// AppendEntries RPCs are initiated by leaders to replicate log entries and to provide a form of heartbeat
//
func (rf *Raft) handleAppendEntries(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != "LEADER" {
		return
	}

	// "LEADER" should degenerate to Follower
	if reply.Term > rf.currentTerm { 
		rf.currentTerm = reply.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1
		rf.restartTime()
		return
	}

	//If successful: update nextIndex and matchindex for follower
	if reply.Success{
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.matchIndex[server] = reply.CommitIndex
		count :=1
		i :=0
		for i < len(rf.peers){
			if i!=rf.me && rf.matchIndex[i] >= rf.matchIndex[server]{
				count += 1
			}
			i++	
		}
		
		//只有leader当前任期的entry才可以通过计数来判断是否提交
		if count >= len(rf.peers)/2+1{
			if rf.commitIndex < rf.matchIndex[server] &&
			 rf.logs[rf.matchIndex[server]].Term == rf.currentTerm {
				rf.commitIndex = rf.matchIndex[server]
				go rf.commit()
			}
		}
	}else{
		//If AppendEntries fails because of log inconsistency, decrement nextIndex and retry
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.SendAppendEntries()
	}


}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the "LEADER", returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the "LEADER"
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the "LEADER".
//

//add EX02  --> 开始一个过程：将client下一条command添加进Raft的log并达成一致。
//需要实现：如果不是leader则返回，初始化LogEntry将必要信息添加进去，并为返回值赋值。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	if rf.state != "LEADER"{
		return index, term, isLeader
	}

	var newLog LogEntry
	newLog.Command = command
	newLog.Term = rf.currentTerm
	rf.logs = append(rf.logs, newLog)
	rf.persist()

	index = len(rf.logs)
	term = rf.currentTerm
	isLeader = true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// when peer timeout, it changes to be a candidate and sendRequestVote.
//
func (rf *Raft) Timeout() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "LEADER" {
		rf.state = "CANDIDATE"
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.votesCount = 1
		rf.persist()

		var args RequestVoteArgs
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.logs) -1

		if args.LastLogIndex>=0{
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		}

		//send RequestVote to all followers ，ok == true says RPC was delivered
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer == rf.me {
				continue
			}

			go func(peer int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.peers[peer].Call("Raft.RequestVote", args, &reply)
				if ok {
					rf.countVote(reply)
				}
			}(peer, args)

		}
	} else {
		rf.SendAppendEntries()
	}
	rf.restartTime()
}



//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = "FOLLOWER"
	rf.applyCh = applyCh

	rf.logs = make([]LogEntry,0)
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.restartTime()

	return rf
}
