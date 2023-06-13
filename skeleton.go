package main

import (
	// "bufio"
	// "flag"
	"fmt"
	// "math"
	// "os"
	// "reflect"
	// "strconv"
	// "strings"
	"math/rand"
	"sync"
	"time"
)

var globalInd int = 0

type Message interface {
	messageType() string
}

type LogEntry struct {
	term  int
	index int
	command   string
}

type RequestVoteArgs struct {
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

type RequestVoteReply struct {
	term int
	voteGranted bool
	id int
}

type AppendEntriesArgs struct {
	term     int
	leaderId int

	prevLogIndex int
	prevLogTerm int
	entries []LogEntry
	leaderCommit int
}

type AppendEntriesReply struct {
	term int
	success bool
	id int
	// index < leader index, leader will can send starting from here
	// nextIndex int
}

func (r RequestVoteArgs) messageType() string {
    return "RV"
}
func (r RequestVoteReply) messageType() string {
    return "RVR"
}
func (r AppendEntriesArgs) messageType() string {
    return "AE"
}
func (r AppendEntriesReply) messageType() string {
    return "AER"
}

type Raft struct {
	lock 		sync.Mutex 	// protects concurrent access to state
	id 		 	int 		// server id
    leaderId 	int 		//so follower can redirect clients

	// Persistent state
	currentTerm int 		//latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int 		//candidateId that received vote in current term (or -1 if none)
	log         []LogEntry 	//log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state
	commitIndex int			//index of highest log entry known to be committed (initialized to 0, increases	monotonically)
	lastApplied int			//index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders: (Reinitialized after election)
	nextIndex[] int			//for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex[] int		//for each server, index of highest log entry known to be replicated on server	(initialized to 0, increases monotonically)

	state 		string
	peers		map[int](chan Message) 	//allows you to send to all other channels
	receiveCh 	chan Message		 	//allows you to receive
}

func makeRaft(m map[int]chan Message, i int, n int) *Raft{
	rf := &Raft{
		id: i,
		leaderId: -1,
		currentTerm: 0,
		votedFor: -1,
		log: make([]LogEntry,0,10),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex: make([]int,n),
		matchIndex: make([]int,n),
		state: "Follower",
		peers: m,
		receiveCh: m[i],
	}
	return rf;
}

func (rf *Raft) start() {
	rf.currentTerm = 0
	rf.votedFor = -1
    rf.state = "Follower"

	for {
		fmt.Printf("server %d state %s, term %d\n",rf.id,rf.state,rf.currentTerm)
        switch rf.state {
        case "Follower":
            rf.follower()
        case "Candidate":
            rf.candidate()
        case "Leader":
            rf.leader()
        }
    }
}

//creates clients, servers
func main() {

	var numServers = 5
	messageCh := make(map[int]chan Message, numServers)
	for i := 0; i<numServers; i++ {
		messageCh[i] = make(chan Message)
	}

	raftNodes := make([]Raft, numServers)
	for i := range raftNodes {
		raftNodes[i] = *makeRaft(messageCh,i,numServers)
	}

	for i := range raftNodes {
		fmt.Printf("starting node %d\n",i)
		go raftNodes[i].start()
	}
	time.Sleep(50 * time.Second)
}

func (rf *Raft) replyRequestVote(message Message) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rep := RequestVoteReply{
		term: -1,
		voteGranted:false,
		id: -1,
	}
	args := message.(RequestVoteArgs)
	rf.requestVote(&args,&rep)
	rf.peers[args.candidateId] <- rep
}

func (rf *Raft) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// make a reply
	//if lower term, vote no
	//if previously voted for or no votes, give vote
	//assuming that it is at least as updated
}

func (rf *Raft) replyAppendEntries(message Message) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rep := AppendEntriesReply{
		term: -1,
		success:false,
		id: -1,
		// nextIndex: -1,
	}
	args := message.(AppendEntriesArgs)
	rf.AppendEntries(&args,&rep)
	rf.peers[args.leaderId] <- rep
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	// make a reply
}

func (rf *Raft) lastLogIndex() int {
	l := len(rf.log)
	if l == 0 {
		return 0
	}
	return rf.log[l-1].index
}

func (rf *Raft) lastLogTerm() int {
	l := len(rf.log)
	if l == 0 {
		return 0
	}
	return rf.log[l-1].term
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4)
func (rf *Raft) updatecommitindex() {
	for ind := len(rf.log) - 1; ind >= 0; ind-- {
		if rf.log[ind].term == rf.currentTerm && rf.countReplicas(ind) > len(rf.peers)/2 {
			rf.commitIndex = ind
			break
		}
	}
}

// Count number of servers that have replicated the log up to index i
func (rf *Raft) countReplicas(i int) int {
	count := 0
	for _, index := range rf.matchIndex {
		if index >= i {
			count++
		}
	}
	return count
}

func (rf *Raft) follower(){
	timeout := time.Duration(500+rand.Intn(250)) * time.Millisecond
	timer := time.NewTimer(timeout)
	select {
	case message := <-rf.receiveCh:
		switch message.messageType() {
		case "AE":
			rf.replyAppendEntries(message)
		case "RV":
			rf.replyRequestVote(message)
		default:
		}
	case <- timer.C:
		rf.state = "Candidate"
		return
	}
}

func (rf *Raft) candidate(){
	timeout := time.Duration(500+rand.Intn(250)) * time.Millisecond
	timer := time.NewTimer(timeout)

	rf.currentTerm++
    rf.votedFor = rf.id
    totalVotes := 1
	yesVotes := 1

	for i := range rf.peers {
		if i == rf.id {
			continue
		}
		req := RequestVoteArgs{
			term: rf.currentTerm,
			candidateId: rf.id,
			lastLogIndex: rf.lastLogIndex(),
			lastLogTerm: rf.lastLogTerm(),
		}
		rf.peers[i] <- req
    }

	for {
        select {
		case message := <-rf.receiveCh:
			switch message.messageType() {
			case "AE":
				args := message.(AppendEntriesArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
                    rf.state = "Follower"
					rf.replyAppendEntries(message)
					return
                }
				rf.replyAppendEntries(message)
			case "RV":
				args := message.(RequestVoteArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
                    rf.state = "Follower"
					rf.replyRequestVote(message)
					return
                }
				rf.replyRequestVote(message)
			case "RVR":
				args := message.(RequestVoteReply)
				if args.term > rf.currentTerm { // shouldnt ever happen
					fmt.Printf("ERROR: REQUEST VOTE RESPONSE FROM LARGER TERM")
                    rf.currentTerm = args.term
                    rf.state = "Follower"
                    return
                }
				totalVotes ++
                if args.voteGranted {
                    yesVotes++
                }
                if yesVotes > len(rf.peers)/2 {
                    rf.state = "Leader"
					rf.nextIndex = make([]int, len(rf.peers))
					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.lastLogIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					rf.leaderId = rf.id
					rf.sendHeartbeat(true)
					newEntry := LogEntry{
						term: rf.currentTerm,
						index: globalInd,
						command: "HELLO",
					}
					globalInd++
					rf.log = append(rf.log,newEntry)
                    return
                }
			default:
			}
		case <- timer.C:
			rf.state = "Candidate"
			return
		}
	}
}

func (rf *Raft) leader(){
	timeout := time.Duration(50) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		select {
		case message := <-rf.receiveCh:
			switch message.messageType() {
			case "AE":
				args := message.(AppendEntriesArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
                    rf.state = "Follower"
					rf.replyAppendEntries(message)
					return
                }
				rf.replyAppendEntries(message)
			case "AER":
				args := message.(AppendEntriesReply)
				if args.success {
                    rf.nextIndex[args.id] += 1
					if(rf.nextIndex[args.id] > rf.lastLogIndex()){
                		rf.nextIndex[args.id] = rf.lastLogIndex()
					}
					rf.updatecommitindex()
                } else {
                    rf.nextIndex[args.id]--
                }
			case "RV":
				args := message.(RequestVoteArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
                    rf.state = "Follower"
					rf.replyRequestVote(message)
					return
                }
				rf.replyRequestVote(message)
			default:
			}
		case <- timer.C:
			rd := rand.Intn(100)
			if rd >= 90{
				newEntry := LogEntry{
					term: rf.currentTerm,
					index: globalInd,
					command: "HELLO",
				}
				globalInd++
				rf.log = append(rf.log,newEntry)
			}
			rf.sendHeartbeat(false)
			timer.Reset(timeout)
		}
	}
}

func (rf *Raft) sendHeartbeat(hb bool){
	for i := range rf.peers {
		if i == rf.id {
			continue
		}
		prevInd := max(0, rf.nextIndex[i] - 1)
		var appends []LogEntry
		if(hb){
			appends = make([]LogEntry, 0)
		}else{
			appends = rf.log[rf.nextIndex[i]:]
		}
		req := AppendEntriesArgs{
			term:         rf.currentTerm,
			leaderId:     rf.id,
			prevLogIndex: prevInd,
			prevLogTerm:  rf.log[prevInd].term,
			entries:      appends,			//make([]LogEntry, 0),  // empty for heartbeat rf.log[rf.nextIndex[i]:],
			leaderCommit: rf.commitIndex,
		}
		rf.peers[i] <- req
    }
}

func max(a int, b int) int{
	if a > b{
		return a
	}
	return b
}