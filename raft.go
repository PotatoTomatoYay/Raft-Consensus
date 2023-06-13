package main

import (
	// "bufio"
	// "math"
	// "strings"
	"flag"
	"fmt"
	"os"
	"strconv"
	"math/rand"
	"sync"
	"time"
	"log"
)

var numServers = flag.Int("servers", 3, "number of Raft servers")
var failure_rate = flag.Float64("failure-rate", 0, "rate of server failure, as a percent")
var failure_duration = flag.Int("failure-duration", 1000, "failure duration, in milliseconds")
var requests = flag.Int("requests", 50 , "number of requests for client to send")
var log_path = flag.String("log-path", "./logs/", "path to log directory")
var timeout = flag.Int("timeout", 500, "minimum follower/candidate timeout duration, in milliseconds")
var timeout_randomness = flag.Int("timeout-randomness", 250, "additional duration for random timeouts, in milliseconds")
var heartbeat = flag.Int("heartbeat", 50, "time between heartbeats, in milliseconds")
var drop_rate = flag.Float64("drop-rate", 0, "drop rate of messages")
var wait_time = flag.Int("wait-time", 1, "time to wait before checking logs and ending program")

//lock to make sure only one server drops at a time
var sleepLock sync.Mutex
// var writeLock sync.Mutex

//every rpc call and response is a message
type Message interface {
	messageType() string
}

type LogEntry struct {
	//term sent out
	term  int 
	//could potentially be used for dropped requests
	index int 
	//for state replication
	command   string
}

func (le LogEntry) String() string{
	// return " " + strconv.Itoa(le.index)
	// return "Term " + strconv.Itoa(le.term) + " Ind " + strconv.Itoa(le.index) + " comm " + le.command
	return " " + le.command
}

//For RequestVote RPC
type RequestVoteArgs struct {
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

//For RequestVote RPC
type RequestVoteReply struct {
	term int
	voteGranted bool
	id int
}

//For AppendEntry RPC
type AppendEntriesArgs struct {
	term     int
	leaderId int

	prevLogIndex int
	prevLogTerm int
	entries []LogEntry
	leaderCommit int
}

//For AppendEntry RPC
type AppendEntriesReply struct {
	term int
	success bool
	id int
	// index < leader index, leader will can send starting from here (not implemented)
	// currently, value of -10 indicates heartbeat
	nextIndex int
}

//For client requests
type ClientRequest struct {
	index int
	command string
}

//When request is committed
type RequestCommitted struct {
	index int
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
func (r ClientRequest) messageType() string {
    return "CR" 
}
func (r RequestCommitted) messageType() string {
    return "RC"
}

//Main state of consensus
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

	logger *log.Logger
}

func makeRaft(m map[int]chan Message, i int, n int) *Raft{
	// for logging
	fileName := fmt.Sprintf(*log_path+"raft-%d.log", i)
    file, err := os.Create(fileName)
    if err != nil {
        log.Fatalf("Failed to create log file for Raft node %d: %v", i, err)
    }

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
		logger: log.New(file, "", log.LstdFlags),
	}

	newEntry := LogEntry{
		term: 0,
		index: 0,
		command: "INIT",
	}
	rf.log = append(rf.log, newEntry)

	return rf;
}

func (rf *Raft) start() {
	rf.currentTerm = 0
	rf.votedFor = -1
    rf.state = "Follower"

	for {
		// rf.logger.Printf("server %d state %s, term %d, log size %d, commit %d\n",rf.id,rf.state,rf.currentTerm, len(rf.log), rf.commitIndex)
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
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	err := os.Mkdir(*log_path, 0755)
    if err != nil {
        // fmt.Println("Error creating directory:", err)
        // return
    }

	//channels for each Raft server to communicate over
	messageCh := make(map[int]chan Message, *numServers)
	for i := 0; i<*numServers; i++ {
		messageCh[i] = make(chan Message)
	}

	//Raft states
	raftNodes := make([]Raft, *numServers)
	for i := range raftNodes {
		raftNodes[i] = *makeRaft(messageCh,i,*numServers)
	}

	//Each Raft server gets its own thread
	for i := range raftNodes {
		fmt.Printf("starting node %d\n",i)
		go raftNodes[i].start()
	}

	//For checking correctness of logs
	//Makes sure logs are replicated to each server
	correctLogs := make([]string,0)
	correctLogs = append(correctLogs,"INIT")
	for i := 1; i< *requests; i++{
		rd := strconv.Itoa(rand.Intn(100000))
		correctLogs = append(correctLogs,rd)
		for {
			if sendEntry(i, rd, raftNodes) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	//Waits a bit for all logs to replicate before ending program
	sleepLock.Lock()
	time.Sleep(time.Duration(*wait_time)* time.Second)
	if logCheck(raftNodes, correctLogs) {
		fmt.Println("All Logs Correct")
	}
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
	// rf.peers[args.candidateId] <- rep
	go rf.quicksend(rep, args.candidateId)

}

func (rf *Raft) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// make a reply
	//if lower term, vote no
	//if previously voted for or no votes, give vote
	//assuming that it is at least as updated
	reply.id = rf.id
	reply.voteGranted = false
	reply.term = rf.currentTerm
	// fmt.Printf("REQUEST VOTE server %d candidate %d currentvote %d lastterm %d/%d lastindex %d/%d\n", rf.id, args.candidateId, rf.votedFor, args.lastLogTerm, rf.lastLogTerm(), args.lastLogIndex,rf.lastLogIndex())
	
	if args.term < rf.currentTerm {
		// fmt.Printf("Candidate late term\n")
        return
    }

	//give vote
	if rf.votedFor == -1 || rf.votedFor == args.candidateId {
		if (args.lastLogTerm > rf.lastLogTerm() || (args.lastLogTerm == rf.lastLogTerm() && args.lastLogIndex >= rf.lastLogIndex())) {
			reply.term = rf.currentTerm
			reply.voteGranted = true
		}
	}

	return
}

func (rf *Raft) replyAppendEntries(message Message) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rep := AppendEntriesReply{
		term: -1,
		success:false,
		id: -1,
		nextIndex: -1,
	}
	args := message.(AppendEntriesArgs)
	rf.AppendEntries(&args,&rep)
	// rf.peers[args.leaderId] <- rep
	go rf.quicksend(rep, args.leaderId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	// make a reply
	reply.id = rf.id
	reply.term = rf.currentTerm
	reply.nextIndex = 0
	reply.success = false
	// fmt.Printf("AppendEntries %d from leader %d with size %d\n", rf.id, args.leaderId, len(args.entries))
	// fmt.Printf("lastInd %d/%d lastTerm %d/%d\n", rf.lastLogIndex(),args.prevLogIndex, rf.lastLogTerm(), args.prevLogTerm)

	if args.term < rf.currentTerm {
		return
	}
	//heartbeat
	if len(args.entries) == 0 {
		reply.success = true
		reply.nextIndex = -10
		// fmt.Printf("HEARTBEAT\n")
		return
	}

	if rf.lastLogIndex() < args.prevLogIndex {
		// reply.nextIndex = len(rf.log)
		// rf.printEntries()
		// fmt.Printf("BADLOGINDEX\n")
		return
	}

	if rf.log[args.prevLogIndex].term != args.prevLogTerm {
		for i := args.prevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].term != rf.log[args.prevLogIndex].term {
				// reply.nextIndex = i + 1
				rf.log = rf.log[:i]
				break
			}
		}
		// fmt.Printf("BADLOGTERM\n")
		return
	}

	i := args.prevLogIndex
    j := 0
    for ; i < len(rf.log) && j < len(args.entries); i++ {
        if rf.log[i].term != args.entries[j].term {
            break
        }
        j++
    }
    rf.log = rf.log[:i]
    rf.log = append(rf.log, args.entries[j:]...)

    if args.leaderCommit > rf.commitIndex {
        rf.commitIndex = min(args.leaderCommit, len(rf.log)-1)
    }

	// fmt.Printf("AppendEntries %d from leader %d good, now len %d\n", rf.id, args.leaderId, len(rf.log))
	// rf.printEntries()
	// reply.nextIndex = len(rf.log)
	reply.success = true
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1 
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
	timeout := time.Duration(*timeout+rand.Intn(*timeout_randomness)) * time.Millisecond
	timer := time.NewTimer(timeout)
	rf.logger.Printf("state %s, term %d, last index %d, commit %d\n",rf.state,rf.currentTerm, len(rf.log)-1, rf.commitIndex)
	num := rand.Float64()
	if num < *failure_rate {
		if sleepLock.TryLock() {
			rf.logger.Printf("state %s, sleeping for %d milliseconds\n",rf.state,*failure_duration)
			// fmt.Printf("RANDOM SLEEP FOLLOWER\n")
			time.Sleep(time.Duration(*failure_duration) * time.Millisecond)
			sleepLock.Unlock()
		}
		// will automatically convert back to follower
	}
	select {
	case message := <-rf.receiveCh:
		rf.logger.Printf("Received message of type %s", message.messageType())

		switch message.messageType() {
		case "AE":
			args := message.(AppendEntriesArgs)
			if args.term > rf.currentTerm { // if lower term, become follower
				rf.currentTerm = args.term
				rf.votedFor = -1
				rf.state = "Follower"
				rf.replyAppendEntries(message)
				return
			}
			rf.replyAppendEntries(message)
		case "RV":
			args := message.(RequestVoteArgs)
			if args.term > rf.currentTerm { // if lower term, become follower
				rf.currentTerm = args.term
				rf.votedFor = -1
				rf.state = "Follower"
				rf.replyRequestVote(message)
				return
			}
			rf.replyRequestVote(message)
		default:
		}
	case <- timer.C:
		rf.currentTerm++
		rf.state = "Candidate"
		return
	}
}

func (rf *Raft) candidate(){
	timeout := time.Duration(*timeout+rand.Intn(*timeout_randomness)) * time.Millisecond
	timer := time.NewTimer(timeout)

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
		// rf.peers[i] <- req
		go rf.quicksend(req, i)
    }

	for {
		num := rand.Float64()
		if num < *failure_rate {
			if sleepLock.TryLock() {
				rf.logger.Printf("state %s, sleeping for %d milliseconds\n",rf.state,*failure_duration)
				// fmt.Printf("RANDOM SLEEP CANDIDATE\n")
				time.Sleep(time.Duration(*failure_duration) * time.Millisecond)
				sleepLock.Unlock()
			}
			// will automatically convert to follower
		}
        select {
		case message := <-rf.receiveCh:
			rf.logger.Printf("Received message of type %s", message.messageType())

			switch message.messageType() {
			case "AE":
				args := message.(AppendEntriesArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
					rf.votedFor = -1
                    rf.state = "Follower"
					rf.replyAppendEntries(message)
					return
                }
				rf.replyAppendEntries(message)
			case "RV":
				args := message.(RequestVoteArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
					rf.votedFor = -1
                    rf.state = "Follower"
					rf.replyRequestVote(message)
					return
                }
				rf.replyRequestVote(message)
			case "RVR":
				args := message.(RequestVoteReply)
				// fmt.Printf("Vote Received %v\n", args.voteGranted)
				rf.logger.Printf("Vote Received with response %v\n", args.voteGranted)
				if args.term > rf.currentTerm {
                    rf.currentTerm = args.term
					rf.votedFor = -1
                    rf.state = "Follower"
                    return
                }
				totalVotes ++
                if args.voteGranted {
                    yesVotes++
                }
                if yesVotes > len(rf.peers)/2 {
					rf.votedFor = -1
                    rf.state = "Leader"
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.lastLogIndex() + 1
					}
					rf.matchIndex[rf.id] = rf.lastLogIndex()
					rf.nextIndex[rf.id] = rf.lastLogIndex() + 1
					rf.leaderId = rf.id

					rf.sendHeartbeat(true)
                    return
                }
			default:
			}
		case <- timer.C:
			rf.currentTerm++
			rf.state = "Candidate"
			return
		}
	}
}

func (rf *Raft) leader(){
	timeout := time.Duration(*heartbeat) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		rf.updatecommitindex()
		// fmt.Printf("leader %d state %s, term %d, log size %d, commit %d\n",rf.id,rf.state,rf.currentTerm, len(rf.log), rf.commitIndex)
		rf.logger.Printf("state %s, term %d, last index %d, commit %d\n",rf.state,rf.currentTerm, len(rf.log)-1, rf.commitIndex)
		// fmt.Println(rf.matchIndex,rf.nextIndex)
		num := rand.Float64()
		if num < *failure_rate {
			if sleepLock.TryLock() {
				// fmt.Printf("RANDOM SLEEP LEADER\n")
				rf.logger.Printf("state %s, sleeping for %d milliseconds\n",rf.state,*failure_duration)
				time.Sleep(time.Duration(*failure_duration) * time.Millisecond)
				sleepLock.Unlock()
			}
			// automatically converts to follower
		}
		select {
		case message := <-rf.receiveCh:
			rf.logger.Printf("Received message of type %s", message.messageType())

			switch message.messageType() {
			case "AE":
				args := message.(AppendEntriesArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
					rf.votedFor = -1
                    rf.state = "Follower"
					rf.replyAppendEntries(message)
					return
                }
				rf.replyAppendEntries(message)
			case "AER":
				args := message.(AppendEntriesReply)
				// rf.nextIndex[args.id] = args.nextIndex
				// rf.matchIndex[args.id] = args.nextIndex-1
				if args.success {
					if args.nextIndex != -10{
						rf.nextIndex[args.id]++
						rf.matchIndex[args.id] = max(0,rf.nextIndex[args.id]-1)
					}
					if(rf.nextIndex[args.id] > rf.lastLogIndex()){
                		rf.nextIndex[args.id] = rf.lastLogIndex()
						// rf.matchIndex[args.id] = max(0,rf.nextIndex[args.id]-1)
					}
					rf.updatecommitindex()
                }else {
                    rf.nextIndex[args.id] = max(0,rf.nextIndex[args.id]-1)
                }
			case "RV":
				args := message.(RequestVoteArgs)
				if args.term > rf.currentTerm { // if lower term, become follower
                    rf.currentTerm = args.term
					rf.votedFor = -1
                    rf.state = "Follower"
					rf.replyRequestVote(message)
					return
                }
				rf.replyRequestVote(message)
			case "CR":
				args := message.(ClientRequest)
				newEntry := LogEntry{
					term: rf.currentTerm,
					index: args.index,
					command: args.command,
				}
				rf.matchIndex[rf.id]++
				// rf.nextIndex[rf.id]++
				rf.nextIndex[rf.id] = min(rf.nextIndex[rf.id]+1, rf.matchIndex[rf.id])
				// globalInd++
				rf.log = append(rf.log,newEntry)
				rf.sendHeartbeat(false)
			default:
			}
		case <- timer.C:

			rf.sendHeartbeat(false)
			timer.Reset(timeout)
		}
	}
}

func (rf *Raft) sendHeartbeat(hb bool){
	// rf.printEntries()
	for i := range rf.peers {
		if i == rf.id {
			continue
		}
		prevInd := max(0, rf.nextIndex[i])
		prevInd = min(prevInd, len(rf.log)-1)
		var appends []LogEntry
		if(hb){
			appends = make([]LogEntry, 0)
		}else{
			appends = rf.log[prevInd:]
		}
		req := AppendEntriesArgs{
			term:         rf.currentTerm,
			leaderId:     rf.id,
			prevLogIndex: prevInd,
			prevLogTerm:  rf.log[prevInd].term,
			entries:      appends,			//make([]LogEntry, 0),  // empty for heartbeat rf.log[rf.nextIndex[i]:],
			leaderCommit: rf.commitIndex,
		}

		go rf.quicksend(req, i)
    }
}

func (rf *Raft) quicksend(req Message, i int){
	num := rand.Float64()
	if num > *drop_rate {
		rf.peers[i] <- req
		rf.logger.Printf("sent message of type %s\n",req.messageType())
	}else{
		rf.logger.Printf("failed to send message of type %s\n",req.messageType())
	}
}

func max(a int, b int) int{
	if a > b{
		return a
	}
	return b
}

func min(a int, b int) int{
	if a < b{
		return a
	}
	return b
}

// func (rf *Raft) printEntries(){
// 	mapLock.Lock()
// 	for i, entry := range rf.log{
// 		if rf.state == "Leader"{
// 			correctnessCheck[i] = entry.command
// 		}else{
// 			if entry.command != correctnessCheck[i] {
// 				if entry.term < rf.currentTerm{
// 					fmt.Printf("INCORRECT STATEMENT IN TERM %d\n", entry.term)
// 					// os.Exit(1)
// 				}
// 			}
// 		}
// 		// fmt.Printf("%s %s ind %d term %d ", rf.state,entry.command, entry.index, entry.term)
// 	} 
// 	mapLock.Unlock()
// 	fmt.Printf("\n")
// }

func sendEntry(ind int, comm string, nodes []Raft) bool {
	if !sleepLock.TryLock() {
		return false
	}
	defer sleepLock.Unlock()
	req := ClientRequest{
		index: ind,
		command: comm,
	}

	for i := 0; i < len(nodes); i++{
		if nodes[i].state == "Leader"{
			nodes[i].receiveCh<-req
			return true
		}else{

		}
	} 
	return false
}

//checks logs of each node, making sure it is in accordance to the client's requests
//it is possible for some requests to be skipped on all servers

//if any request doesnt match with a server, we will return an error
//if any server has different logs compared to other servers, we will return error
func logCheck(nodes []Raft, clientlog []string) bool{
	check := 0

	for i := 0; i < len(nodes); i++{
		missed := 0
		fmt.Printf("server %d: %s\n",i,nodes[i].log)
		for j, entry := range nodes[i].log{
			if j == 0{
				continue
			}
			if clientlog[j+missed] != entry.command {
				// fmt.Printf("Node %d dropped %s at index %d\n",i, clientlog[j+missed], j+missed)
				nodes[i].logger.Printf("Node %d dropped %s at index %d\n",i, clientlog[j+missed], j+missed)
				missed += 1
			}
		}
		if check != 0 && check != missed {
			fmt.Printf("Logs different\n")
			return false
		}

		if missed > 0{
			check = missed
		}
	}
	fmt.Printf("Dropped %d total client requests\n", check)
	return true
}