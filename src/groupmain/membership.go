package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"grepserver"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"utils"
)

const (
	GATEWAY        = "172.31.23.202" //Designated Gateway for the nodes to join
	MIN_GROUP_SIZE = 4
	ACK_TIMEOUT    = time.Millisecond * 2500
	SYN_TIMEOUT    = time.Second * 1
	MSG_PORT       = ":50000" // Port for listening to messages
	MG_GT_PORT     = ":50001" // Gateway port to listen to membership list
	FL_GT_PORT     = ":50002" // Gateway port to listen to file list
	LCL_PORT       = ":0"     // Dummy local port
	UDP            = "udp"    // UDP protocol identifier
	PACKET_LOSS    = 0        // Packet loss simulation constant between 0-100
)

// Message structure
type message struct {
	Host          string
	Status        string
	TimeStamp     string
	FS513Name 	  string
}

// Member structure
type member struct {
	Host      string
	TimeStamp string
}

var (
	currHost        string
	partofGroup     int
	mutex           = &sync.Mutex{}
	timers          [3]*time.Timer // Timer arrays for checking ACK timeouts
	resetTimerFlags [3]int
	membershipGroup = make([]member, 0) // Array holds the membership list
	packet_loss_cnt int
)

//For logging
var (
	logfile  *os.File
	errlog   *log.Logger
	infolog  *log.Logger
	emptylog *log.Logger
)

/*
 * Main function entry point
 */
func main() {

	go listenToMessages()
	go listenToGatewayMG()
	go listenToGatewayFL()
	go sendSyn()
	go checkAck(1)
	go checkAck(2)
	go checkAck(3)
	go grepserver.StartGrepServer()

	takeUserInput()
}

/*
 * Initialize all variables
 */
func init() {

	currHost = utils.GetLocalIP()
	initMG()

	timers[0] = time.NewTimer(ACK_TIMEOUT)
	timers[1] = time.NewTimer(ACK_TIMEOUT)
	timers[2] = time.NewTimer(ACK_TIMEOUT)
	timers[0].Stop()
	timers[1].Stop()
	timers[2].Stop()

	absPath, _ := filepath.Abs(utils.LOG_FILE_GREP)
	logfile_exists := 1
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		logfile_exists = 0
		os.Mkdir("src/logs", os.ModePerm)
	}

	logfile, _ := os.OpenFile(absPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	errlog = log.New(logfile, "ERROR: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	infolog = log.New(logfile, "INFO: ", log.Ldate|log.Lmicroseconds)
	emptylog = log.New(logfile, "\n----------------------------------------------------------------------------------------\n", log.Ldate|log.Ltime)

	if logfile_exists == 1 {
		emptylog.Println("")
	}

}

/*
 * Take input from user from stdin and executes corresponding function
 */
func takeUserInput() {

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("1  - Print membership list")
		fmt.Println("2  - Print self ID")
		fmt.Println("3  - Join group")
		fmt.Println("4  - Leave group")
		fmt.Println("5  - Grep node logs")
		fmt.Println("********************* FS513 Options *****************************")
		fmt.Println("6  - put [localfilename] [fs513filename]")
		fmt.Println("7  - get [fs513filename]")
		fmt.Println("8  - remove [fs513filename]")
		fmt.Println("9  - locate [fs513filename]")
		fmt.Println("10 - list all fs513 files")
		fmt.Println("11 - list all local files")
		fmt.Println("Enter option: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSuffix(input, "\n")
		switch input {
		case "1":
			for _, element := range membershipGroup {
				fmt.Println(element)
			}
		case "2":
			fmt.Println(currHost)
		case "3":
			if currHost != GATEWAY && partofGroup == 0 {
				fmt.Println("Joining group")
				gatewayConnect()
				partofGroup = 1
			} else {
				fmt.Println("I am Master or I am already connected")
			}
		case "4":
			if partofGroup == 1 {
				fmt.Println("Leaving group TS - " + time.Now().Format(time.StampMicro))
				exitGroup()
				os.Exit(0)
			} else {
				fmt.Println("You are currently not connected to a group or You are master")
			}
		case "5":
			grepClient(reader)
		case "6":
			fmt.Println("Local path?")
			local_path, _ := reader.ReadString('\n')
			local_path = strings.TrimRight(local_path, "\n")
			fmt.Println("FS513 name?")
			fs513_name, _ := reader.ReadString('\n')
			fs513_name = strings.TrimRight(fs513_name, "\n")
			fmt.Println("Add file Start..", time.Now().Format(time.StampMicro))
			addFileToFS(local_path, fs513_name)
		case "7":
			fmt.Println("FS513 name?")
			fs513_name, _ := reader.ReadString('\n')
			fs513_name = strings.TrimRight(fs513_name, "\n")
			fmt.Println("GetFile Start..", time.Now().Format(time.StampMicro))
			getFileFromDest(fs513_name)
		case "8":
		    fmt.Println("FS513 name?")
			fs513_name, _ := reader.ReadString('\n')
			fs513_name = strings.TrimRight(fs513_name, "\n")
			fmt.Println("Remove File..", time.Now().Format(time.StampMicro))
			deleteFileFromFS(fs513_name)
		case "9":
			fmt.Println("FS513 name?")
			fs513_name, _ := reader.ReadString('\n')
			fs513_name = strings.TrimRight(fs513_name, "\n")
			fmt.Println("Locate: " + fs513_name + " IPs: "  , fs513_list[fs513_name])
		case "10":
			//list all fs513 files
			for k,v := range fs513_list {
				fmt.Println("FS513 File:" + k + " IPs:", v)
			} 
		case "11":
			getLocalFiles();
		default:
			fmt.Println("Invalid command")
		}
		fmt.Println("\n\n")
	}
}

/*
 * Run grep on the servers currently in the membership list
 */
func grepClient(reader *bufio.Reader) {

	fmt.Println("Usage: -options keywordToSearch")
	fmt.Println("-options: available in linux grep command")
	fmt.Println("Enter: ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSuffix(input, "\n")
	serverInput := strings.Split(input, " ")
	// Send data to every server in membershipList
	membersToGrep := make([]string, 0)
	for _, element := range membershipGroup {
		membersToGrep = append(membersToGrep, element.Host+":"+grepserver.PORT)
	}
	tStart := time.Now()
	utils.SendToServer(membersToGrep, serverInput)
	tEnd := time.Now()
	fmt.Println("Grep results took ", tEnd.Sub(tStart))
}

/*
 * Listen to messages on UDP port from other nodes and take appropriate action. Possible message types are
 * Join, SYN, ACK, Failed and Leave
 */
func listenToMessages() {
	addr, err := net.ResolveUDPAddr(UDP, MSG_PORT)
	if err != nil {
		fmt.Println("listenmessages:Not able to resolve udp")
		errlog.Println(err)
	}
	conn, err := net.ListenUDP(UDP, addr)
	if err != nil {
		fmt.Println("listenmessages:Not able to resolve listen to UDP")
		errlog.Println(err)
	}
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		pkt := message{}
		n, _, err := conn.ReadFromUDP(buf)
		err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&pkt)
		if err != nil {
			fmt.Println("listenmessages:Not able to read from Conn")
			errlog.Println(err)
		}
		go processMsg(pkt)
	}
}

func processMsg(pkt message){
		switch pkt.Status {
		case "Join":   // Received only by Gateway
			node := member{pkt.Host, time.Now().Format(time.RFC850)}
			if checkTimeStamp(node) == 0 {
				mutex.Lock()
				resetCorrespondingTimers()
				membershipGroup = append(membershipGroup, node)
				mutex.Unlock()
			}
			broadcastGroup(node)
		case "SYN":
			respondAck(pkt.Host)
		case "ACK":
			if pkt.Host == membershipGroup[(getIx()+1)%len(membershipGroup)].Host {
				timers[0].Reset(ACK_TIMEOUT)
			} else if pkt.Host == membershipGroup[(getIx()+2)%len(membershipGroup)].Host {
				timers[1].Reset(ACK_TIMEOUT)
			} else if pkt.Host == membershipGroup[(getIx()+3)%len(membershipGroup)].Host {
				timers[2].Reset(ACK_TIMEOUT)
			}
			//infolog.Println("ACK response  " + time.Now().Format(time.StampMicro))
		case "Failed", "Leave":
			infolog.Println("Received [" + pkt.Status + "] Msg from " + pkt.Host + " TS - " + time.Now().Format(time.StampMicro))
			mutex.Lock()
			resetCorrespondingTimers()
			if forwardMsg(pkt) == 0 && currHost == GATEWAY {
				go updateFileList(pkt.Host)
			}
			mutex.Unlock()
		case "AddFile":  // Received only by Gateway
			// append to fs513 list
			ip_dest1 := membershipGroup[(getIdxOfHost(pkt.Host)+1)%len(membershipGroup)].Host
			ip_dest2 := membershipGroup[(getIdxOfHost(pkt.Host)+2)%len(membershipGroup)].Host

			file_ips := make([]string, 0)
			file_ips = append(file_ips, pkt.Host)
			file_ips = append(file_ips, ip_dest1)
			file_ips = append(file_ips, ip_dest2)
			fmt.Println("Pkt: " , pkt)
			//info := file_info{pkt.fs513FileName, file_ips}
			fs513_list[pkt.FS513Name] = file_ips
			// Broadcast update to all nodes
			broadcastFileList()
			fmt.Println("Add file " + pkt.FS513Name + " End..", time.Now().Format(time.StampMicro))
		case "DelFile":   // Received only by Gateway
			// Get IPs for the fs513 file name
			ips := fs513_list[pkt.FS513Name]		
			// Send remove file msg to the ips
			msg := message{currHost, "rmfile", time.Now().Format(time.RFC850), pkt.FS513Name}
			sendToHosts(msg, ips)
			// delete from file list and broadcast filelist
			delete(fs513_list, pkt.FS513Name)
			broadcastFileList()
		case "rmfile":   // Received by node where file is located
			removeFileFromFS(pkt.FS513Name)
			fmt.Println("File " + pkt.FS513Name + " Removed..", time.Now().Format(time.StampMicro))
		case "replicateFile":
			// Get Path for file and do SCP
			scpFile(COM_FS513_PATH + pkt.FS513Name, pkt.Host)
			fmt.Println("ReplicateFile " + pkt.FS513Name +" End..", time.Now().Format(time.StampMicro))
		}
}
/*
 * Listen to membership list updates send from Gateway node.
 */
func listenToGatewayMG() {
	addr, err := net.ResolveUDPAddr(UDP, MG_GT_PORT)
	if err != nil {
		fmt.Println("listen gateway:Not able to resolve udp")
		errlog.Println(err)
	}

	conn, err := net.ListenUDP(UDP, addr)
	if err != nil {
		fmt.Println("listen gateway:Not able to resolve udp")
		errlog.Println(err)
	}
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		list := make([]member, 0)
		n, _, err := conn.ReadFromUDP(buf)
		err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&list)
		if err != nil {
			fmt.Println("listen gateway:Not able to resolve udp")
			errlog.Println(err)
		}

		mutex.Lock()
		resetCorrespondingTimers()
		if len(list) == 1 {
			membershipGroup = append(membershipGroup, list[0])
		} else {
			membershipGroup = list
		}
		mutex.Unlock()

		var N = len(list) - 1
		infolog.Println("New VM joined the group: (" + list[N].Host + " | " + list[N].TimeStamp + ")")
	}
}

/*
 * This function would take care of timeout events of the neighbouring nodes. SYN and ACK messaging would start only when there are
 * Minimum of 4 nodes are present in the group.If there is a timeout detected in a neighbour, then all the other timers are also reset in order
 * to take care of seriliazation of the EVENTS happening at  node.
 * Events could be 1.Leave message arriving at the node 2.Join broadcast arriving from GATEWAY 3.Simulataneos timeouts or individual
 * timeouts happening in any of the next three successor neightbours. The neighbour to check is based current host index i.
 * (i+1)%N, (i+2)%N, (i+3)%N. N=Total number of nodesin the memeber. This method is called for relativeindex 1,2 and 3
 */
func checkAck(relativeIx int) {

	for len(membershipGroup) < MIN_GROUP_SIZE {
		time.Sleep(100 * time.Millisecond)
	}

	host := membershipGroup[(getIx()+relativeIx)%len(membershipGroup)].Host

	timers[relativeIx-1] = time.NewTimer(ACK_TIMEOUT)
	<-timers[relativeIx-1].C

	mutex.Lock()
	if len(membershipGroup) >= MIN_GROUP_SIZE && getRelativeIx(host) == relativeIx && resetTimerFlags[relativeIx-1] != 1 {
		msg := message{membershipGroup[(getIx()+relativeIx)%len(membershipGroup)].Host, "Failed", time.Now().Format(time.RFC850), ""}
		infolog.Println("Failure detected at host: " + msg.Host)
		forwardMsg(msg)
	}
	// None of of the Events should be updating the MembershipList , only then this condition would be set.
	// Reset all the other timers (which the current node is monitoring) as well if the above condition is met
	if resetTimerFlags[relativeIx-1] == 0 {
		infolog.Print("Force stopping other timers " + string(relativeIx))
		for i := 1; i < 3; i++ {
			resetTimerFlags[i] = 1
			timers[i].Reset(0)
		}
	} else {
		resetTimerFlags[relativeIx-1] = 0
	}

	mutex.Unlock()
	go checkAck(relativeIx)

}

/*
 * Initailize the ML with current host
 */
func initMG() {
	node := member{currHost, time.Now().Format(time.RFC850)}
	membershipGroup = append(membershipGroup, node)
}


/*
 * The function which removes the node from the Membershiplist and updates the list.
 * Go library gives the flexiblity of moving the elements in the static array very elegantly by append and Array slice operators
 */
func updateMG(Ix int, msg message) {
	localTime, _ := time.Parse(time.RFC850, membershipGroup[Ix].TimeStamp)
	givenTime, _ := time.Parse(time.RFC850, msg.TimeStamp)

	if givenTime.After(localTime) {
		membershipGroup = append(membershipGroup[:Ix], membershipGroup[Ix+1:]...)
		ts := time.Now().Format(time.StampMicro)
		fmt.Println("Processed ["+msg.Status+"] Msg from "+msg.Host+" TS - ", ts)
		infolog.Println("Processed ["+msg.Status+"] Msg from "+msg.Host+" TS - ", ts)
	} else {
		fmt.Println("Timestamp of msg [" + msg.TimeStamp + "] older than my record [" + membershipGroup[Ix].TimeStamp + "]")
		infolog.Println("Timestamp of msg [" + msg.TimeStamp + "] older than my record [" + membershipGroup[Ix].TimeStamp + "]")
	}
}

func resetCorrespondingTimers() {
	resetTimerFlags[0] = 1
	resetTimerFlags[1] = 1
	resetTimerFlags[2] = 1
	timers[0].Reset(0)
	timers[1].Reset(0)
	timers[2].Reset(0)
}

/*
 * Get index of current host
 */
func getIx() int {
	for i, element := range membershipGroup {
		if currHost == element.Host {
			return i
		}
	}
	return -1
}

func getIdxOfHost(host string) int {
	for i, element := range membershipGroup {
		if host == element.Host {
			return i
		}
	}
	return -1
}

/*
 * Function to give the relative location of the host with respect to the current node in the ML
 */
func getRelativeIx(host string) int {
	localIx := getIx()
	if strings.Compare(membershipGroup[(localIx+1)%len(membershipGroup)].Host, host) == 0 {
		return 1
	} else if strings.Compare(membershipGroup[(localIx+2)%len(membershipGroup)].Host, host) == 0 {
		return 2
	} else if strings.Compare(membershipGroup[(localIx+3)%len(membershipGroup)].Host, host) == 0 {
		return 3
	}
	return -1
}

/*
 * This function sends SYN messages to next three successive neighbours every SYN_TIMEOUT
 */
func sendSyn() {
	for {
		num := len(membershipGroup)
		if num >= MIN_GROUP_SIZE {
			msg := message{currHost, "SYN", time.Now().Format(time.RFC850), ""}
			var targetConnections = make([]string, 3)
			targetConnections[0] = membershipGroup[(getIx()+1)%len(membershipGroup)].Host
			targetConnections[1] = membershipGroup[(getIx()+2)%len(membershipGroup)].Host
			targetConnections[2] = membershipGroup[(getIx()+3)%len(membershipGroup)].Host
			sendToHosts(msg, targetConnections)
			//infolog.Println("SYN messages send: " + time.Now().Format(time.RFC850))
		}
		time.Sleep(SYN_TIMEOUT)
	}
}

/*
 * This function sends back the ACK to the host which sent SYN to it.
 */
func respondAck(host string) {
	msg := message{currHost, "ACK", time.Now().Format(time.RFC850), ""}
	var targetConnections = make([]string, 1)
	targetConnections[0] = host

	sendToHosts(msg, targetConnections)

}

/*
 * This function sends Join request to Gateway node
 */
func gatewayConnect() {
	msg := message{currHost, "Join", time.Now().Format(time.RFC850), ""}
	var targetConnections = make([]string, 1)
	targetConnections[0] = GATEWAY

	sendToHosts(msg, targetConnections)
}

/*
 * This function is for any node which wants to leave the group. Message is formed and sent to three predecessors
 */
func exitGroup() {
	msg := message{currHost, "Leave", time.Now().Format(time.RFC850), ""}

	var targetConnections = make([]string, 3)
	for i := 1; i < 4; i++ {
		var targetHostIndex = (getIx() - i) % len(membershipGroup)
		if targetHostIndex < 0 {
			targetHostIndex = len(membershipGroup) + targetHostIndex
		}
		targetConnections[i-1] = membershipGroup[targetHostIndex].Host
	}

	sendToHosts(msg, targetConnections)
}

/*
 * This function is to update the membershiplist by removing the left/failed host and then propogate
 * the message to next three successive neighbours.If the the membershiplist is already updated then stop the propagation.
 */
func forwardMsg(msg message) int {
	var hostIx = -1
	for i, element := range membershipGroup {
		if msg.Host == element.Host {
			hostIx = i
			break
		}
	}
	if hostIx == -1 {
		return 1
	}

	updateMG(hostIx, msg)

	var targetConnections = make([]string, 3)
	targetConnections[0] = membershipGroup[(getIx()+1)%len(membershipGroup)].Host
	targetConnections[1] = membershipGroup[(getIx()+2)%len(membershipGroup)].Host
	targetConnections[2] = membershipGroup[(getIx()+3)%len(membershipGroup)].Host

	sendToHosts(msg, targetConnections)
	
	return 0
}

/*
 * This function is used by the GATEWAY to send an updated membershiplist after appending the new joinee in to the list.
 * Port Number used is 5001
 */
func broadcastGroup(node member) {
	var compbuf bytes.Buffer
	var nodebuf bytes.Buffer

	memberToAdd := make([]member, 0)
	memberToAdd = append(memberToAdd, node)

	if err := gob.NewEncoder(&nodebuf).Encode(memberToAdd); err != nil {
		fmt.Println("BroadcastGroup: not able to encode new node")
		errlog.Println(err)
	}

	if err := gob.NewEncoder(&compbuf).Encode(membershipGroup); err != nil {
		fmt.Println("BroadcastGroup: not able to encode")
		errlog.Println(err)
	}

	for ix, element := range membershipGroup {
		if element.Host != currHost {

			serverAddr, err := net.ResolveUDPAddr(UDP, membershipGroup[ix].Host+MG_GT_PORT)
			if err != nil {
				fmt.Println("BroadcastGroup: not able to Resolve server address")
				errlog.Println(err)
			}

			localAddr, err := net.ResolveUDPAddr(UDP, currHost+LCL_PORT)
			if err != nil {
				fmt.Println("BroadcastGroup: not able to Resolve local address")
				errlog.Println(err)
			}

			conn, err := net.DialUDP(UDP, localAddr, serverAddr)
			if err != nil {
				fmt.Println("BroadcastGroup: not able to dial")
				errlog.Println(err)
			}

			if element.Host == node.Host {
				_, err = conn.Write(compbuf.Bytes())
			} else {
				_, err = conn.Write(nodebuf.Bytes())
			}
			if err != nil {
				fmt.Println("BroadcastGroup: not able to write to connection")
				errlog.Println(err)
			}

		}
	}
}

/*
 * Send given message to the target nodes
 */
func sendToHosts(msg message, targetConnections []string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		fmt.Println("sendToHosts:problem during encoding")
		errlog.Println(err)
	}

	localAddr, err := net.ResolveUDPAddr(UDP, currHost+LCL_PORT)
	if err != nil {
		fmt.Println("sendToHosts:problem while resolving localip")
		errlog.Println(err)
	}

	for _, targetHost := range targetConnections {
		if msg.Status == "Leave" || msg.Status == "Failed" {
			infolog.Print("Propagating ")
			infolog.Print(msg)
			infolog.Print(" to :")
			infolog.Println(targetHost)
		}

		remoteAddr, err := net.ResolveUDPAddr(UDP, targetHost+MSG_PORT)

		if err != nil {
			fmt.Println("sendToHosts:problem while resolving serverip")
			errlog.Println(err)
		}
		conn, err := net.DialUDP(UDP, localAddr, remoteAddr)

		if err != nil {
			fmt.Println("sendToHosts:problem while dial")
			errlog.Println(err)
		}
		randNum := rand.Intn(100)
		if !((msg.Status == "SYN" || msg.Status == "ACK" || msg.Status == "Leave" || msg.Status == "Failed") && randNum < PACKET_LOSS) {
			_, err = conn.Write(buf.Bytes())
			if err != nil {
				fmt.Println("sendToHosts:problem while writing to connection")
				errlog.Println(err)
			}
		} else {
			packet_loss_cnt++
			fmt.Println("Packet Loss: " + string(packet_loss_cnt))
		}
	}
}

/*
 * Check timestamp for incomming and existing member. If incomming is newer then return 1 else 0
 */
func checkTimeStamp(m member) int {
	for _, element := range membershipGroup {
		if m.Host == element.Host {
			t1, _ := time.Parse(time.RFC850, m.TimeStamp)
			t2, _ := time.Parse(time.RFC850, element.TimeStamp)
			if t2.After(t1) {
				element = m
				return 1
			} else {
				break
			}
		}
	}
	return 0
}
