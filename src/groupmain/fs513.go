package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/bramvdbogaerde/go-scp"
	"github.com/bramvdbogaerde/go-scp/auth"
	"golang.org/x/crypto/ssh"
	"net"
	"os"
	"os/exec"
	"time"
)

const (
	COM_FS513_PATH     = "/home/ec2-user/fs513_files/"
	IDENTITY_FILE_PATH = "/home/ec2-user/id_file/chet0804.pem.txt"
)

var fs513_list = make(map[string][]string)

var local_files = make([]string, 0)

func addFileToFS(local_path string, fs513_name string) {

	//absPath, _ := filepath.Abs(FS513_PATH)
	/*if _, err := os.Stat(absPath); os.IsNotExist(err){
		os.MkdirAll(absPath, os.ModePerm)
	}*/
	if _, ok := fs513_list[fs513_name]; ok {
		fmt.Println("File " + fs513_name + " exists in FS513 system")
		// Do you want to update?
		return
	}

	fs513Path := COM_FS513_PATH + fs513_name
	cmdOut, err := exec.Command("cp", local_path, fs513Path).CombinedOutput()
	//errorCheck(err)
	if err != nil {
		fmt.Println("Error while copying ", err)
		fmt.Println(cmdOut)
		return
	}
	
	replicateFile(fs513Path)

	if currHost != GATEWAY {
		sendUpdGateway(fs513_name, "AddFile")
	} else {
		// Update fs513 List for Gateway 
		ip_dest1 := membershipGroup[(getIx()+1)%len(membershipGroup)].Host
		ip_dest2 := membershipGroup[(getIx()+2)%len(membershipGroup)].Host

		file_ips := make([]string, 0)
		file_ips = append(file_ips, currHost)
		file_ips = append(file_ips, ip_dest1)
		file_ips = append(file_ips, ip_dest2)

		fs513_list[fs513_name] = file_ips
		// Broadcast update to all nodes
		broadcastFileList()
	} 
	local_files = append(local_files, fs513_name)
	infolog.Println("file " + fs513_name + " added to " + currHost)
}

func deleteFileFromFS(fs513_name string){
	
	if _, ok := fs513_list[fs513_name]; !ok {
		fmt.Println("File " + fs513_name + " does not exists in FS513 system")
		return
	}
	// Send Delete msg to Gateway
	if currHost != GATEWAY {
		sendUpdGateway(fs513_name, "DelFile")
	} else {
		removeFileFromFS(fs513_name)
	}
}

func removeFileFromFS(fs513_name string){
	// Check if present locally
	var present bool = false
	for _,v := range local_files{
		if v == fs513_name {
			present = true
		}
	} 
	if !present {
		fmt.Println("File " + fs513_name + " does not exists in locally in " + currHost)
		return
	}
	
	// Remove file from directory
	cmdArgs := make([]string, 0)
	cmdArgs = append(cmdArgs, "-f")
	cmdArgs = append(cmdArgs, COM_FS513_PATH + fs513_name)
	fmt.Println("rm file string: ", cmdArgs)
	cmdOut, err := exec.Command("rm", cmdArgs...).CombinedOutput()
	
	if err != nil {
		fmt.Println("ERROR WHILE READING")
		fmt.Println(err)
		fmt.Println(string(cmdOut))
	}
	// Remove file local array	
	for index, element := range local_files {
		if element == fs513_name {
			local_files = append(local_files[:index], local_files[index+1:]...)
			break
		}
	}
	infolog.Println("file " + fs513_name + " removed from " + currHost)

}

func sendUpdGateway(fs513_name string, status string) {
	msg := message{currHost, status, time.Now().Format(time.RFC850), fs513_name}
	var targetHosts = make([]string, 1)
	targetHosts[0] = GATEWAY

	sendToHosts(msg, targetHosts)
}

func replicateFile(path string) {
	ipDest1 := membershipGroup[(getIx()+1)%len(membershipGroup)].Host
	ipDest2 := membershipGroup[(getIx()+2)%len(membershipGroup)].Host

	scpFile(path, ipDest1)
	scpFile(path, ipDest2)
}

func scpFile(fs513Path string, ip_dest string) {
	// scp -i chet0804.pem.txt SAATHE ec2-user@ip-172-31-29-21:/home/ec2-user/

	// Use SSH key authentication from the auth package
	// we ignore the host key in this example, please change this if you use this library
	clientConfig, _ := auth.PrivateKey("ec2-user", IDENTITY_FILE_PATH, ssh.InsecureIgnoreHostKey())

	// For other authentication methods see ssh.ClientConfig and ssh.AuthMethod

	// Create a new SCP client
	client := scp.NewClient(ip_dest+":22", &clientConfig)

	// Connect to the remote server
	err := client.Connect()
	if err != nil {
		fmt.Println("Couldn't establish a connection to the remote server ", err)
		return
	}

	// Open a file
	srcpath, _ := os.Open(fs513Path)
	// Create remote path
	remotePath := fs513Path
	// Close session after the file has been copied
	defer client.Session.Close()

	// Close the file after it has been copied
	defer srcpath.Close()

	// Finaly, copy the file over
	// Usage: CopyFile(fileReader, remotePath, permission)
	client.CopyFile(srcpath, remotePath, "0655")
}

/*
 * Listen to fs513 file list updates send from Gateway node.
 */
func listenToGatewayFL() {
	addr, err := net.ResolveUDPAddr(UDP, FL_GT_PORT)
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
		rcvd_list := make(map[string][]string)
		n, _, err := conn.ReadFromUDP(buf)
		err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&rcvd_list)
		if err != nil {
			fmt.Println("listen gateway:Not able to resolve udp")
			errlog.Println(err)
		}

		mutex.Lock()
		fs513_list = rcvd_list
		mutex.Unlock()

		infolog.Println("File List Received: ", fs513_list)
	}
}
