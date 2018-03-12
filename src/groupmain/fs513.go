package main

import (
	"fmt"
	"os/exec"
	"os"
	"time"
	
	"github.com/bramvdbogaerde/go-scp/auth"
	"github.com/bramvdbogaerde/go-scp"
    "golang.org/x/crypto/ssh"	
)

const (
	FS513_PATH = "/home/ec2-user/fs513_files/"
	IDENTITY_FILE_PATH = "/home/ec2-user/DistributedFileSystem-FS513/chet0804.pem.txt"
)

type file_info struct {
	Name  string
	IPs   []string
}

var fs513_list = make(map[string]file_info)

var local_files = make([]string, 0)

func addFileToFS(local_path string, sdfs_name string) {
		
	//absPath, _ := filepath.Abs(FS513_PATH)
	/*if _, err := os.Stat(absPath); os.IsNotExist(err){
		os.MkdirAll(absPath, os.ModePerm)
	}*/
	sdfsPath :=  FS513_PATH + sdfs_name
	cmdOut, err := exec.Command("cp", local_path, sdfsPath).CombinedOutput()
	//errorCheck(err)
	if err != nil {
		fmt.Println("Error while copying ", err)
		fmt.Println(cmdOut)
		return
	}
	if currHost != GATEWAY {
		replicateFile(sdfsPath)
		sendUpdGateway(sdfs_name)
	} /*else {
		//TODO What if file exists?
		ip_dest1 := membershipList[(getIndex(currHost)+1)%len(membershipList)].Host
		ip_dest2 := membershipList[(getIndex(currHost)+2)%len(membershipList)].Host
		ip_dest3 := membershipList[(getIndex(currHost)+3)%len(membershipList)].Host
		go sendFile(currHost, ip_dest1, sdfs_name)
		go sendFile(currHost, ip_dest2, sdfs_name)
		go sendFile(currHost, ip_dest3, sdfs_name)

		file_ips := make([]string, 0)
		file_ips = append(file_ips, currHost)
		file_ips = append(file_ips, ip_dest1)
		file_ips = append(file_ips, ip_dest2)
		file_ips = append(file_ips, ip_dest3)
		file_list[sdfs_name] = file_info{sdfs_name, file_ips, getFileSize(local_path)}
		sendFileMD()

		message := message{currHost, "FileSent", time.Now().Format(time.RFC850), file_list[sdfs_name]}
		var targetHosts = make([]string, 3)
		targetHosts[0] = ip_dest1
		targetHosts[1] = ip_dest2
		targetHosts[2] = ip_dest3

		sendMsg(message, targetHosts)
	} */
	local_files = append(local_files, sdfs_name)
	//infoCheck("file " + sdfs_name + " added to "+ currHost)
}

func sendUpdGateway(sdfs_name string) {
	msg := message{currHost, "AddFile", time.Now().Format(time.RFC850), sdfs_name}
	var targetHosts = make([]string, 1)
	targetHosts[0] = GATEWAY

	sendToHosts(msg, targetHosts)
}

func replicateFile(path string){
	ipDest1 := membershipGroup[(getIx()+1)%len(membershipGroup)].Host
	ipDest2 := membershipGroup[(getIx()+2)%len(membershipGroup)].Host
	
	scpFile(path,ipDest1)
	scpFile(path,ipDest2)
}

func scpFile(sdfsPath string, ip_dest string) {
	// scp -i chet0804.pem.txt SAATHE ec2-user@ip-172-31-29-21:/home/ec2-user/
	/*cmdArgs := []string{}
	cmdArgs = append(cmdArgs, "-i " + IDENTITY_FILE_PATH)
	cmdArgs = append(cmdArgs, sdfsPath)
	cmdArgs = append(cmdArgs, "ec2-user@"+ip_dest+":/home/ec2-user/fs513_files/")	
	
	fmt.Println("cmdArgs: ", cmdArgs)
	cmdOut, err := exec.Command("scp", cmdArgs...).CombinedOutput()
	if err !=nil{
		fmt.Println("Error ", err)
		fmt.Println("Cmdout " + string(cmdOut))
	}*/
	
	// Use SSH key authentication from the auth package
    // we ignore the host key in this example, please change this if you use this library
	clientConfig, _ := auth.PrivateKey("ec2-user", IDENTITY_FILE_PATH , ssh.InsecureIgnoreHostKey())
	
	// For other authentication methods see ssh.ClientConfig and ssh.AuthMethod

	// Create a new SCP client
	client := scp.NewClient(ip_dest+":22", &clientConfig)
	
	// Connect to the remote server
	err := client.Connect()
	if err != nil{
		fmt.Println("Couldn't establish a connection to the remote server ", err)
		return		
	}

	// Open a file
	f, _ := os.Open(sdfsPath)

	// Close session after the file has been copied
	defer client.Session.Close()
	
	// Close the file after it has been copied
	defer f.Close()
	
	// Finaly, copy the file over
	// Usage: CopyFile(fileReader, remotePath, permission)

	client.CopyFile(f, FS513_PATH, "0655")
}
