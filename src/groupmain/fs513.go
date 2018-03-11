package main

import (
	"os"
	"path/filepath"
	"fmt"
	"os/exec"
	"time"
)

const (
	FS513_PATH = "/home/ec2-user/fs513_files/"
)

type file_info struct {
	Name string
	IP   string
}

var fs513_list = make(map[string]file_info)

var local_files = make([]string, 0)

func addFileToFS(local_path string, sdfs_name string) {
		
	absPath, _ := filepath.Abs(FS513_PATH)
	if _, err := os.Stat(absPath); os.IsNotExist(err){
		os.MkdirAll(absPath, os.ModePerm)
	}
	cmdOut, err := exec.Command("cp", local_path, absPath + sdfs_name).CombinedOutput()
	//errorCheck(err)
	if err != nil {
		fmt.Println("Error while copying ", err)
		fmt.Println(cmdOut)
		return
	}
	if currHost != GATEWAY {
		sendAddFile(sdfs_name)
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

/*Helper function for sending an 'addFile' message*/
func sendAddFile(sdfs_name string) {
	msg := message{currHost, "AddFile", time.Now().Format(time.RFC850), sdfs_name}
	var targetHosts = make([]string, 1)
	targetHosts[0] = GATEWAY

	sendToHosts(msg, targetHosts)
}

func scpFile(ip_src string, ip_dest string, sdfs_name string) {
	//infoCheck("scp" + " ddle2@" + IP_src.String() + ":/home/ddle2/CS425-MP3/files/" + sdfs_name + " ddle2@" + IP_dest.String() + ":/home/ddle2/CS425-MP3/files")
	cmdArgs := []string{}
	cmdArgs = append(cmdArgs,"ec2-user@"+ip_src+":/home/ec2-user/fs513_files/"+sdfs_name)
	cmdArgs = append(cmdArgs,"ec2-user@"+ip_dest+":/home/ec2-user/fs513_files")	
	
	fmt.Println("cmdArgs", cmdArgs)
	cmdOut, err := exec.Command("scp", cmdArgs...).CombinedOutput()
	if err !=nil{
		fmt.Println("Error ", err)
		fmt.Println("Cmdout ", cmdOut)
	}
}
