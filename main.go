package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

const SERVER_NETWORK_PROTOCOL = "tcp"
const SERVER_PORT = "6379"
const BANNER = `
                 _ _    
 __ ___ _ ___ __| (_)___
 \ \ / '_/ -_) _| | (_-<
 /_\_\_| \___\__,_|_/__/
                        
`

func main() {
	fmt.Println(BANNER)
	log.Println("Starting xRedis")
	xredis := NewXRedis()
	listener, err := net.Listen(SERVER_NETWORK_PROTOCOL, ":"+SERVER_PORT)
	if err != nil {
		log.Panic("Could not start xredis on port " + SERVER_PORT)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Could not handle incoming connection")
			continue
		}

		go handleConnection(xredis, conn)
	}

}

func handleConnection(xredis *XRedis, conn net.Conn) {
	data := make([]byte, 1024)
	for {
		_, err := conn.Read(data)
		if err != nil {
			if err != io.EOF {
				log.Println("An error occurred reading from connection: " + conn.RemoteAddr().String())
			}
			conn.Close()
			return
		}

		rsp := xredis.handleRequest(data)
		conn.Write([]byte(rsp.serialize()))
	}
}
