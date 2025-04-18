package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

const SERVER_NETWORK_PROTOCOL = "tcp"
const SERVER_PORT = "6379"
const BANNER = `
                 _ _    
 __ ___ _ ___ __| (_)___
 \ \ / '_/ -_) _| | (_-<
 /_\_\_| \___\__,_|_/__/

`
const DB_DUMP_FILE = "xredis_dump.db"

func main() {
	fmt.Print(BANNER)
	log.Println("Starting xRedis on port ", SERVER_PORT)

	xredis := NewXRedis()
	loadStoredState(xredis)

	listener, err := net.Listen(SERVER_NETWORK_PROTOCOL, ":"+SERVER_PORT)
	if err != nil {
		log.Panic("Could not start xredis on port " + SERVER_PORT)
	}

	log.Println("Ready to receive connections")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Could not handle incoming connection")
			continue
		}

		go handleConnection(xredis, conn)
	}
}

func loadStoredState(xredis *XRedis) {
	log.Println("Loading dump file")
	_, err := os.Stat(DB_DUMP_FILE)
	if errors.Is(err, os.ErrNotExist) {
		log.Println("No dump file to load")
		return
	}
	file, err := os.Open(DB_DUMP_FILE)
	if err != nil {
		log.Println("Failed opening dump file")
		return
	}
	defer file.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, file)
	if err != nil {
		log.Println("Failed copying dump file to byte buffer")
		return
	}

	log.Println("Dump file loaded succesfully")
	xredis.Load(buf.Bytes())
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

		conn.Write([]byte(handleRequest(xredis, data)))
	}
}
