package relp

import (
	"fmt"

	"bufio"
	"log"
	"net"
)

/*
Server - Contains info about the RELP listener

  MessageChannel - Emits messages as they are received
*/
type Server struct {
	MessageChannel chan Message

	AutoAck bool

	listener net.Listener
	done     bool
}

type ServerConn struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	delayFlush bool
}

func (sc *ServerConn) Read(p []byte) (n int, err error) {
	return sc.reader.Read(p)
}

func (sc *ServerConn) Write(p []byte) (n int, err error) {
	n, err = sc.writer.Write(p)
	if err == nil && !sc.delayFlush {
		sc.writer.Flush()
	}
	return
}

func (sc *ServerConn) Close() (err error) {
	sc.writer.Flush()
	sc.reader = nil
	sc.writer = nil
	return sc.conn.Close()
}

func handleConnection(conn *ServerConn, server Server) {
	defer conn.Close()

	log.Print("handling connection")

	for {
		log.Print("reading message")
		message, _ := readMessage(conn.reader)
		message.sourceConnection = conn

		log.Print("got command ", message.Command)

		response := Message{
			Txn:     message.Txn,
			Command: "rsp",
		}

		switch message.Command {
		case "open":
			response.Data = fmt.Sprintf("200 OK\nrelp_version=%d\nrelp_software=%s\ncommands=syslog", relpVersion, relpSoftware)
			_, err := response.send(conn)
			if err != nil {
				log.Println(err)
				return
			}
		case "syslog":
			server.MessageChannel <- message
			if server.AutoAck {
				err := message.Ack()
				if err != nil {
					fmt.Println("Error sending syslog ok:", err)
					return
				}
			}
		case "close":
			fmt.Println("Got a close, closing!")
			return
		default:
			log.Println("Got unknown command:", message.Command)
			response.Data = "500 ERR"
			_, err := response.send(conn)
			if err != nil {
				log.Println("Error sending 500 ERR:", err)
				return
			}
		}
	}
}

func acceptConnections(server Server) {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}
		if conn != nil {
			conn := ServerConn{
				conn:       conn,
				reader:     bufio.NewReader(conn),
				writer:     bufio.NewWriter(conn),
				delayFlush: false,
			}
			go handleConnection(&conn, server)
		}
		if server.done {
			return
		}
	}
}

// NewServer - Fire up a server to accept connections and emit messages
// Returns a Server
func NewServer(host string, port int, autoAck bool) (server Server, err error) {
	server.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	server.AutoAck = autoAck

	if err != nil {
		return server, err
	}

	server.MessageChannel = make(chan Message)
	go acceptConnections(server)
	return server, nil
}

// Close - Stops listening for connections and closes the message channel
func (s Server) Close() {
	s.done = true
	s.listener.Close()
	close(s.MessageChannel)
}

// Ack - Acknowledges a message
func (m *Message) Ack() (err error) {
	if m.Acked {
		return fmt.Errorf("called Ack on already-acknowledged message %d", m.Txn)
	}

	if m.sourceConnection == nil {
		// If the source connection is gone, we don't need to do any work.
		return nil
	}

	ackMessage := Message{
		Txn:     m.Txn,
		Command: "rsp",
		Data:    "200 OK",
	}
	_, err = ackMessage.send(m.sourceConnection)
	if err != nil {
		return err
	}
	m.Acked = true
	return
}
