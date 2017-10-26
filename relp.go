package relp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const relpVersion = 0
const relpSoftware = "gorelp,0.2.0,https://github.com/stith/gorelp"

// Message - A single RELP message
type Message struct {
	// The transaction ID that the message was sent in
	Txn int
	// The command that was run. Will be "syslog" pretty much always under normal
	// operation
	Command string
	// The actual message data
	Data string

	// true if the message has been acked
	Acked bool

	// Used internally for acking.
	sourceConnection io.ReadWriteCloser
}

type ClientEvent interface {
	Event() string
	Message() string
	Error() error
}

type Stats struct {
	sent     int64
	received int64
	errors   int
	acked    int64
}

// Client - A client to a RELP server
type Client struct {
	Id string

	// connection net.Conn
	connection io.ReadWriteCloser
	reader     *bufio.Reader
	writer     *bufio.Writer

	windowSize int
	window     Window
	nextTxn    int

	eventChan chan ClientEvent
	mutex     *sync.Mutex

	Error  error
	Closed bool

	stats Stats
}

func readMessage(reader *bufio.Reader) (message Message, err error) {
	txn, err := reader.ReadString(' ')
	if err == io.EOF {
		// A graceful EOF means the client closed the connection. Hooray!
		fmt.Println("Done!")
		return
	} else if err != nil && strings.HasSuffix(err.Error(), "connection reset by peer") {
		fmt.Println("Client rudely disconnected, but that's fine.")
		return
	} else if err != nil {
		log.Println("Error reading txn:", err, txn)
		return
	}
	message.Txn, _ = strconv.Atoi(strings.TrimSpace(txn))

	cmd, err := reader.ReadString(' ')
	if err != nil {
		log.Println("Error reading cmd:", err)
		return
	}
	message.Command = strings.TrimSpace(cmd)

	// Check for dataLen == 0
	peekLen, err := reader.Peek(1)
	message.Data = ""
	if string(peekLen[:]) != "0" {
		dataLenS, err := reader.ReadString(' ')
		if err != nil {
			log.Println("Error reading dataLen:", err)
			return message, err
		}

		dataLen, err := strconv.Atoi(strings.TrimSpace(dataLenS))
		if err != nil {
			log.Println("Error converting dataLenS to int:", err)
			return message, err
		}

		dataBytes := make([]byte, dataLen)
		_, err = io.ReadFull(reader, dataBytes)
		if err != nil {
			log.Println("Error reading message:", err)
			return message, err
		}
		message.Data = string(dataBytes[:dataLen])
	}
	return message, err
}

// NewClient - Starts a new RELP client
func NewClient(host string, port int, windowSize int, eventChan chan ClientEvent) (Client, error) {
	connection, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return Client{}, err
	}

	return NewClientFrom(connection, windowSize, eventChan)
}

// NewClientFrom creates a RelpClient from a ReadWriteCloser (i.e. TCP or TLS connection)
func NewClientFrom(rwc io.ReadWriteCloser, windowSize int, eventChan chan ClientEvent) (client Client, err error) {
	client.Id = fmt.Sprintf("%v", uint64(math.Abs(float64(rand.Int63()))))

	client.connection = rwc
	client.eventChan = eventChan
	client.mutex = &sync.Mutex{}

	client.reader = bufio.NewReaderSize(rwc, 512*1024)
	client.writer = bufio.NewWriterSize(rwc, 2*1024*1024)

	offer := Message{
		Txn:     1,
		Command: "open",
		Data:    fmt.Sprintf("relp_version=%d\nrelp_software=%s\ncommands=syslog", relpVersion, relpSoftware),
	}
	offer.send(client.writer)
	client.writer.Flush()

	offerResponse, err := readMessage(client.reader)
	if err != nil {
		return client, err
	}

	responseParts := strings.Split(offerResponse.Data, "\n")
	if !strings.HasPrefix(responseParts[0], "200 OK") {
		err = fmt.Errorf("server responded to offer with: %s", responseParts[0])
	} else {
		err = nil
	}

	client.nextTxn = 2
	// TODO: Parse the server's info/commands into the Client object

	client.windowSize = windowSize
	client.window = NewArrayWindow(windowSize)

	go client.Reader()

	return client, err
}

// Send - Sends a message
func (m Message) send(out io.Writer) (nn int, err error) {
	outLength := len([]byte(m.Data))
	outString := fmt.Sprintf("%d %s %d %s\n", m.Txn, m.Command, outLength, m.Data)
	return out.Write([]byte(outString))
}

// SendString - Convenience method which constructs a Message and sends it
func (c *Client) SendString(msg string) (err error) {
	message := Message{
		Txn:     c.nextTxn,
		Command: "syslog",
		Data:    msg,
	}

	err = c.SendMessage(message)

	return err
}

func (c *Client) Flush() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.writer.Flush()
}

// SendMessage - Sends a message using the client's connection
func (c *Client) SendMessage(msg Message) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// return last error from reader thread
	if c.Error != nil {
		return c.Error
	}

	c.stats.sent++

	c.nextTxn = c.nextTxn + 1
	c.window.Add(Txn(msg.Txn))
	_, err = msg.send(c.writer)

	if err != nil {
		c.notifyError(err)
		c.Error = err
	}

	// TODO - magic number of messages remaining before we flush
	if c.window.Remaining() < 10 {
		c.writer.Flush()
	}

	return c.Error
}

// Outstanding returns the number of messages outstanding
func (c *Client) Outstanding() int {
	return c.window.Outstanding()
}

// Drain waits for all outstanding messages to be acked
func (c *Client) Drain(timeout time.Duration) (err error) {
	deadline := time.Now().Add(timeout)

	for {
		c.Flush()
		if c.window.Outstanding() == 0 {
			return nil
		}
		if timeout != 0 && time.Now().After(deadline) {
			err = errors.New("drain() timed out")
			c.notifyError(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Reader reader responses on the client connection and updates the protocol window
// Any errors encountered are sent on the eventChan
func (c *Client) Reader() {
	for {
		var err error
		if c.Closed {
			return
		}
		ack, err := readMessage(c.reader)
		doClose := false
		if err != nil {
			log.Fatal("Failed to read response: ", err)
			doClose = true
		} else {
			switch ack.Command {
			case "rsp":
				c.stats.acked++
				c.window.Remove(Txn(ack.Txn))
			case "serverclose":
				log.Print("Received serverclose hint")
				c.notifyClose()
				doClose = true
			default:
				c.stats.errors++
				err = errors.Errorf("Received non-rsp response from server (%v, %v, %v)\n", ack.Txn, ack.Command, ack.Data)
				doClose = true
			}
		}
		if err != nil {
			c.Error = err
			log.Fatal("Error in Reader event loop: ", err.Error())
			c.notifyError(err)
		}
		if doClose {
			c.Close()
			return
		}
	}

}

func (c *Client) forceClose() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.Closed {
		return
	}

	c.connection.Close()
	c.Closed = true

	c.notifyClose()
}

// Close - Closes the connection gracefully
func (c *Client) Close() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	closeMessage := Message{
		Txn:     c.nextTxn,
		Command: "close",
	}
	_, err = closeMessage.send(c.writer)
	if err != nil {
		c.notifyError(err)
	}
	c.writer.Flush()
	if err != nil {
		c.notifyError(err)
	}

	// force close in 5 seconds if the server doesn't handle it
	go func() {
		time.Sleep(5 * time.Second)
		c.forceClose()
	}()

	return
}
