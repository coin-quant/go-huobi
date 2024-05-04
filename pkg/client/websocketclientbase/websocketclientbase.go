package websocketclientbase

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/huobirdcenter/huobi_golang/internal/gzip"
	"github.com/huobirdcenter/huobi_golang/internal/model"
	"github.com/huobirdcenter/huobi_golang/logging/applogger"
	"strings"
	"sync"
)

const (
	wsPath   = "/ws"
	feedPath = "/feed"
)

type ErrHandler func(err error)

// It will be invoked after websocket connected
type ConnectedHandler func()

// It will be invoked after valid message received
type MessageHandler func(message string) (interface{}, error)

// It will be invoked after response is parsed
type ResponseHandler func(response interface{})

// The base class that responsible to get data from websocket
type WebSocketClientBase struct {
	host             string
	path             string
	conn             *websocket.Conn
	connectedHandler ConnectedHandler
	messageHandler   MessageHandler
	responseHandler  ResponseHandler
	sendMutex        *sync.Mutex
}

// Initializer
func (p *WebSocketClientBase) Init(host string) *WebSocketClientBase {
	p.host = host
	p.path = wsPath
	p.sendMutex = &sync.Mutex{}

	return p
}

// Initializer with path
func (p *WebSocketClientBase) InitWithFeedPath(host string) *WebSocketClientBase {
	p.Init(host)
	p.path = feedPath
	return p
}

// Set callback handler
func (p *WebSocketClientBase) SetHandler(connHandler ConnectedHandler, msgHandler MessageHandler, repHandler ResponseHandler) {
	p.connectedHandler = connHandler
	p.messageHandler = msgHandler
	p.responseHandler = repHandler
}

// Connect to websocket server
// if autoConnect is true, then the connection can be re-connect if no data received after the pre-defined timeout
func (p *WebSocketClientBase) Connect(errHandler ErrHandler) (chan struct{}, error) {
	var err error
	url := fmt.Sprintf("wss://%s%s", p.host, p.path)
	applogger.Debug("WebSocket connecting...")
	p.conn, _, err = websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		applogger.Error("WebSocket connected error: %s", err)
		return nil, err
	}
	applogger.Info("WebSocket connected")

	// start loop to read and handle message
	doneC := p.startReadLoop(errHandler)

	if p.connectedHandler != nil {
		p.connectedHandler()
	}
	return doneC, nil
}

// Send data to websocket server
func (p *WebSocketClientBase) Send(data string) {
	if p.conn == nil {
		applogger.Error("WebSocket sent error: no connection available")
		return
	}

	p.sendMutex.Lock()
	err := p.conn.WriteMessage(websocket.TextMessage, []byte(data))
	p.sendMutex.Unlock()

	if err != nil {
		applogger.Error("WebSocket sent error: data=%s, error=%s", data, err)
	}
}

func (p *WebSocketClientBase) Close() {
	p.conn.Close()
}

func (p *WebSocketClientBase) startReadLoop(errHandler ErrHandler) (doneC chan struct{}) {
	doneC = make(chan struct{})
	go func() {
		defer close(doneC)
		defer p.Close()
		for {
			msgType, buf, err := p.conn.ReadMessage()
			if err != nil {
				errHandler(err)
				return
			}

			// decompress gzip data if it is binary message
			if msgType == websocket.BinaryMessage {
				message, err := gzip.GZipDecompress(buf)
				if err != nil {
					applogger.Error("UnGZip data error: %s", err)
					errHandler(err)
					return
				}

				// Try to pass as PingMessage
				pingMsg := model.ParsePingMessage(message)

				// If it is Ping then respond Pong
				if pingMsg != nil && pingMsg.Ping != 0 {
					applogger.Debug("Received Ping: %d", pingMsg.Ping)
					pongMsg := fmt.Sprintf("{\"pong\": %d}", pingMsg.Ping)
					p.Send(pongMsg)
					applogger.Debug("Replied Pong: %d", pingMsg.Ping)
				} else if strings.Contains(message, "tick") || strings.Contains(message, "data") {
					// If it contains expected string, then invoke message handler and response handler
					result, err := p.messageHandler(message)
					if err != nil {
						applogger.Error("Handle message error: %s", err)
						continue
					}
					if p.responseHandler != nil {
						p.responseHandler(result)
					}
				}
			}
		}
	}()
	return
}
