package websocketclientbase

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/huobirdcenter/huobi_golang/internal/gzip"
	"github.com/huobirdcenter/huobi_golang/internal/model"
	"github.com/huobirdcenter/huobi_golang/internal/requestbuilder"
	"github.com/huobirdcenter/huobi_golang/logging/applogger"
	"github.com/huobirdcenter/huobi_golang/pkg/model/auth"
	"github.com/huobirdcenter/huobi_golang/pkg/model/base"
	"sync"
)

const (
	websocketV2Path = "/ws/v2"
)

// It will be invoked after websocket v2 authentication response received
type AuthenticationV2ResponseHandler func(resp *auth.WebSocketV2AuthenticationResponse)

// The base class that responsible to get data from websocket authentication v2
type WebSocketV2ClientBase struct {
	host string
	conn *websocket.Conn

	authenticationResponseHandler AuthenticationV2ResponseHandler
	messageHandler                MessageHandler
	responseHandler               ResponseHandler

	sendMutex      *sync.Mutex
	requestBuilder *requestbuilder.WebSocketV2RequestBuilder
}

// Initializer
func (p *WebSocketV2ClientBase) Init(accessKey string, secretKey string, host string) *WebSocketV2ClientBase {
	p.host = host
	p.requestBuilder = new(requestbuilder.WebSocketV2RequestBuilder).Init(accessKey, secretKey, host, websocketV2Path)
	p.sendMutex = &sync.Mutex{}
	return p
}

// Set callback handler
func (p *WebSocketV2ClientBase) SetHandler(authHandler AuthenticationV2ResponseHandler, msgHandler MessageHandler, repHandler ResponseHandler) {
	p.authenticationResponseHandler = authHandler
	p.messageHandler = msgHandler
	p.responseHandler = repHandler
}

// Connect to websocket server
// if autoConnect is true, then the connection can be re-connect if no data received after the pre-defined timeout
func (p *WebSocketV2ClientBase) Connect(errHandler ErrHandler) (chan struct{}, error) {
	var err error
	url := fmt.Sprintf("wss://%s%s", p.host, websocketV2Path)
	p.conn, _, err = websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		applogger.Error("WebSocket connected error: %s", err)
		return nil, err
	}
	applogger.Info("WebSocket connected")

	// start loop to read and handle message
	doneC := p.startReadLoop(errHandler)

	// send authentication if connect to websocket successfully
	auth, err := p.requestBuilder.Build()
	if err != nil {
		applogger.Error("Signature generated error: %s", err)
		return nil, err
	}
	p.Send(auth)
	return doneC, nil
}

// Send data to websocket server
func (p *WebSocketV2ClientBase) Send(data string) {
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

// Close the connection to server
func (p *WebSocketV2ClientBase) Close() {
	p.conn.Close()
}

func (p *WebSocketV2ClientBase) startReadLoop(errHandler ErrHandler) (doneC chan struct{}) {
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
			var message string
			if msgType == websocket.BinaryMessage {
				message, err = gzip.GZipDecompress(buf)
				if err != nil {
					applogger.Error("UnGZip data error: %s", err)
					errHandler(err)
					return
				}
			} else if msgType == websocket.TextMessage {
				message = string(buf)
			}

			// Try to pass as PingV2Message
			// If it is Ping then respond Pong
			pingV2Msg := model.ParsePingV2Message(message)
			if pingV2Msg.IsPing() {
				applogger.Debug("Received Ping: %d", pingV2Msg.Data.Timestamp)
				pongMsg := fmt.Sprintf("{\"action\": \"pong\", \"data\": { \"ts\": %d } }", pingV2Msg.Data.Timestamp)
				p.Send(pongMsg)
				applogger.Debug("Respond  Pong: %d", pingV2Msg.Data.Timestamp)
			} else {
				// Try to pass as websocket v2 authentication response
				// If it is then invoke authentication handler
				wsV2Resp := base.ParseWSV2Resp(message)
				if wsV2Resp != nil {
					switch wsV2Resp.Action {
					case "req":
						authResp := auth.ParseWSV2AuthResp(message)
						if authResp != nil && p.authenticationResponseHandler != nil {
							p.authenticationResponseHandler(authResp)
						}

					case "sub", "push":
						{
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
			}
		}
	}()
	return
}
