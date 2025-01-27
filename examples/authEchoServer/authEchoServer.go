package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	wsconnector "github.com/maxbarbieri/go-ws-connector-v2"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

var WsConnUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Accepting all requests
	},
}

type EchoMsg struct {
	Msg string `json:"msg"`
}

type AuthRequest struct {
	Token string `json:"token"`
}

type AuthResponse struct {
	Success bool `json:"success"`
}

type AuthError struct {
	Success bool `json:"success"`
}

func (authRespErr *AuthError) Error() string {
	return "Failed authentication"
}

func wsEchoHandler(responder wsconnector.Responder, reqReader *wsconnector.RequestReader) {
	reqMsg := wsconnector.GetTypedRequestMessage[*EchoMsg, *wsconnector.ErrorMessage](reqReader)
	if reqMsg.Error != nil {
		log.Warningf("Error in wsconnector.GetTypedRequestMessage[*EchoMsg, *wsconnector.ErrorMessage](reqReader): %s %s\n", reqMsg.Error.ErrorLevel, reqMsg.Error.ErrorMessage)
		_ = responder.SendResponse(nil, fmt.Errorf("couldn't get typed request message: %s %s", reqMsg.Error.ErrorLevel, reqMsg.Error.ErrorMessage))
		return
	}

	err := responder.SendResponse(reqMsg.Data, nil)
	if err != nil {
		log.Warningf("Error in responder.SendResponse(reqMsg.Data, nil): %s\n", err)
	}
}

func checkAuth(reqReader *wsconnector.RequestReader) bool {
	authMsg := wsconnector.GetTypedRequestMessage[*AuthRequest, *AuthError](reqReader)
	if authMsg.Error != nil {
		return false
	}

	if authMsg.Data.Token != "abcd" { //invalid token
		return false
	}

	return true
}

func wsAuthHandler(authenticatedChan chan bool) func(wsconnector.Responder, *wsconnector.RequestReader) {
	return func(responder wsconnector.Responder, requestReader *wsconnector.RequestReader) {
		if responder == nil { //if no response has been requested
			authenticatedChan <- false //do not authenticate
			return
		}

		//check authentication
		authSuccess := checkAuth(requestReader)

		//send authentication response / error
		if authSuccess {
			log.Infof("Successful authentication")
			err := responder.SendResponse(&AuthResponse{Success: true}, nil)
			if err != nil {
				log.Warningf("[wsAuthHandler] Error in responder.SendResponse(&AuthResponse{Success: true},nil): %s\n", err)
			}

		} else {
			err := responder.SendResponse(nil, &AuthError{Success: false})
			if err != nil {
				log.Warningf("[wsAuthHandler] Error in responder.SendResponse(nil, &AuthError{Success: false}): %s\n", err)
			}
		}

		//notify authentication outcome on channel
		authenticatedChan <- authSuccess
	}
}

func main() {
	http.HandleFunc("/wsc", func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := WsConnUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Warningf("Error in WsConnUpgrader.Upgrade(): %s\n", err)
		}

		authenticatedChan := make(chan bool, 1)

		reqHandlers := []*wsconnector.RequestHandlerInfo{
			{Method: "auth", Handler: wsAuthHandler(authenticatedChan)},
		}

		srvWsConn := wsconnector.NewServerConnectorWithDefaultParameters(wsConn, reqHandlers, nil, "ServerConnector_"+r.RemoteAddr, nil)

		select {
		case <-time.After(2 * time.Second): //timeout
			srvWsConn.Close()
			return

		case authSuccess := <-authenticatedChan:
			if !authSuccess { //not authenticated
				srvWsConn.Close()
				return
			}
		}

		//add authenticated handlers
		srvWsConn.RegisterRequestHandler("echo", wsEchoHandler)
	})

	log.Panic(http.ListenAndServe("0.0.0.0:3000", nil))
}
