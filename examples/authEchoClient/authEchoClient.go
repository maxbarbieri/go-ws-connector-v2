package main

import (
	wsconnector "github.com/maxbarbieri/go-ws-connector-v2"
	log "github.com/sirupsen/logrus"
)

type EchoMsg struct {
	Msg string `json:"msg"`
}

type AuthRequest struct {
	Token string `json:"token"`
}

type AuthResponse struct {
	Success bool `json:"success"`
}

func main() {
	url := "ws://localhost:3000/wsc"

	clientWsConn, err := wsconnector.NewClientConnectorWithDefaultParameters(url, nil, nil, "authEchoClient", nil, nil, nil)
	if err != nil {
		log.Panicf("Failed to create client websocket connector: %s\n", err)
	}

	//withoutAuthentication(clientWsConn)
	withAuthentication(clientWsConn)
}

func withAuthentication(clientWsConn wsconnector.ClientConnector) {
	log.Infof("WITH AUTHENTICATION")

	authReq := &AuthRequest{
		Token: "abcd",
	}

	authRespReader, err := clientWsConn.SendRequest("auth", authReq, true)
	if err != nil {
		log.Panicf("Failed to send auth request: %s\n", err)
	}

	var authResp *wsconnector.Message[*AuthResponse, error]
	authResp, err = wsconnector.GetTypedResponse[*AuthResponse, error](authRespReader)
	if err != nil {
		log.Panicf("Failed to get auth response: %s\n", err)
	}

	log.Infof("Got auth response: %+v %+v\n", authResp.Data, authResp.Error)

	echoMsg := &EchoMsg{
		Msg: "Hello world!",
	}

	var echoRespReader *wsconnector.ResponseReader
	echoRespReader, err = clientWsConn.SendRequest("echo", echoMsg, true)
	if err != nil {
		log.Panicf("Failed to send echo request: %s\n", err)
	}

	var echoResp *wsconnector.Message[*EchoMsg, error]
	echoResp, err = wsconnector.GetTypedResponse[*EchoMsg, error](echoRespReader)
	if err != nil {
		log.Panicf("Failed to get echo response: %s\n", err)
	}

	log.Infof("Got echo response: %+v %+v\n", echoResp.Data, echoResp.Error)
}

func withoutAuthentication(clientWsConn wsconnector.ClientConnector) {
	log.Infof("WITHOUT AUTHENTICATION")

	echoMsg := &EchoMsg{
		Msg: "Hello world!",
	}

	echoRespReader, err := clientWsConn.SendRequest("echo", echoMsg, true)
	if err != nil {
		log.Panicf("Failed to send echo request: %s\n", err)
	}

	var echoResp *wsconnector.Message[*EchoMsg, error]
	echoResp, err = wsconnector.GetTypedResponse[*EchoMsg, error](echoRespReader)
	if err != nil {
		log.Panicf("Failed to get echo response: %s\n", err)
	}

	log.Infof("Got echo response: %+v\n", echoResp)
}
