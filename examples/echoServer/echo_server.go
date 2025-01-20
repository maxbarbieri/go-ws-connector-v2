package main

import (
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	wsconnector "github.com/maxbarbieri/go-ws-connector-v2"
	log "github.com/sirupsen/logrus"
	"net/http"
)

var WsConnUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Accepting all requests
	},
}

type EchoMsg struct {
	Msg string `json:"msg"`
}

func wsEchoHandler(responder wsconnector.Responder, reqReader *wsconnector.RequestReader) {
	reqData, err := wsconnector.GetTypedRequestMessage[EchoMsg, error](reqReader)
	if err != nil {
		log.Warningf("Error in wsconnector.GetTypedRequestMessage[EchoMsg, error](reqReader): %s\n", err)
		_ = responder.SendResponse(nil, err)
		return
	}

	err = responder.SendResponse(reqData, nil)
	if err != nil {
		log.Warningf("Error in responder.SendResponse(reqData, nil): %s\n", err)
	}
}

func main() {
	//ws connector
	http.HandleFunc("/wsc", func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := WsConnUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Warningf("Error in WsConnUpgrader.Upgrade(): %s\n", err)
		}

		reqHandlers := []*wsconnector.RequestHandlerInfo{{Method: "echo", Handler: wsEchoHandler}}
		_ = wsconnector.NewServerConnectorWithDefaultParameters(wsConn, reqHandlers, nil, "ServerConnector_"+r.RemoteAddr, nil)
	})

	//ws connection (json)
	http.HandleFunc("/ws_json", func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := WsConnUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Warningf("Error in WsConnUpgrader.Upgrade(): %s\n", err)
		}

		defer wsConn.Close()

		var readMsgBytes []byte
		var writeMsgBytes []byte
		for {
			_, readMsgBytes, err = wsConn.ReadMessage()
			if err != nil {
				return
			}

			var msg EchoMsg
			err = jsoniter.ConfigFastest.Unmarshal(readMsgBytes, &msg)
			if err != nil {
				log.Warningf("Error in jsoniter.Unmarshal(readMsgBytes, &msg): %s\n", err)
				return
			}

			writeMsgBytes, err = jsoniter.ConfigFastest.Marshal(&msg)
			if err != nil {
				log.Warningf("Error in jsoniter.Marshal(writeMsgBytes): %s\n", err)
				return
			}

			err = wsConn.WriteMessage(websocket.TextMessage, writeMsgBytes)
			if err != nil {
				return
			}
		}
	})

	//ws connection (raw)
	http.HandleFunc("/ws_raw", func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := WsConnUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Warningf("Error in WsConnUpgrader.Upgrade(): %s\n", err)
		}

		defer wsConn.Close()

		var msgBytes []byte
		for {
			_, msgBytes, err = wsConn.ReadMessage()
			if err != nil {
				return
			}

			err = wsConn.WriteMessage(websocket.TextMessage, msgBytes)
			if err != nil {
				return
			}
		}
	})

	log.Panic(http.ListenAndServe("0.0.0.0:3000", nil))
}
