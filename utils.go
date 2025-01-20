package ws_connector

import (
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

var (
	/*
		Errors that may only be returned from Connector methods.
	*/

	ATTEMPT_TO_SEND_NIL_ERROR                           = fmt.Errorf("attempt to send nil data")
	ATTEMPT_TO_RESPOND_TO_FIRE_AND_FORGET_REQUEST_ERROR = fmt.Errorf("attempt to respond to a fire&forget request")
	ATTEMPT_TO_SEND_MULTIPLE_RESPONSES_TO_REQUEST_ERROR = fmt.Errorf("attempt to send multiple responses to a request")
	RESPONSE_CHANNEL_ALREADY_REQUESTED_ERROR            = fmt.Errorf("response channel already requested for this ResponseReader")
	REQUEST_CHANNEL_ALREADY_REQUESTED_ERROR             = fmt.Errorf("request channel already requested for this SubscriptionRequestReader")
	DATA_CHANNEL_ALREADY_REQUESTED_ERROR                = fmt.Errorf("data channel already requested for this SubscriptionDataReader")

	/*
		Errors that may also be sent on the websocket connection.
	*/

	UNKNOWN_METHOD_ERROR     = fmt.Errorf("unknown method")
	UNKNOWN_TOPIC_ERROR      = fmt.Errorf("unknown topic")
	WS_CONNECTION_DOWN_ERROR = fmt.Errorf("ws connection down")
	DUPLICATE_REQ_ID_ERROR   = fmt.Errorf("duplicate req id (wait for previous request with this id to be completed before reusing the id)")

	UNKNOWN_METHOD_ERROR_MESSAGE = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: UNKNOWN_METHOD_ERROR.Error(),
		ErrorInfo:    UNKNOWN_METHOD_ERROR,
	}}
	UNKNOWN_TOPIC_ERROR_MESSAGE = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: UNKNOWN_TOPIC_ERROR.Error(),
		ErrorInfo:    UNKNOWN_TOPIC_ERROR,
	}}
	WS_CONNECTION_DOWN_ERROR_MESSAGE = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: WS_CONNECTION_DOWN_ERROR.Error(),
		ErrorInfo:    WS_CONNECTION_DOWN_ERROR,
	}}
	DUPLICATE_REQ_ID_ERROR_MESSAGE = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: DUPLICATE_REQ_ID_ERROR.Error(),
		ErrorInfo:    DUPLICATE_REQ_ID_ERROR,
	}}

	MARSHALED_UNKNOWN_METHOD_ERROR_MESSAGE     json.RawMessage
	MARSHALED_UNKNOWN_TOPIC_ERROR_MESSAGE      json.RawMessage
	MARSHALED_WS_CONNECTION_DOWN_ERROR_MESSAGE json.RawMessage
	MARSHALED_DUPLICATE_REQ_ID_ERROR_MESSAGE   json.RawMessage
)

func init() {
	var err error

	MARSHALED_UNKNOWN_METHOD_ERROR_MESSAGE, err = jsoniter.ConfigFastest.Marshal(UNKNOWN_METHOD_ERROR_MESSAGE)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", UNKNOWN_METHOD_ERROR)
	}

	MARSHALED_UNKNOWN_TOPIC_ERROR_MESSAGE, err = jsoniter.ConfigFastest.Marshal(UNKNOWN_TOPIC_ERROR_MESSAGE)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", UNKNOWN_TOPIC_ERROR)
	}

	MARSHALED_WS_CONNECTION_DOWN_ERROR_MESSAGE, err = jsoniter.ConfigFastest.Marshal(WS_CONNECTION_DOWN_ERROR_MESSAGE)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", WS_CONNECTION_DOWN_ERROR)
	}

	MARSHALED_DUPLICATE_REQ_ID_ERROR_MESSAGE, err = jsoniter.ConfigFastest.Marshal(DUPLICATE_REQ_ID_ERROR_MESSAGE)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", DUPLICATE_REQ_ID_ERROR)
	}
}

type requestInfo struct {
	requestReader *RequestReader
	responder     *wsResponder
}

type subscriptionInfo struct {
	subscriptionRequestReader *SubscriptionRequestReader
	sender                    *wsSender
}

// SendRequest function that wraps the SendRequest method of a connector + the request of
// typed response channels, all in one call.
// This function sends only requests that require a response, for Fire&Forget requests
// please use the connector's SendRequest method directly.
func SendRequest[ResponseType any, ErrorType error](conn Connector, method string, data interface{}) (chan *Message[*ResponseType, ErrorType], error) {
	responseReader, err := conn.SendRequest(method, data, true)
	if err != nil {
		return nil, err
	}
	var typedResponseChan chan *Message[*ResponseType, ErrorType]
	typedResponseChan, err = GetTypedResponseChannel[ResponseType, ErrorType](responseReader)
	if err != nil {
		return nil, err
	}
	return typedResponseChan, nil
}
