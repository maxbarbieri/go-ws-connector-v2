package ws_connector

import "encoding/json"

type msgType int

const (
	request msgType = iota
	response
	subscriptionRequest
	subscriptionData
	unsubscriptionRequest
)

type ErrorLevel int

const (
	ConnectorLevel ErrorLevel = iota
	ApplicationLevel
)

type Error[ErrorType any] struct {
	ErrorLevel   ErrorLevel
	ErrorMessage string
	ErrorInfo    ErrorType // ErrorInfo may also be nil or a zero value, please check before using to avoid nil pointer dereference errors.
}

type Message[DataType, ErrorType any] struct {
	Data  DataType          `json:"data,omitempty"`
	Error *Error[ErrorType] `json:"error,omitempty"`
}

type wsSentMessage struct {
	Type    msgType                      `json:"type"`
	Id      uint64                       `json:"id,omitempty"` //used to match requests and responses (optional, a request with no id or with id = 0 does not require a response)
	Method  string                       `json:"method,omitempty"`
	Last    bool                         `json:"last,omitempty"` //used only for subscription data messages, if true it means that this is the last response for the specified request id
	Message *Message[interface{}, error] `json:"msg"`
}

type wsReceivedMessage struct {
	Type    msgType         `json:"type"`
	Id      uint64          `json:"id,omitempty"` //used to match requests and responses (optional, a request with no id or with id = 0 does not require a response)
	Method  string          `json:"method,omitempty"`
	Last    bool            `json:"last,omitempty"` //used only for subscription data messages, if true it means that this is the last response for the specified request id
	Message json.RawMessage `json:"msg"`
}
