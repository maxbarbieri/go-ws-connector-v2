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

type ErrorLevel string

const (
	ConnectorLevel   ErrorLevel = "ConnectorLevel"
	ApplicationLevel ErrorLevel = "ApplicationLevel"
)

type Error[ErrorType error] struct {
	ErrorLevel   ErrorLevel `json:"err_level"`
	ErrorMessage string     `json:"err_msg"`
	ErrorInfo    ErrorType  `json:"err_info,omitempty"` // ErrorInfo may also be nil or a zero value, please check before using to avoid nil pointer dereference errors.
}

type Message[DataType any, ErrorType error] struct {
	Data  DataType          `json:"data,omitempty"`
	Error *Error[ErrorType] `json:"error,omitempty"`
}

type wsSentMessage struct {
	Type    msgType                      `json:"type"`
	Id      uint64                       `json:"id,omitempty"`     //used to match requests and responses (optional, a request with no id or with id = 0 does not require a response)
	Method  string                       `json:"method,omitempty"` //optional, all subscription update requests are sent with no method
	Last    bool                         `json:"last,omitempty"`   //used only for subscription data messages, if true it means that this is the last response for the specified request id
	Message *Message[interface{}, error] `json:"msg"`
}

type wsReceivedMessage struct {
	Type    msgType         `json:"type"`
	Id      uint64          `json:"id,omitempty"` //used to match requests and responses (optional, a request with no id or with id = 0 does not require a response)
	Method  string          `json:"method,omitempty"`
	Last    bool            `json:"last,omitempty"` //used only for subscription data messages, if true it means that this is the last response for the specified request id
	Message json.RawMessage `json:"msg"`
}
