package ws_connector

import (
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

type ErrorMessage struct {
	Err string `json:"err"`
}

func (em *ErrorMessage) Error() string {
	return em.Err
}

func NewErrorMessage(format string, a ...any) *ErrorMessage {
	return &ErrorMessage{
		Err: fmt.Sprintf(format, a...),
	}
}

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

func (e *Error[ErrorType]) IsWsConnectionDown() bool {
	return e.ErrorMessage == WsConnectionDownError.Error()
}

func (e *Error[ErrorType]) IsUnknownMethodError() bool {
	return e.ErrorMessage == UnknownMethodError.Error()
}

func (e *Error[ErrorType]) IsUnknownTopicError() bool {
	return e.ErrorMessage == UnknownTopicError.Error()
}

func (e *Error[ErrorType]) IsDuplicateReqIdError() bool {
	return e.ErrorMessage == DuplicateReqIdError.Error()
}

var (
	/*
		Errors that may only be returned from Connector methods.
	*/

	AttemptToSendNilError                        = fmt.Errorf("attempt to send nil data")
	AttemptToRespondToFireAndForgetRequestError  = fmt.Errorf("attempt to respond to a fire&forget request")
	AttemptToSendMultipleResponsesToRequestError = fmt.Errorf("attempt to send multiple responses to a request")
	ResponseChannelAlreadyRequestedError         = fmt.Errorf("response channel already requested for this ResponseReader")
	RequestChannelAlreadyRequestedError          = fmt.Errorf("request channel already requested for this SubscriptionRequestReader")
	DataChannelAlreadyRequestedError             = fmt.Errorf("data channel already requested for this SubscriptionDataReader")

	/*
		Errors that may also be sent on the websocket connection.
	*/

	UnknownMethodError    = fmt.Errorf("unknown method")
	UnknownTopicError     = fmt.Errorf("unknown topic")
	WsConnectionDownError = fmt.Errorf("ws connection down")
	DuplicateReqIdError   = fmt.Errorf("duplicate req id (wait for previous request with this id to be completed before reusing the id)")

	unknownMethodMessage = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: UnknownMethodError.Error(),
	}}
	unknownTopicMessage = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: UnknownTopicError.Error(),
	}}
	wsConnectionDownMessage = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: WsConnectionDownError.Error(),
	}}
	duplicateReqIdMessage = &Message[interface{}, error]{Error: &Error[error]{
		ErrorLevel:   ConnectorLevel,
		ErrorMessage: DuplicateReqIdError.Error(),
	}}

	marshaledUnknownMethodMessage    json.RawMessage
	marshaledUnknownTopicMessage     json.RawMessage
	marshaledWsConnectionDownMessage json.RawMessage
	marshaledDuplicateReqIdMessage   json.RawMessage
)

func init() {
	var err error

	marshaledUnknownMethodMessage, err = jsoniter.ConfigFastest.Marshal(unknownMethodMessage)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", UnknownMethodError)
	}

	marshaledUnknownTopicMessage, err = jsoniter.ConfigFastest.Marshal(unknownTopicMessage)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", UnknownTopicError)
	}

	marshaledWsConnectionDownMessage, err = jsoniter.ConfigFastest.Marshal(wsConnectionDownMessage)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", WsConnectionDownError)
	}

	marshaledDuplicateReqIdMessage, err = jsoniter.ConfigFastest.Marshal(duplicateReqIdMessage)
	if err != nil {
		log.Panicf("Error marshaling WsConnector internal error %s\n", DuplicateReqIdError)
	}
}
