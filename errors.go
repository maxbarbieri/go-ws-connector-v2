package ws_connector

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
	return e.ErrorMessage == WS_CONNECTION_DOWN_ERROR.Error()
}

func (e *Error[ErrorType]) IsUnknownMethodError() bool {
	return e.ErrorMessage == UNKNOWN_METHOD_ERROR.Error()
}

func (e *Error[ErrorType]) IsUnknownTopicError() bool {
	return e.ErrorMessage == UNKNOWN_TOPIC_ERROR.Error()
}

func (e *Error[ErrorType]) IsDuplicateReqIdError() bool {
	return e.ErrorMessage == DUPLICATE_REQ_ID_ERROR.Error()
}
