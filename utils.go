package ws_connector

import jsoniter "github.com/json-iterator/go"

// JsoniterConfig is a global jsoniter config which is like jsoniter.ConfigFastest but marshals float64 values without
// truncating them to 6 decimal digits.
var JsoniterConfig = jsoniter.Config{
	EscapeHTML:                    false,
	ObjectFieldMustBeSimpleString: true,
}.Froze()

type requestInfo struct {
	requestReader *RequestReader
	responder     *wsResponder
}

type subscriptionInfo struct {
	subscriptionRequestReader *SubscriptionRequestReader
	sender                    *wsSender
}

// SendRequest function that wraps the SendRequest method of a connector + the request of the
// typed response channel, all in one call.
// This function sends only requests that require a response, for Fire&Forget requests
// please use the connector's SendRequest method directly.
func SendRequest[ResponseType any, ErrorType error](conn Connector, method string, data interface{}) (chan *Message[ResponseType, ErrorType], error) {
	responseReader, err := conn.SendRequest(method, data, true)
	if err != nil {
		return nil, err
	}
	var typedResponseChan chan *Message[ResponseType, ErrorType]
	typedResponseChan = GetTypedResponseChannel[ResponseType, ErrorType](responseReader)
	return typedResponseChan, nil
}
