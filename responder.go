package ws_connector

import (
	"sync/atomic"
)

type Responder interface {
	// SendResponse sends a response (for the request to which this responder is attached) to the peer on the other side of the websocket connection.
	// You can send either some data, an error,  or even both together. You cannot send a nil payload, so either data or err must be != nil.
	SendResponse(data interface{}, err error) error

	// GetConnector get a reference to the parent Connector object.
	GetConnector() Connector

	// ResponseRequired returns true if the request sender wants a response, false if this is a fire&forget request.
	ResponseRequired() bool
}

type wsResponder struct {
	wsConnector *websocketConnector
	reqId       uint64
	method      string
	disabled    atomic.Bool
}

func (r *wsResponder) SendResponse(data interface{}, err error) error {
	if r.reqId == 0 {
		return ATTEMPT_TO_RESPOND_TO_FIRE_AND_FORGET_REQUEST_ERROR
	}

	if data == nil && err == nil { //if both data and error are nil
		return ATTEMPT_TO_SEND_NIL_ERROR
	}

	//build the message object
	msgObj := Message[interface{}, error]{
		Data: data,
	}
	if err != nil { //if the error is nil, build the error object, otherwise just leave the whole error set to nil
		msgObj.Error = &Error[error]{
			ErrorLevel:   ApplicationLevel,
			ErrorMessage: err.Error(),
			ErrorInfo:    err,
		}
	}

	//if this responder is enabled (the peer is still subscribed) and the connection is active
	if !r.disabled.Load() {
		if r.wsConnector.ongoingResetLock.TryRLock() {
			defer r.wsConnector.ongoingResetLock.RUnlock()

			//just send the message to the outgoing messages handler
			r.wsConnector.outgoingWsMsgChan <- &wsSentMessage{
				Type:    response,
				Id:      r.reqId,
				Method:  r.method,
				Message: &msgObj,
			}

			//remove the request info from the wsConnector's map
			r.wsConnector.removeRequestInfo(r.reqId, true)

			//disable the responder
			r.disable()

			return nil

		} else { //if the connection is not active
			return WS_CONNECTION_DOWN_ERROR
		}

	} else { //if the responder is disabled
		return ATTEMPT_TO_SEND_MULTIPLE_RESPONSES_TO_REQUEST_ERROR
	}
}

func (r *wsResponder) disable() {
	r.disabled.Store(true)
}

func (r *wsResponder) GetConnector() Connector {
	return r.wsConnector
}

func (r *wsResponder) ResponseRequired() bool {
	return r.reqId != 0
}
