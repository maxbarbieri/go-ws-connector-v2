package ws_connector

import (
	"sync/atomic"
)

type Sender interface {
	// Send sends a response to the associated subscriber. If last is set to true, the subscriber will not be able to
	// receive any more data. You can send either some data, an error, or even both together. You can send a nil payload
	// (both nil data and nil err) only when sending the last message of the subscription (i.e.: to close the
	// subscription, but you can also use the Close method for that). If last == false, you cannot send a nil payload,
	// so either data or err must be != nil.
	Send(data interface{}, err error, last bool) error

	// Close closes the subscription. After calling close, the subscriber will not be able to receive any more data.
	Close() error

	IsUnsubscribed() bool
	GetCustomFields() Map

	GetConnector() Connector
}

type wsSender struct {
	wsConnector  *websocketConnector
	subId        uint64
	topic        string
	disabled     atomic.Bool
	customFields Map
}

func (s *wsSender) Send(data interface{}, err error, last bool) error {
	//allow sending a nil payload only if this is the last message (i.e.: if we want to close the subscription)
	if data == nil && err == nil && !last {
		return ATTEMPT_TO_SEND_NIL_ERROR
	}

	//build the message object
	var msgObjPtr *Message[interface{}, error]
	if data != nil || err != nil { //only if either the data or the error is != nil, otherwise just leave the whole message set to nil
		msgObjPtr = &Message[interface{}, error]{}
		msgObjPtr.Data = data
		if err != nil { //if the error is nil, build the error object, otherwise just leave the whole error set to nil
			msgObjPtr.Error = &Error[error]{
				ErrorLevel:   ApplicationLevel,
				ErrorMessage: err.Error(),
				ErrorInfo:    err,
			}
		}
	}

	//if this sender is enabled (the peer is still subscribed) and the connection is active
	if !s.disabled.Load() && s.wsConnector.ongoingResetLock.TryRLock() {
		defer s.wsConnector.ongoingResetLock.RUnlock()

		//just send the message to the outgoing messages handler
		s.wsConnector.outgoingWsMsgChan <- &wsSentMessage{
			Type:    subscriptionData,
			Id:      s.subId,
			Method:  s.topic,
			Last:    last,
			Message: msgObjPtr,
		}

		if last { //if this was the last message for this subscription
			//disable this sender
			s.disable()

			//remove the subscription info from the wsConnector's map
			s.wsConnector.removeSubscriptionRequestInfo(s.subId, true)
		}

		return nil

	} else { //if the sender is disabled or the connection is not active
		return WS_CONNECTION_DOWN_ERROR
	}
}

func (s *wsSender) Close() error {
	return s.Send(nil, nil, true)
}

func (s *wsSender) IsUnsubscribed() bool {
	return s.disabled.Load()
}

func (s *wsSender) disable() {
	s.disabled.Store(true)
}

func (s *wsSender) GetCustomFields() Map {
	return s.customFields
}

func (s *wsSender) GetConnector() Connector {
	return s.wsConnector
}
