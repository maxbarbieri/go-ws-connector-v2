package ws_connector

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

type ClientConnector interface {
	// RegisterRequestHandler adds the specified request handler to the connector, associated with the specified method.
	RegisterRequestHandler(method string, handler RequestHandlerFunction)

	// RemoveRequestHandler removes the request handler that corresponds to the specified method.
	RemoveRequestHandler(method string)

	// RegisterSubscriptionRequestHandler adds the specified subscription request handler to the connector, associated with the specified topic.
	RegisterSubscriptionRequestHandler(topic string, handler SubscriptionRequestHandlerFunction)

	// RemoveSubscriptionRequestHandler removes the subscription request handler that corresponds to the specified topic.
	RemoveSubscriptionRequestHandler(topic string)

	// SendRequest sends a request to the peer on the other side of the connection.
	// If requireResponse is set to false, the request is sent with a "Fire&Forget" semantics. In this case the returned channels structure will be nil.
	// If requireResponse is set to true, a response will be sent on the returned channels.
	SendRequest(method string, data interface{}, requireResponse bool) (*ResponseReader, error)

	// Subscribe sends a subscription request to the peer on the other side of the connection.
	// Subscription data will be sent on the returned channels, until the peer sends a special message, marked as the
	// "last" one, that causes the channels to be closed.
	// For client connectors: standard subscriptions are not automatically restored (the subscription request is not
	// automatically re-sent to the peer) when the connection is restored after a failure.
	Subscribe(topic string, data interface{}) (*SubscriptionDataReader, error)

	// PersistentSubscribe sends a persistent subscription request to the peer on the other side of the connection.
	// Subscription data will be sent on the returned channels, until the peer sends a special message, marked as the
	// "last" one, that causes the channels to be closed.
	// Persistent subscriptions are automatically restored (the subscription request is automatically re-sent to the
	// peer) when the connection is restored after a failure. In this case, the same channel, subscription ID and
	// SubscriptionDataReader of the original subscription are used for the restored subscription.
	// If an error is returned, no subscription has been made, you have to retry by calling PersistentSubscribe again.
	// Once the "first" subscription is made (and no error is returned), only then it will automatically be restored on
	// reconnections.
	PersistentSubscribe(topic string, data interface{}) (*SubscriptionDataReader, error)

	// UpdateSubscription If a subscription update message is sent associated with a SubscriptionDataReader of a previous
	// subscription that is not active anymore, the message will be discarded by the peer (since these messages are sent
	// without method/topic field).
	UpdateSubscription(subDataReader *SubscriptionDataReader, data interface{}) error

	// Unsubscribe unsubscribes the subscription identified by the specified SubscriptionDataReader.
	// A message is sent to the peer to request the unsubscription. The peer cannot decide to ignore the unsubscription
	// request, but some messages may still be sent after the call to this method if, for some reason, the peer doesn't
	// process the unsubscription request immediately.
	// When the peer processes the unsubscription request, it will send an empty message marked as the last one that
	// will be received on the subscription's response reader. The channel is then closed and the response handler is
	// removed. It is possible that, due to concurrency on the peer's side, a message is sent immediately after the
	// "last" message triggered by the unsubscribe operation; in this case that message will be treated as a message
	// for an unknown subscription (because the response reader has been removed after the "last" message; this triggers
	// a warning log) and it won't be handled (it won't be sent to the response reader).
	// Keep in mind that this is not guaranteed to cancel the "side effects" of a subscription on the peer's side, as
	// its handler on the peer may still be running and, even though it has a way of checking if the subscriber has
	// unsubscribed, this is not enforced. The unsubscription request is sent as a Fire&Forget request.
	// It does not return an error if the specified subDataReader is unknown (or already unsubscribed).
	Unsubscribe(subDataReader *SubscriptionDataReader) error

	// UnsubscribeAll is the same as calling Unsubscribe on all subscriptions that are currently open on the connector.
	UnsubscribeAll() error

	// Close closes the connector and the underlying websocket connection
	Close()

	// GetLogTag returns the connector's log tag
	GetLogTag() string
}

type ServerConnector interface {
	// RegisterRequestHandler adds the specified request handler to the connector, associated with the specified method.
	RegisterRequestHandler(method string, handler RequestHandlerFunction)

	// RemoveRequestHandler removes the request handler that corresponds to the specified method.
	RemoveRequestHandler(method string)

	// RegisterSubscriptionRequestHandler adds the specified subscription request handler to the connector, associated with the specified topic.
	RegisterSubscriptionRequestHandler(topic string, handler SubscriptionRequestHandlerFunction)

	// RemoveSubscriptionRequestHandler removes the subscription request handler that corresponds to the specified topic.
	RemoveSubscriptionRequestHandler(topic string)

	// SendRequest sends a request to the peer on the other side of the connection.
	// If requireResponse is set to false, the request is sent with a "Fire&Forget" semantics. In this case the returned channels structure will be nil.
	// If requireResponse is set to true, a response will be sent on the returned channels.
	SendRequest(method string, data interface{}, requireResponse bool) (*ResponseReader, error)

	// Subscribe sends a subscription request to the peer on the other side of the connection.
	// Subscription data will be sent on the returned channels, until the peer sends a special message, marked as the "last"
	// one, that causes the channels to be closed.
	// For client connectors: standard subscriptions are not automatically restored (the subscription request is not
	// automatically re-sent to the peer) when the connection is restored after a failure.
	Subscribe(topic string, data interface{}) (*SubscriptionDataReader, error)

	// UpdateSubscription If a subscription update message is sent associated with a SubscriptionDataReader of a previous
	// subscription that is not active anymore, the message will be discarded by the peer (since these messages are sent
	// without method/topic field).
	UpdateSubscription(subDataReader *SubscriptionDataReader, data interface{}) error

	// Unsubscribe unsubscribes the subscription identified by the specified SubscriptionDataReader.
	// A message is sent to the peer to request the unsubscription. The peer cannot decide to ignore the unsubscription
	// request, but some messages may still be sent after the call to this method if, for some reason, the peer doesn't
	// process the unsubscription request immediately.
	// When the peer processes the unsubscription request, it will send an empty message marked as the last one that
	// will be received on the subscription's response reader. The channel is then closed and the response handler is
	// removed. It is possible that, due to concurrency on the peer's side, a message is sent immediately after the
	// "last" message triggered by the unsubscribe operation; in this case that message will be treated as a message
	// for an unknown subscription (because the response reader has been removed after the "last" message; this triggers
	// a warning log) and it won't be handled (it won't be sent to the response reader).
	// Keep in mind that this is not guaranteed to cancel the "side effects" of a subscription on the peer's side, as
	// its handler on the peer may still be running and, even though it has a way of checking if the subscriber has
	// unsubscribed, this is not enforced. The unsubscription request is sent as a Fire&Forget request.
	// It does not return an error if the specified subDataReader is unknown (or already unsubscribed).
	Unsubscribe(subDataReader *SubscriptionDataReader) error

	// Close closes the connector and the underlying websocket connection
	Close()

	// GetLogTag returns the connector's log tag
	GetLogTag() string
}

// Connector generic connector interface, which has all the methods of the ServerConnector, which
// is the most "restrictive" between the two types of Connector (client and server)
type Connector ServerConnector

type websocketConnector struct {
	logTag string

	wsUrl              string                      //empty string for servers, otherwise it's a client
	authTokenGenerator func() ([]string, []string) //always nil for servers, for clients it's nil if no authentication is required
	wsConn             *websocket.Conn
	incomingWsMsgChan  chan *wsReceivedMessage
	outgoingWsMsgChan  chan *wsSentMessage

	responseChanBufferSize            int
	subscriptionRequestChanBufferSize int
	incomingMsgChanBufferSize         int
	outgoingMsgChanBufferSize         int

	secondsBetweenReconnections int64

	nextReqIdLock sync.Mutex
	nextReqId     uint64
	nextSubIdLock sync.Mutex
	nextSubId     uint64

	mapSentRequestIdToResponseReader           map[uint64]*ResponseReader
	mapSentRequestIdToResponseReaderLock       sync.RWMutex
	mapSentSubIdToSubDataReader                map[uint64]*SubscriptionDataReader
	mapSentSubIdToSubDataReaderLock            sync.RWMutex
	mapReceivedRequestMethodToHandler          map[string]RequestHandlerFunction
	mapReceivedRequestMethodToHandlerLock      sync.RWMutex
	mapReceivedSubscriptionMethodToHandler     map[string]SubscriptionRequestHandlerFunction
	mapReceivedSubscriptionMethodToHandlerLock sync.RWMutex
	mapReceivedReqIdToRequestInfo              map[uint64]*requestInfo
	mapReceivedReqIdToRequestInfoLock          sync.RWMutex
	mapReceivedSubIdToSubscriptionInfo         map[uint64]*subscriptionInfo
	mapReceivedSubIdToSubscriptionInfoLock     sync.RWMutex

	//called when the connection fails for server WSCs or when the connection is restored for client WSCs
	connFailedCallback   func(failedConnector Connector)
	connRestoredCallback func(restoredConnector Connector)

	ongoingResetLock     sync.RWMutex
	goroutinesActiveLock sync.RWMutex
	closing              bool       //to differentiate a connection error from an explicit call to .Close()
	resetOnce            *sync.Once //use a pointer here, so it can be reset after the reset procedure (we can't directly overwrite a sync.Once that's being used, but we can change the pointer for the next reset procedure)
}

func NewClientConnectorWithDefaultParameters(wsUrl string, requestHandlers []*RequestHandlerInfo, subscriptionRequestHandlers []*SubscriptionRequestHandlerInfo, logTag string, authTokenGenerator func() ([]string, []string), connFailedCallback func(failedConnector Connector), connRestoredCallback func(restoredConnector Connector)) (ClientConnector, error) {
	return NewClientConnector(wsUrl, requestHandlers, subscriptionRequestHandlers, 1000, 100, 300, 10, 2, logTag, authTokenGenerator, connFailedCallback, connRestoredCallback)
}

func NewClientConnector(wsUrl string, requestHandlers []*RequestHandlerInfo, subscriptionRequestHandlers []*SubscriptionRequestHandlerInfo, incomingMsgChanBufferSize, outgoingMsgChanBufferSize, responseChanBufferSize, subscriptionRequestChanBufferSize int, secondsBetweenReconnections int64, logTag string, authTokenGenerator func() ([]string, []string), connFailedCallback func(failedConnector Connector), connRestoredCallback func(restoredConnector Connector)) (ClientConnector, error) {
	//instantiate the websocket connector
	wsConnector := websocketConnector{
		logTag:                                 logTag,
		wsUrl:                                  wsUrl,
		authTokenGenerator:                     authTokenGenerator,
		secondsBetweenReconnections:            secondsBetweenReconnections,
		responseChanBufferSize:                 responseChanBufferSize,
		subscriptionRequestChanBufferSize:      subscriptionRequestChanBufferSize,
		incomingMsgChanBufferSize:              incomingMsgChanBufferSize,
		outgoingMsgChanBufferSize:              outgoingMsgChanBufferSize,
		incomingWsMsgChan:                      make(chan *wsReceivedMessage, incomingMsgChanBufferSize),
		outgoingWsMsgChan:                      make(chan *wsSentMessage, outgoingMsgChanBufferSize),
		mapSentRequestIdToResponseReader:       make(map[uint64]*ResponseReader),
		mapSentSubIdToSubDataReader:            make(map[uint64]*SubscriptionDataReader),
		mapReceivedRequestMethodToHandler:      make(map[string]RequestHandlerFunction),
		mapReceivedSubscriptionMethodToHandler: make(map[string]SubscriptionRequestHandlerFunction),
		mapReceivedReqIdToRequestInfo:          make(map[uint64]*requestInfo),
		mapReceivedSubIdToSubscriptionInfo:     make(map[uint64]*subscriptionInfo),
		connRestoredCallback:                   connRestoredCallback,
		connFailedCallback:                     connFailedCallback,
		resetOnce:                              &sync.Once{},
	}

	//add request handlers, if any
	for _, reqHandler := range requestHandlers {
		wsConnector.RegisterRequestHandler(reqHandler.Method, reqHandler.Handler)
	}
	for _, subReqHandler := range subscriptionRequestHandlers {
		wsConnector.RegisterSubscriptionRequestHandler(subReqHandler.Topic, subReqHandler.Handler)
	}

	//open ws connection (for clients only)
	err := wsConnector.openClientWsConnection()
	if err != nil {
		return nil, err
	}

	wsConnector.startGoroutines()

	return &wsConnector, nil
}

func NewServerConnectorWithDefaultParameters(wsConn *websocket.Conn, requestHandlers []*RequestHandlerInfo, subscriptionRequestHandlers []*SubscriptionRequestHandlerInfo, logTag string, connFailedCallback func(failedConnector Connector)) ServerConnector {
	return NewServerConnector(wsConn, requestHandlers, subscriptionRequestHandlers, 100, 1000, 100, 50, logTag, connFailedCallback)
}

func NewServerConnector(wsConn *websocket.Conn, requestHandlers []*RequestHandlerInfo, subscriptionRequestHandlers []*SubscriptionRequestHandlerInfo, incomingMsgChanBufferSize, outgoingMsgChanBufferSize, responseChanBufferSize, subscriptionRequestChanBufferSize int, logTag string, connFailedCallback func(failedConnector Connector)) ServerConnector {
	//instantiate the websocket connector
	wsConnector := websocketConnector{
		logTag:                                 logTag,
		wsConn:                                 wsConn,
		responseChanBufferSize:                 responseChanBufferSize,
		subscriptionRequestChanBufferSize:      subscriptionRequestChanBufferSize,
		incomingMsgChanBufferSize:              incomingMsgChanBufferSize,
		outgoingMsgChanBufferSize:              outgoingMsgChanBufferSize,
		incomingWsMsgChan:                      make(chan *wsReceivedMessage, incomingMsgChanBufferSize),
		outgoingWsMsgChan:                      make(chan *wsSentMessage, outgoingMsgChanBufferSize),
		mapSentRequestIdToResponseReader:       make(map[uint64]*ResponseReader),
		mapSentSubIdToSubDataReader:            make(map[uint64]*SubscriptionDataReader),
		mapReceivedRequestMethodToHandler:      make(map[string]RequestHandlerFunction),
		mapReceivedSubscriptionMethodToHandler: make(map[string]SubscriptionRequestHandlerFunction),
		mapReceivedReqIdToRequestInfo:          make(map[uint64]*requestInfo),
		mapReceivedSubIdToSubscriptionInfo:     make(map[uint64]*subscriptionInfo),
		connFailedCallback:                     connFailedCallback,
		resetOnce:                              &sync.Once{},
	}

	//add request handlers, if any
	for _, reqHandler := range requestHandlers {
		wsConnector.RegisterRequestHandler(reqHandler.Method, reqHandler.Handler)
	}
	for _, subReqHandler := range subscriptionRequestHandlers {
		wsConnector.RegisterSubscriptionRequestHandler(subReqHandler.Topic, subReqHandler.Handler)
	}

	wsConnector.startGoroutines()

	return &wsConnector
}

func (wsc *websocketConnector) openClientWsConnection() error {
	var reqHeader http.Header = nil
	if wsc.authTokenGenerator != nil {
		headerNames, headerValues := wsc.authTokenGenerator()
		reqHeader = http.Header{}
		for i := range headerNames {
			reqHeader.Add(headerNames[i], headerValues[i])
		}
	}

	//connect to websocket
	var err error
	wsc.wsConn, _, err = websocket.DefaultDialer.Dial(wsc.wsUrl, reqHeader)
	if err != nil {
		return fmt.Errorf("error in websocket.DefaultDialer.Dial(wsUrl, nil): %s", err)
	}

	return nil
}

func (wsc *websocketConnector) startGoroutines() {
	go wsc.incomingWsMessageHandler()
	go wsc.outgoingWsMessageWriter()
	go wsc.incomingWsMessageReader()
}

func (wsc *websocketConnector) incomingWsMessageReader() {
	wsc.goroutinesActiveLock.RLock()
	defer wsc.goroutinesActiveLock.RUnlock()

	var msgBytes []byte
	var err error

	for {
		//read the next message from the websocket
		_, msgBytes, err = wsc.wsConn.ReadMessage()
		if err != nil {
			log.Warningf("[%s][WsReader] Error in wsc.wsConn.ReadMessage(): %s | Triggering reset procedure...\n", wsc.logTag, err)

			//start the reset procedure (in a separate goroutine, because the reset procedure requires all three main goroutines of the connector to be closed)
			go wsc.resetOnce.Do(wsc.reset)

			//close the incoming messages channel, so the incomingWsMessageHandler goroutine will return too
			close(wsc.incomingWsMsgChan)

			//kill this goroutine
			return
		}

		log.Tracef("[%s][WsReader] Received ws msg: %s\n", wsc.logTag, msgBytes)

		//unmarshal message
		var msg wsReceivedMessage
		err = jsoniter.ConfigFastest.Unmarshal(msgBytes, &msg)
		if err != nil {
			log.Warningf("[%s][WsReader] Error in jsoniter.Unmarshal(msgBytes, &msg): %s\n", wsc.logTag, err)
			continue //skip this message
		}

		//send message to handler goroutine
		wsc.incomingWsMsgChan <- &msg
	}
}

func (wsc *websocketConnector) incomingWsMessageHandler() {
	wsc.goroutinesActiveLock.RLock()
	defer wsc.goroutinesActiveLock.RUnlock()

	var msg *wsReceivedMessage
	var responseReader *ResponseReader
	var subDataReader *SubscriptionDataReader
	var subHandler SubscriptionRequestHandlerFunction
	var reqHandler RequestHandlerFunction
	var subscriptionToUnsubscribe, prevActiveSubInfo *subscriptionInfo
	var exists bool
	var chanOpen bool

	for {
		//get next message from peer
		msg, chanOpen = <-wsc.incomingWsMsgChan
		if !chanOpen { //if the incomingWsMessageReader goroutine has died
			//the incomingWsMsgChan is closed only by the incomingWsMessageReader goroutine, which already triggers the
			//reset procedure if needed, so here we just have to kill this goroutine
			return
		}

		switch msg.Type {
		case request:
			//check reqId uniqueness
			wsc.mapReceivedReqIdToRequestInfoLock.RLock()
			_, exists = wsc.mapReceivedReqIdToRequestInfo[msg.Id]
			wsc.mapReceivedReqIdToRequestInfoLock.RUnlock()
			if exists { //if the reqId already exists
				log.Warningf("[%s][IncomingWsMsgHandler] Received request with same reqId of an active previous request: %+v\n", wsc.logTag, msg)
				if msg.Id != 0 { //if a response is required
					//send an error response
					wsc.outgoingWsMsgChan <- &wsSentMessage{
						Type:    response,
						Id:      msg.Id,
						Method:  msg.Method,
						Message: duplicateReqIdMessage,
					}
				}

			} else { //if the reqId is valid (if it's "new", if it doesn't exist in the map yet)
				wsc.mapReceivedRequestMethodToHandlerLock.RLock()
				reqHandler, exists = wsc.mapReceivedRequestMethodToHandler[msg.Method]
				wsc.mapReceivedRequestMethodToHandlerLock.RUnlock()
				if exists { //if there is a handler for this method
					if msg.Id != 0 { //if a response is required
						//create a requestInfo object
						reqInfo := &requestInfo{
							requestReader: &RequestReader{rawReqMsg: msg.Message},
							responder: &wsResponder{
								wsConnector: wsc,
								reqId:       msg.Id,
								method:      msg.Method,
							},
						}

						//store the request info
						wsc.mapReceivedReqIdToRequestInfoLock.Lock()
						wsc.mapReceivedReqIdToRequestInfo[msg.Id] = reqInfo
						wsc.mapReceivedReqIdToRequestInfoLock.Unlock()

						//pass the request to the handler
						go reqHandler(reqInfo.responder, reqInfo.requestReader)

					} else { //if no response is required (fire&forget)
						//just pass a temporary responder and request reader with the request data to the handler
						//the temporary responder will contain the reference to the Connector, but sending a
						//response through this temporary responder will return an error.
						go reqHandler(&wsResponder{wsConnector: wsc, reqId: 0}, &RequestReader{rawReqMsg: msg.Message})
					}

				} else { //if no handler exists for the method
					log.Warningf("[%s][IncomingWsMsgHandler] Received request for unknown method: %+v\n", wsc.logTag, msg)
					if msg.Id != 0 { //if a response is required
						//send an error response
						wsc.outgoingWsMsgChan <- &wsSentMessage{
							Type:    response,
							Id:      msg.Id,
							Method:  msg.Method,
							Message: unknownMethodMessage,
						}
					}
				}
			}

		case subscriptionRequest:
			if msg.Id == 0 { //if the subId is not valid
				log.Warningf("[%s][IncomingWsMsgHandler] Received subscription request with invalid subId (subscription request message ID must be != 0): %+v\n", wsc.logTag, msg)
				continue
			}

			//if the subId is valid

			//check if this message is related to a previous, still active, subscription
			wsc.mapReceivedSubIdToSubscriptionInfoLock.RLock()
			prevActiveSubInfo, exists = wsc.mapReceivedSubIdToSubscriptionInfo[msg.Id]
			wsc.mapReceivedSubIdToSubscriptionInfoLock.RUnlock()
			if exists { //if this is a subsequent message of a previous, still active, subscription
				//send this message to the already active handler through the appropriate channel
				prevActiveSubInfo.subscriptionRequestReader.rawSubscriptionRequestDataChan <- msg.Message

			} else { //if this is a new subscription request
				wsc.mapReceivedSubscriptionMethodToHandlerLock.RLock()
				subHandler, exists = wsc.mapReceivedSubscriptionMethodToHandler[msg.Method]
				wsc.mapReceivedSubscriptionMethodToHandlerLock.RUnlock()
				if exists { //if there is a handler for this method
					//create a subscriptionInfo object
					subInfo := &subscriptionInfo{
						subscriptionRequestReader: &SubscriptionRequestReader{
							rawSubscriptionRequestDataChan:         make(chan json.RawMessage, wsc.subscriptionRequestChanBufferSize),
							typedSubscriptionRequestChanBufferSize: wsc.subscriptionRequestChanBufferSize,
						},
						sender: &wsSender{
							wsConnector:  wsc,
							subId:        msg.Id,
							topic:        msg.Method,
							customFields: NewMap(),
						},
					}

					//store the request info
					wsc.mapReceivedSubIdToSubscriptionInfoLock.Lock()
					wsc.mapReceivedSubIdToSubscriptionInfo[msg.Id] = subInfo
					wsc.mapReceivedSubIdToSubscriptionInfoLock.Unlock()

					//send the initial request data into the subscription request data channel
					subInfo.subscriptionRequestReader.rawSubscriptionRequestDataChan <- msg.Message

					//pass the request to the handler
					go subHandler(subInfo.sender, subInfo.subscriptionRequestReader)

				} else { //if no handler exists for the method
					log.Warningf("[%s][IncomingWsMsgHandler] Received subscription request for unknown method: %+v\n", wsc.logTag, msg)
					//send an error response
					wsc.outgoingWsMsgChan <- &wsSentMessage{
						Type:    subscriptionData,
						Id:      msg.Id,
						Method:  msg.Method,
						Last:    true,
						Message: unknownTopicMessage,
					}
				}
			}

		case response:
			if msg.Id == 0 { //the id field is set to 0 by go when there is no id in the json, so when the message does not require a response (that's why it shouldn't happen in a response)
				log.Warningf("[%s][IncomingWsMsgHandler] Received response with id = 0, ignoring it... | msg: %+v\n", wsc.logTag, msg)
				continue
			}

			//if the received message id has an associated response channel in the map
			wsc.mapSentRequestIdToResponseReaderLock.RLock()
			responseReader, exists = wsc.mapSentRequestIdToResponseReader[msg.Id]
			wsc.mapSentRequestIdToResponseReaderLock.RUnlock()
			if exists {
				//send response on the response channel
				if msg.Message != nil { //but only if the message is not nil
					responseReader.rawResponseChan <- msg.Message
				}

				//close the channels and remove the response reader from the map
				responseReader.closeChannel()
				wsc.mapSentRequestIdToResponseReaderLock.Lock()
				delete(wsc.mapSentRequestIdToResponseReader, msg.Id)
				wsc.mapSentRequestIdToResponseReaderLock.Unlock()

			} else {
				log.Warningf("[%s][IncomingWsMsgHandler] Received response for reqId not in map, ignoring it... | msg: %+v\n", wsc.logTag, msg)
			}

		case subscriptionData:
			if msg.Id == 0 { //the id field is set to 0 by go when there is no id in the json, so when the message does not require a response (that's why it shouldn't happen in a response/subscriptionData)
				log.Warningf("[%s][IncomingWsMsgHandler] Received data with id = 0, ignoring it... | msg: %+v\n", wsc.logTag, msg)
				continue
			}

			//if the received message id has an associated response channel in the map
			wsc.mapSentSubIdToSubDataReaderLock.RLock()
			subDataReader, exists = wsc.mapSentSubIdToSubDataReader[msg.Id]
			wsc.mapSentSubIdToSubDataReaderLock.RUnlock()
			if exists {
				//send data on the data channel
				if msg.Message != nil { //but only if the message is not nil
					subDataReader.rawDataChan <- msg.Message
				}

				if msg.Last { //if this is marked as the last message of this subscription
					//close the channels and remove the subscription data reader from the map
					subDataReader.closeChannel()
					wsc.mapSentSubIdToSubDataReaderLock.Lock()
					delete(wsc.mapSentSubIdToSubDataReader, msg.Id)
					wsc.mapSentSubIdToSubDataReaderLock.Unlock()
				}

			} else {
				log.Warningf("[%s][IncomingWsMsgHandler] Received data for subId not in map (this is normal if the message was sent very close to an Unsubscribe() call on the receiver's side) ignoring it... | msg: %+v\n", wsc.logTag, msg)
			}

		case unsubscriptionRequest:
			if msg.Id == 0 {
				//the id field is set to 0 by go when there is no id in the json, and an
				//unsubscriptionRequest with no id to unsubscribe is invalid.
				log.Warningf("[%s][IncomingWsMsgHandler] Received unsubscription request with id = 0, ignoring it... | msg: %+v\n", wsc.logTag, msg)
				continue
			}

			//the message id, for unsubscription requests, is the subId to unsubscribe
			wsc.mapReceivedSubIdToSubscriptionInfoLock.RLock()
			subscriptionToUnsubscribe, exists = wsc.mapReceivedSubIdToSubscriptionInfo[msg.Id]
			wsc.mapReceivedSubIdToSubscriptionInfoLock.RUnlock()
			if exists {
				//disable the sender
				subscriptionToUnsubscribe.sender.disable()

				//remove the subscription info from the wsConnector's map
				wsc.removeSubscriptionRequestInfo(msg.Id, true)

			} else {
				log.Warningf("[%s][IncomingWsMsgHandler] Received unsubscribe request for unknown subId: %+v\n", wsc.logTag, msg)
			}

		default:
			log.Warningf("[%s][IncomingWsMsgHandler] Received unknown message type: %d\n", wsc.logTag, msg.Type)
		}
	}
}

func (wsc *websocketConnector) outgoingWsMessageWriter() {
	wsc.goroutinesActiveLock.RLock()
	defer wsc.goroutinesActiveLock.RUnlock()

	var msg *wsSentMessage
	var msgBytes []byte
	var err error

	for {
		//get next message that has to be sent to the peer
		msg = <-wsc.outgoingWsMsgChan
		if msg == nil { //if the reset procedure wants to kill this goroutine
			//nil is sent on the outgoingWsMsgChan only by the reset procedure (this means it's already running, so we
			//don't need to trigger it here, we just have to kill this goroutine)
			//note that msg is the message read from the outgoingWsMsgChan, so it's the wrapped wsMessage
			//(with the "envelope", not just the payload, it must always be != nil for actual messages to send)
			return
		}

		msgBytes, err = jsoniter.ConfigFastest.Marshal(msg)
		if err != nil {
			log.Warningf("[%s][OutgoingWsMsgHandler] Error in jsoniter.Marshal(msg): %s | skipping this message...\n", wsc.logTag, err)
			continue
		}

		log.Tracef("[%s][OutgoingWsMsgHandler] Sending ws msg: %s\n", wsc.logTag, msgBytes)

		err = wsc.wsConn.WriteMessage(websocket.TextMessage, msgBytes)
		if err != nil {
			log.Warningf("[%s][OutgoingWsMsgHandler] Error in wsc.wsConn.WriteMessage(websocket.TextMessage, msgBytes): %s | Triggering reset procedure...\n", wsc.logTag, err)

			//start the reset procedure (in a separate goroutine, because the reset procedure requires all three main goroutines of the connector to be closed)
			go wsc.resetOnce.Do(wsc.reset)

			//kill this goroutine
			return
		}
	}
}

func (wsc *websocketConnector) getNextRequestId() uint64 {
	wsc.nextReqIdLock.Lock()
	defer wsc.nextReqIdLock.Unlock()

	//the following is safe even in case of overflow of the uint64 reqId variable
	wsc.nextReqId++
	wsc.mapSentRequestIdToResponseReaderLock.RLock()
	for { //if the reqId is not available (or not valid, i.e. == 0), keep incrementing it until an available one is found
		if wsc.nextReqId != 0 {
			_, exists := wsc.mapSentRequestIdToResponseReader[wsc.nextReqId]
			if !exists {
				break
			}
		}
		wsc.nextReqId++
	}
	wsc.mapSentRequestIdToResponseReaderLock.RUnlock()

	return wsc.nextReqId
}

func (wsc *websocketConnector) getNextSubscriptionId() uint64 {
	wsc.nextSubIdLock.Lock()
	defer wsc.nextSubIdLock.Unlock()

	//the following is safe even in case of overflow of the uint64 subId variable
	wsc.nextSubId++
	wsc.mapSentSubIdToSubDataReaderLock.RLock()
	for { //if the subId is not available (or not valid, i.e. == 0), keep incrementing it until an available one is found
		if wsc.nextSubId != 0 {
			_, exists := wsc.mapSentSubIdToSubDataReader[wsc.nextSubId]
			if !exists {
				break
			}
		}
		wsc.nextSubId++
	}
	wsc.mapSentSubIdToSubDataReaderLock.RUnlock()

	return wsc.nextSubId
}

func (wsc *websocketConnector) SendRequest(method string, data interface{}, requireResponse bool) (*ResponseReader, error) {
	//process outgoing requests only if the connection is active (and delay reset procedure if someone is sending a request)
	if wsc.ongoingResetLock.TryRLock() {
		defer wsc.ongoingResetLock.RUnlock()

		if requireResponse { //requires a response
			//get a unique req id
			reqId := wsc.getNextRequestId()

			//register response reader
			responseInfo := &ResponseReader{
				connectorLogTag: wsc.logTag,
				rawResponseChan: make(chan json.RawMessage, 1),
			}
			wsc.mapSentRequestIdToResponseReaderLock.Lock()
			wsc.mapSentRequestIdToResponseReader[reqId] = responseInfo
			wsc.mapSentRequestIdToResponseReaderLock.Unlock()

			//send message to the outgoing messages handler
			wsc.outgoingWsMsgChan <- &wsSentMessage{
				Type:   request,
				Id:     reqId,
				Method: method,
				Message: &Message[interface{}, error]{
					Data: data,
				},
			}

			return responseInfo, nil

		} else { //fire and forget
			//just send the message to the outgoing messages handler, without specifying an ID
			wsc.outgoingWsMsgChan <- &wsSentMessage{
				Type:   request,
				Method: method,
				Message: &Message[interface{}, error]{
					Data: data,
				},
			}

			return nil, nil
		}

	} else { //if there is an ongoing reset procedure
		return nil, WsConnectionDownError
	}
}

func (wsc *websocketConnector) PersistentSubscribe(topic string, data interface{}) (*SubscriptionDataReader, error) {
	if wsc.wsUrl == "" { //if this is a server websocket connector
		//persistent subscriptions are not allowed by server websocket connector, return an error
		return nil, fmt.Errorf("persistent subscriptions are not allowed by server websocket connectors")
	}

	//if this is a client websocket connector, call the actual subscribe method
	return wsc.subscribe(topic, true, data)
}

func (wsc *websocketConnector) Subscribe(topic string, data interface{}) (*SubscriptionDataReader, error) {
	//for non-persistent subscriptions, just call the actual subscribe method with the "restore" flag set to false
	return wsc.subscribe(topic, false, data)
}

func (wsc *websocketConnector) subscribe(topic string, restoreSubscriptionOnReconnection bool, data interface{}) (*SubscriptionDataReader, error) {
	//process outgoing requests only if the connection is active (and delay reset procedure if someone is sending a request)
	if wsc.ongoingResetLock.TryRLock() {
		defer wsc.ongoingResetLock.RUnlock()

		//get a unique subscription id
		subId := wsc.getNextSubscriptionId()

		//build the subscription request message
		subReqMsg := &Message[interface{}, error]{
			Data: data,
		}

		//register subscription data reader
		subDataReader := &SubscriptionDataReader{
			subId:                   subId,
			wsConnector:             wsc,
			topic:                   topic,
			rawDataChan:             make(chan json.RawMessage, wsc.responseChanBufferSize),
			typedDataChanBufferSize: wsc.responseChanBufferSize,
		}
		if restoreSubscriptionOnReconnection { //if this is a persistent subscription
			subDataReader.persistent = true                      //set the persistent flag
			subDataReader.lastSubscriptionRequestMsg = subReqMsg //store the subscription request too
		}
		wsc.mapSentSubIdToSubDataReaderLock.Lock()
		wsc.mapSentSubIdToSubDataReader[subId] = subDataReader
		wsc.mapSentSubIdToSubDataReaderLock.Unlock()

		//send message to the outgoing messages handler
		wsc.outgoingWsMsgChan <- &wsSentMessage{
			Type:    subscriptionRequest,
			Id:      subId,
			Method:  topic,
			Message: subReqMsg,
		}

		return subDataReader, nil

	} else { //if there is an ongoing reset procedure
		return nil, WsConnectionDownError
	}
}

func (wsc *websocketConnector) UpdateSubscription(subDataReader *SubscriptionDataReader, data interface{}) error {
	//process outgoing subscription update requests only if the connection is active (and delay reset procedure if someone is sending a subscription update request)
	if wsc.ongoingResetLock.TryRLock() {
		defer wsc.ongoingResetLock.RUnlock()

		//build the subscription request message
		subReqMsg := &Message[interface{}, error]{
			Data: data,
		}

		wsc.mapSentSubIdToSubDataReaderLock.RLock()
		_, exists := wsc.mapSentSubIdToSubDataReader[subDataReader.subId]
		wsc.mapSentSubIdToSubDataReaderLock.RUnlock()
		if exists { //if the specified subId actually belongs to a registered subscription
			if subDataReader.persistent { //if this is a persistent subscription
				//update the latest subscription request data, so the most recent subscription request is available in case of reconnection
				subDataReader.lastSubscriptionRequestMsg = subReqMsg
			}

			//send message to the outgoing messages handler
			wsc.outgoingWsMsgChan <- &wsSentMessage{
				Type:    subscriptionRequest,
				Id:      subDataReader.subId,
				Message: subReqMsg,
			}
		}

		return nil

	} else { //if there is an ongoing reset procedure
		return WsConnectionDownError
	}
}

func (wsc *websocketConnector) Unsubscribe(subDataReader *SubscriptionDataReader) error {
	//process outgoing unsubscription requests only if the connection is active (and delay reset procedure if someone is sending an unsubscription request)
	if wsc.ongoingResetLock.TryRLock() {
		defer wsc.ongoingResetLock.RUnlock()

		wsc.mapSentSubIdToSubDataReaderLock.RLock()
		_, exists := wsc.mapSentSubIdToSubDataReader[subDataReader.subId]
		wsc.mapSentSubIdToSubDataReaderLock.RUnlock()
		if exists { //if the specified subId actually belongs to a registered subscription
			//close the channels and remove the subscription data reader from the map
			subDataReader.closeChannel()
			wsc.mapSentSubIdToSubDataReaderLock.Lock()
			delete(wsc.mapSentSubIdToSubDataReader, subDataReader.subId)
			wsc.mapSentSubIdToSubDataReaderLock.Unlock()

			//send the unsubscribe request message to the outgoing messages handler and set the unsubscribing flag
			subDataReader.unsubscribing = true
			wsc.outgoingWsMsgChan <- &wsSentMessage{Type: unsubscriptionRequest, Id: subDataReader.subId}
		}

		return nil

	} else { //if there is an ongoing reset procedure
		return WsConnectionDownError
	}
}

func (wsc *websocketConnector) UnsubscribeAll() error {
	//process outgoing unsubscription requests only if the connection is active (and delay reset procedure if someone is sending an unsubscription request)
	if wsc.ongoingResetLock.TryRLock() {
		defer wsc.ongoingResetLock.RUnlock()

		wsc.mapSentSubIdToSubDataReaderLock.Lock()
		for subId, subDataReader := range wsc.mapSentSubIdToSubDataReader {
			//close the channels and remove the subscription data reader from the map
			subDataReader.closeChannel()
			delete(wsc.mapSentSubIdToSubDataReader, subId)

			//send the unsubscribe request message to the outgoing messages handler and set the unsubscribing flag
			subDataReader.unsubscribing = true
			wsc.outgoingWsMsgChan <- &wsSentMessage{Type: unsubscriptionRequest, Id: subId}
		}
		wsc.mapSentSubIdToSubDataReaderLock.Unlock()

		return nil

	} else { //if there is an ongoing reset procedure
		return WsConnectionDownError
	}
}

func (wsc *websocketConnector) RegisterRequestHandler(method string, handler RequestHandlerFunction) {
	wsc.mapReceivedRequestMethodToHandlerLock.Lock()
	defer wsc.mapReceivedRequestMethodToHandlerLock.Unlock()
	wsc.mapReceivedRequestMethodToHandler[method] = handler
}

func (wsc *websocketConnector) RemoveRequestHandler(method string) {
	wsc.mapReceivedRequestMethodToHandlerLock.Lock()
	defer wsc.mapReceivedRequestMethodToHandlerLock.Unlock()
	_, exists := wsc.mapReceivedRequestMethodToHandler[method]
	if exists {
		delete(wsc.mapReceivedRequestMethodToHandler, method)
	}
}

func (wsc *websocketConnector) RegisterSubscriptionRequestHandler(method string, handler SubscriptionRequestHandlerFunction) {
	wsc.mapReceivedSubscriptionMethodToHandlerLock.Lock()
	defer wsc.mapReceivedSubscriptionMethodToHandlerLock.Unlock()
	wsc.mapReceivedSubscriptionMethodToHandler[method] = handler
}

func (wsc *websocketConnector) RemoveSubscriptionRequestHandler(method string) {
	wsc.mapReceivedSubscriptionMethodToHandlerLock.Lock()
	defer wsc.mapReceivedSubscriptionMethodToHandlerLock.Unlock()
	_, exists := wsc.mapReceivedSubscriptionMethodToHandler[method]
	if exists {
		delete(wsc.mapReceivedSubscriptionMethodToHandler, method)
	}
}

func (wsc *websocketConnector) removeRequestInfo(reqId uint64, lock bool) {
	if lock {
		wsc.mapReceivedReqIdToRequestInfoLock.Lock()
		defer wsc.mapReceivedReqIdToRequestInfoLock.Unlock()
	}

	//remove the request info from the map
	delete(wsc.mapReceivedReqIdToRequestInfo, reqId)
}

func (wsc *websocketConnector) removeSubscriptionRequestInfo(subId uint64, lock bool) {
	if lock {
		wsc.mapReceivedSubIdToSubscriptionInfoLock.Lock()
		defer wsc.mapReceivedSubIdToSubscriptionInfoLock.Unlock()
	}

	//close the channels and remove the request info from the map
	wsc.mapReceivedSubIdToSubscriptionInfo[subId].subscriptionRequestReader.closeChannel()
	delete(wsc.mapReceivedSubIdToSubscriptionInfo, subId)
}

func (wsc *websocketConnector) Close() {
	//set the closing flag, to differentiate a connection error from an explicit call to .Close()
	wsc.closing = true

	//wait for any outgoing request to be registered in the maps and sent on the outgoingWsMsgChan,
	//this lock operation will block any subsequent outgoing requests, so we can safely close everything
	wsc.ongoingResetLock.Lock()
	//do not defer the unlock, since this wsConnector is being closed and can't be re-used

	//close the websocket connection (this will kill the incomingWsMessageReader goroutine, and also trigger the reset
	//procedure, with a different behavior since the wsc.closing flag has been set)
	if wsc.wsConn != nil {
		_ = wsc.wsConn.Close()
	}
}

// GetLogTag returns the connector's log tag
func (wsc *websocketConnector) GetLogTag() string {
	return wsc.logTag
}

func (wsc *websocketConnector) reset() {
	//note that the closing flag may change during the execution of this reset procedure (for example, if Close() is called right after the connection goes down)
	if !wsc.closing { //if this reset procedure is being executed due to a connection error
		//close the websocket connection (this will kill the incomingWsMessageReader goroutine, if it's still active)
		//no need to close the websocket connection if the connector is closing, since the connector's .Close() method
		//already closes the connection (in fact, that's how the reset procedure is triggered when closing a connector)
		if wsc.wsConn != nil {
			_ = wsc.wsConn.Close()
		}

		//we want to call the connFailedCallback or connRestoredCallback as the last thing we do in the reset
		//procedure, after the ongoingResetLock has been released.
		//To achieve this we have to call it with the first defer statements, because defer is
		//LIFO, so the first defer of a function is the last one called when the function returns.
		if wsc.wsUrl == "" { //server connector
			if wsc.connFailedCallback != nil { //if a callback has been specified
				//call it (in a separate goroutine) when the function returns
				defer func() {
					if !wsc.closing {
						go wsc.connFailedCallback(wsc)
					}
				}()
			}

		} else { //client connector
			if wsc.connFailedCallback != nil { //if a conn failed callback has been specified
				//call it immediately (in a separate goroutine)
				go wsc.connFailedCallback(wsc)
			}
			if wsc.connRestoredCallback != nil { //if a conn restored callback has been specified
				//call it (in a separate goroutine) when the function returns
				defer func() {
					if !wsc.closing {
						go wsc.connRestoredCallback(wsc)
					}
				}()
			}
		}

		//wait for any outgoing request to be registered in the maps and sent on the outgoingWsMsgChan,
		//this lock operation will block any subsequent outgoing requests, so we can safely reset everything.
		//this lock has to be taken (and released) only for "resets", it's already taken for calls to .Close()
		wsc.ongoingResetLock.Lock()
		defer wsc.ongoingResetLock.Unlock()
	}

	//before returning, the incomingWsMessageReader goroutine will close incomingWsMsgChan, which will kill the incomingWsMessageHandler
	//goroutine too, so there's no need to do that here

	//since we don't know whether there is anything on the outgoingWsMsgChan that the outgoingWsMessageWriter can read,
	//we'll just send a nil on it. If the outgoingWsMessageWriter is waiting to read from the channel, it will detect
	//the nil and return. If it's handling a previous outgoing message it will detect an error on the WriteMessage, since
	//the connection is now closed. Either way it will close itself.
	wsc.outgoingWsMsgChan <- nil

	//wait for the three goroutines (incomingWsMessageReader, incomingWsMessageHandler and outgoingWsMessageWriter) to be closed
	//(once the lock has been acquired, it means that all goroutines are closed, so we can unlock it immediately)
	wsc.goroutinesActiveLock.Lock()
	wsc.goroutinesActiveLock.Unlock()

	//at this point we are sure that no one will send anything on outgoingWsMsgChan, so we can close it
	close(wsc.outgoingWsMsgChan)

	wsc.closeSendersRespondersAndReaders()

	if !wsc.closing { //if this is a "reset" procedure and not the result of a call to .Close()
		//create a new resetOnce to allow a subsequent reset procedure
		wsc.resetOnce = &sync.Once{}

		if wsc.wsUrl == "" { //if this is a server websocket connector
			log.Warningf("[%s][ResetProcedure] Destroying connector\n", wsc.logTag)

		} else { //if this is a client websocket connector
			log.Warningf("[%s][ResetProcedure] Re-connecting...\n", wsc.logTag)

			//remake the internal channels, to discard all previous unprocessed messages, if any
			wsc.incomingWsMsgChan = make(chan *wsReceivedMessage, wsc.incomingMsgChanBufferSize)
			wsc.outgoingWsMsgChan = make(chan *wsSentMessage, wsc.outgoingMsgChanBufferSize)

			//open ws connection
			err := wsc.openClientWsConnection()
			for err != nil {
				log.Warningf("[%s][ResetProcedure] Error in wsc.openClientWsConnection(): %s | retrying in %d seconds...\n", wsc.logTag, err, wsc.secondsBetweenReconnections)

				//wait before reconnection
				time.Sleep(time.Duration(wsc.secondsBetweenReconnections) * time.Second)

				if wsc.closing { //if during the sleep the closing flag was set to true
					wsc.closeSendersRespondersAndReaders() //make sure to close all senders, responders and readers
					return                                 //destroy connector
				}

				//open ws connection
				err = wsc.openClientWsConnection()
			}

			//once websocket has been opened successfully, start goroutines
			wsc.startGoroutines()

			//restore persistent subscriptions (note that the only subscriptions that remain in mapSentSubIdToSubDataReader
			//are the persistent ones, since we removed the standard ones earlier in the reset procedure)
			wsc.mapSentSubIdToSubDataReaderLock.RLock()
			for subId, subDataReader := range wsc.mapSentSubIdToSubDataReader {
				if !subDataReader.unsubscribing { //only if not unsubscribing
					//send subscription request message to the outgoing messages handler, with the same id, method and data as
					//the latest subscription update of original subscription
					wsc.outgoingWsMsgChan <- &wsSentMessage{
						Type:    subscriptionRequest,
						Id:      subId,
						Method:  subDataReader.topic,
						Message: subDataReader.lastSubscriptionRequestMsg,
					}
				}
			}
			wsc.mapSentSubIdToSubDataReaderLock.RUnlock()

			log.Warningf("[%s][ResetProcedure] Re-connected...\n", wsc.logTag)
		}
	}
}

func (wsc *websocketConnector) closeSendersRespondersAndReaders() {
	//close all channels for responses to previously sent requests and delete them from the map
	//(we can close them because, now that the incomingMsgHandler goroutine is down, no one will send on those channels)
	wsc.mapSentRequestIdToResponseReaderLock.Lock()
	for sentReqId, responseReader := range wsc.mapSentRequestIdToResponseReader {
		responseReader.rawResponseChan <- marshaledWsConnectionDownMessage
		responseReader.closeChannel()
		delete(wsc.mapSentRequestIdToResponseReader, sentReqId)
	}
	wsc.mapSentRequestIdToResponseReaderLock.Unlock()

	//close all channels for responses to active standard subscriptions and delete them from the map
	//(we can close them because, now that the incomingMsgHandler goroutine is down, no one will send on those channels)
	wsc.mapSentSubIdToSubDataReaderLock.Lock()
	for subId, subDataReader := range wsc.mapSentSubIdToSubDataReader {
		//send the connection down error to response readers of both standard and persistent subscriptions
		subDataReader.rawDataChan <- marshaledWsConnectionDownMessage

		//destroy standard subscriptions only if this is a reset procedure (but if the connector has been closed, destroy persistent subscriptions too)
		if !subDataReader.persistent || wsc.closing {
			subDataReader.closeChannel()
			delete(wsc.mapSentSubIdToSubDataReader, subId)
		}
	}
	wsc.mapSentSubIdToSubDataReaderLock.Unlock()

	//disable all active responders for previously received requests
	wsc.mapReceivedReqIdToRequestInfoLock.Lock()
	for receivedReqId, reqInfo := range wsc.mapReceivedReqIdToRequestInfo {
		//disable the responder (from now on, no more responses will be sent through this responder, even if the request handler tries to)
		reqInfo.responder.disable()

		//remove the request info object from the map
		wsc.removeRequestInfo(receivedReqId, false) //do not take the mapReceivedReqIdToRequestInfoLock here, since we already locked it outside this for loop
	}
	wsc.mapReceivedReqIdToRequestInfoLock.Unlock()

	//disable all active senders for previously received subscriptions
	wsc.mapReceivedSubIdToSubscriptionInfoLock.Lock()
	for receivedSubId, subInfo := range wsc.mapReceivedSubIdToSubscriptionInfo {
		//disable the sender (from now on, no more responses will be sent through this responder, even if the request handler tries to)
		subInfo.sender.disable()

		//send the "connection down" error to the subscription request reader
		subInfo.subscriptionRequestReader.rawSubscriptionRequestDataChan <- marshaledWsConnectionDownMessage

		//remove the subscription info object from the map (and close its channels)
		wsc.removeSubscriptionRequestInfo(receivedSubId, false) //do not take the mapReceivedSubIdToSubscriptionInfoLock here, since we already locked it outside this for loop
	}
	wsc.mapReceivedSubIdToSubscriptionInfoLock.Unlock()
}
