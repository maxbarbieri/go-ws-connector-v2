package ws_connector

import (
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"sync"
)

/*
	RequestReader
*/

type RequestReader struct {
	rawReqMsg json.RawMessage
}

func (rr *RequestReader) GetRawRequestMessage() json.RawMessage {
	return rr.rawReqMsg
}

func GetTypedRequestMessage[RequestType any, ErrorType error](rr *RequestReader) *Message[RequestType, ErrorType] {
	var obj Message[RequestType, ErrorType]
	err := jsoniter.ConfigFastest.Unmarshal(rr.rawReqMsg, &obj)
	if err != nil {
		return &Message[RequestType, ErrorType]{
			Error: &Error[ErrorType]{
				ErrorLevel:   ConnectorLevel,
				ErrorMessage: fmt.Sprintf("error in jsoniter.Unmarshal(rr.rawReqMsg, &obj): %s", err),
			},
		}
	}
	return &obj
}

/*
	SubscriptionRequestReader
*/

type SubscriptionRequestReader struct {
	rawSubscriptionRequestDataChan         chan json.RawMessage
	channelRequested                       bool
	typedSubscriptionRequestChanBufferSize int
	channelClosed                          bool
	lock                                   sync.Mutex
}

func (srr *SubscriptionRequestReader) closeChannel() {
	srr.lock.Lock()
	defer srr.lock.Unlock()

	if !srr.channelClosed {
		close(srr.rawSubscriptionRequestDataChan)
		srr.channelClosed = true
	}
}

func (srr *SubscriptionRequestReader) GetRawSubscriptionRequestChannel() (chan json.RawMessage, error) {
	//use locks to avoid concurrent access to the SubscriptionRequestReader
	srr.lock.Lock()
	defer srr.lock.Unlock()

	if srr.channelRequested { //if the channel for this reader have already been requested
		return nil, RequestChannelAlreadyRequestedError
	}

	srr.channelRequested = true

	return srr.rawSubscriptionRequestDataChan, nil
}

func GetTypedSubscriptionRequestChannel[SubscriptionRequestType any, ErrorType error](srr *SubscriptionRequestReader) (chan *Message[SubscriptionRequestType, ErrorType], error) {
	//use locks to avoid concurrent access to the SubscriptionRequestReader
	srr.lock.Lock()
	defer srr.lock.Unlock()

	if srr.channelRequested { //if the channel for this reader have already been requested
		return nil, RequestChannelAlreadyRequestedError
	}

	srr.channelRequested = true

	//create typed subscription requests channel
	typedChan := make(chan *Message[SubscriptionRequestType, ErrorType], srr.typedSubscriptionRequestChanBufferSize)

	//create a goroutine that "translates" all incoming subscription requests
	go func() {
		for {
			subReqMsg, chanOpen := <-srr.rawSubscriptionRequestDataChan
			if chanOpen {
				var msgObj Message[SubscriptionRequestType, ErrorType]
				err := jsoniter.ConfigFastest.Unmarshal(subReqMsg, &msgObj)
				if err == nil { //no error
					typedChan <- &msgObj

				} else { //error
					typedChan <- &Message[SubscriptionRequestType, ErrorType]{
						Error: &Error[ErrorType]{
							ErrorLevel:   ConnectorLevel,
							ErrorMessage: fmt.Sprintf("error in jsoniter.Unmarshal(subReqMsg.Data, &msgObj): %s", err),
						},
					}
				}

			} else { //if requests channel is closed
				close(typedChan) //close the typed requests channel too
				return           //kill this goroutine
			}
		}
	}()

	return typedChan, nil
}

/*
	ResponseReader
*/

type ResponseReader struct {
	connectorLogTag  string
	method           string
	rawResponseChan  chan json.RawMessage
	channelRequested bool
	channelClosed    bool
	lock             sync.Mutex
}

func (rr *ResponseReader) closeChannel() {
	rr.lock.Lock()
	defer rr.lock.Unlock()

	if !rr.channelClosed {
		close(rr.rawResponseChan)
		rr.channelClosed = true
	}
}

// GetRawResponseChannel note that the channel returned by this function is closed by the WsConnector as soon as they're not needed anymore, handle channel open flag accordingly!
func (rr *ResponseReader) GetRawResponseChannel() (chan json.RawMessage, error) {
	//use locks to avoid concurrent access to the ResponseReader (from different goroutines calling GetTypedResponseChannel at the same time)
	rr.lock.Lock()
	defer rr.lock.Unlock()

	if rr.channelRequested { //if channels for this reader have already been requested
		return nil, ResponseChannelAlreadyRequestedError
	}

	rr.channelRequested = true

	return rr.rawResponseChan, nil
}

// GetRawResponse blocks until the response is received
func (rr *ResponseReader) GetRawResponse() (json.RawMessage, error) {
	respChan, err := rr.GetRawResponseChannel()
	if err != nil {
		return nil, fmt.Errorf("error in rr.GetRawResponseChannel(): %s", err)
	}

	return <-respChan, nil
}

// GetTypedResponseOnChannel does NOT automatically close the channel passed as parameters after the response has been received.
func GetTypedResponseOnChannel[ResponseType any, ErrorType error](rr *ResponseReader, typedResponseChan chan *Message[ResponseType, ErrorType]) {
	if typedResponseChan == nil {
		return
	}

	//use locks to avoid concurrent access to the ResponseReader (from different goroutines calling GetTypedResponseChannel at the same time)
	rr.lock.Lock()
	defer rr.lock.Unlock()

	if rr.channelRequested { //if channel for this reader have already been requested
		typedResponseChan <- &Message[ResponseType, ErrorType]{
			Error: &Error[ErrorType]{
				ErrorLevel:   ConnectorLevel,
				ErrorMessage: ResponseChannelAlreadyRequestedError.Error(),
			},
		}
		return
	}

	rr.channelRequested = true

	//create two goroutines that "translate" the incoming responses and errors

	go func() {
		//avoid crashing the entire process if the specified channels are closed when writing to it
		defer func() {
			if r := recover(); r != nil {
				log.Warningf("[%s][GetTypedResponseOnChannel] recovered from panic: %+v\n", rr.connectorLogTag, r)
			}
		}()

		var rawJsonResponse json.RawMessage
		var chanOpen bool
		for {
			rawJsonResponse, chanOpen = <-rr.rawResponseChan
			if chanOpen {
				var msgObj Message[ResponseType, ErrorType]
				err := jsoniter.ConfigFastest.Unmarshal(rawJsonResponse, &msgObj)
				if err == nil { //no error
					typedResponseChan <- &msgObj
				} else { //error
					typedResponseChan <- &Message[ResponseType, ErrorType]{
						Error: &Error[ErrorType]{
							ErrorLevel:   ConnectorLevel,
							ErrorMessage: fmt.Sprintf("error in jsoniter.Unmarshal(rawJsonResponse, &msgObj): %s", err),
						},
					}
				}

			} else { //if response channel is closed
				return //kill this goroutine
			}
		}
	}()
}

// GetTypedResponseChannel DOES automatically close the returned channels after the response has been received
func GetTypedResponseChannel[ResponseType any, ErrorType error](rr *ResponseReader) chan *Message[ResponseType, ErrorType] {
	//use locks to avoid concurrent access to the ResponseReader (from different goroutines calling GetTypedResponseChannel at the same time)
	rr.lock.Lock()
	defer rr.lock.Unlock()

	//create typed response channel
	typedResponseChan := make(chan *Message[ResponseType, ErrorType], 1)

	if rr.channelRequested { //if channels for this reader have already been requested
		typedResponseChan <- &Message[ResponseType, ErrorType]{
			Error: &Error[ErrorType]{
				ErrorLevel:   ConnectorLevel,
				ErrorMessage: ResponseChannelAlreadyRequestedError.Error(),
			},
		}
		return typedResponseChan
	}

	rr.channelRequested = true

	//create a goroutine that "translates" the incoming responses
	go func() {
		for {
			rawJsonResponse, chanOpen := <-rr.rawResponseChan
			if chanOpen {
				var msgObj Message[ResponseType, ErrorType]
				err := jsoniter.ConfigFastest.Unmarshal(rawJsonResponse, &msgObj)
				if err == nil { //no error
					typedResponseChan <- &msgObj

				} else { //error
					typedResponseChan <- &Message[ResponseType, ErrorType]{
						Error: &Error[ErrorType]{
							ErrorLevel:   ConnectorLevel,
							ErrorMessage: fmt.Sprintf("error in jsoniter.Unmarshal(rawJsonResponse, &msgObj): %s", err),
						},
					}
				}

			} else { //if response channel is closed
				close(typedResponseChan) //close the typed response channel too
				return                   //kill this goroutine
			}
		}
	}()

	return typedResponseChan
}

// GetTypedResponse blocks until the response is received
func GetTypedResponse[ResponseType any, ErrorType error](rr *ResponseReader) *Message[ResponseType, ErrorType] {
	return <-GetTypedResponseChannel[ResponseType, ErrorType](rr)
}

/*
	SubscriptionDataReader
*/

type SubscriptionDataReader struct {
	subId                      uint64
	wsConnector                *websocketConnector
	topic                      string
	lastSubscriptionRequestMsg *Message[interface{}, error]
	persistent                 bool
	unsubscribing              bool
	rawDataChan                chan json.RawMessage
	typedDataChanBufferSize    int
	channelRequested           bool
	channelClosed              bool
	lock                       sync.Mutex
}

func (sdr *SubscriptionDataReader) closeChannel() {
	sdr.lock.Lock()
	defer sdr.lock.Unlock()

	if !sdr.channelClosed {
		close(sdr.rawDataChan)
		sdr.channelClosed = true
	}
}

func (sdr *SubscriptionDataReader) GetRawSubscriptionDataChannel() (chan json.RawMessage, error) {
	//use locks to avoid concurrent access to the SubscriptionDataReader (from different goroutines calling GetTypedResponseChannel at the same time)
	sdr.lock.Lock()
	defer sdr.lock.Unlock()

	if sdr.channelRequested { //if channels for this reader have already been requested
		return nil, DataChannelAlreadyRequestedError
	}

	sdr.channelRequested = true

	return sdr.rawDataChan, nil
}

// GetTypedSubscriptionDataOnChannel does NOT automatically close the channels passed as parameters when the subscription is closed.
func GetTypedSubscriptionDataOnChannel[DataType any, ErrorType error](sdr *SubscriptionDataReader, typedDataChan chan *Message[DataType, ErrorType]) error {
	if typedDataChan == nil {
		return fmt.Errorf("typedDataChan can't be nil")
	}

	//use locks to avoid concurrent access to the SubscriptionDataReader (from different goroutines calling GetTypedResponseChannel at the same time)
	sdr.lock.Lock()
	defer sdr.lock.Unlock()

	if sdr.channelRequested { //if channels for this reader have already been requested
		return DataChannelAlreadyRequestedError
	}

	sdr.channelRequested = true

	//create two goroutines that "translate" all incoming data and errors

	go func() {
		//avoid crashing the entire process if the specified channels are closed when writing to it
		defer func() {
			if r := recover(); r != nil {
				log.Warningf("[%s][GetTypedSubscriptionDataOnChannel] recovered from panic: %+v\n", sdr.wsConnector.logTag, r)
			}
		}()

		var rawJsonData json.RawMessage
		var chanOpen bool
		for {
			rawJsonData, chanOpen = <-sdr.rawDataChan
			if chanOpen {
				var msgObj Message[DataType, ErrorType]
				err := jsoniter.ConfigFastest.Unmarshal(rawJsonData, &msgObj)
				if err == nil { //no error
					typedDataChan <- &msgObj

				} else { //error
					typedDataChan <- &Message[DataType, ErrorType]{
						Error: &Error[ErrorType]{
							ErrorLevel:   ConnectorLevel,
							ErrorMessage: fmt.Sprintf("error in jsoniter.Unmarshal(rawJsonData, &msgObj): %s", err),
						},
					}
				}

			} else { //if data channel is closed
				return //kill this goroutine
			}
		}
	}()

	return nil
}

// GetTypedSubscriptionDataChannel DOES automatically close the returned channel when the subscription is closed.
func GetTypedSubscriptionDataChannel[DataType any, ErrorType error](sdr *SubscriptionDataReader) (chan *Message[DataType, ErrorType], error) {
	//use locks to avoid concurrent access to the SubscriptionDataReader (from different goroutines calling GetTypedResponseChannel at the same time)
	sdr.lock.Lock()
	defer sdr.lock.Unlock()

	if sdr.channelRequested { //if channels for this reader have already been requested
		return nil, DataChannelAlreadyRequestedError
	}

	sdr.channelRequested = true

	//create typed data channels
	typedDataChan := make(chan *Message[DataType, ErrorType], sdr.typedDataChanBufferSize)

	//create a goroutine that "translates" all incoming data
	go func() {
		for {
			rawJsonData, chanOpen := <-sdr.rawDataChan
			if chanOpen {
				var msgObj Message[DataType, ErrorType]
				err := jsoniter.ConfigFastest.Unmarshal(rawJsonData, &msgObj)
				if err == nil { //no error
					typedDataChan <- &msgObj

				} else { //error
					typedDataChan <- &Message[DataType, ErrorType]{
						Error: &Error[ErrorType]{
							ErrorLevel:   ConnectorLevel,
							ErrorMessage: fmt.Sprintf("error in jsoniter.Unmarshal(rawJsonData, &msgObj): %s", err),
						},
					}
				}

			} else { //if data channel is closed
				close(typedDataChan) //close the typed data channel too
				return               //kill this goroutine
			}
		}
	}()

	return typedDataChan, nil
}

// Unsubscribe unsubscribes the associated subscription, alias for the Connector's Unsubscribe method.
func (sdr *SubscriptionDataReader) Unsubscribe() error {
	return sdr.wsConnector.Unsubscribe(sdr)
}

// UpdateSubscription updates the associated subscription, alias for the Connector's UpdateSubscription method.
func (sdr *SubscriptionDataReader) UpdateSubscription(data interface{}) error {
	return sdr.wsConnector.UpdateSubscription(sdr, data)
}
