package main

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	adapter_library "github.com/clearblade/adapter-go-library"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/debug"
	"github.com/gopcua/opcua/ua"
)

//TODO
// Implement subscriptions
//	create - in progress
//	publish - Needs research. I believe this refers to a "republish" request
//
// Verify response code for write and method
// Implement alarms
//

const (
	adapterName         = "opc-ua-adapter"
	appuri              = "urn:cb-opc-ua-adapter:client"
	readTopic           = "read"
	writeTopic          = "write"
	methodTopic         = "method"
	subscribeTopic      = "subscribe"
	javascriptISOString = "2006-01-02T15:04:05.000Z07:00"
)

var (
	adapterSettings   *opcuaAdapterSettings
	adapterConfig     *adapter_library.AdapterConfig
	opcuaClient       *opcua.Client
	openSubscriptions = make(map[uint32]*opcua.Subscription)
	clientHandle      uint32
)

func main() {
	err := adapter_library.ParseArguments(adapterName)
	if err != nil {
		log.Fatalf("[FATAL] Failed to parse arguments: %s\n", err.Error())
	}

	adapterConfig, err = adapter_library.Initialize()
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize: %s\n", err.Error())
	}

	adapterSettings = &opcuaAdapterSettings{}
	err = json.Unmarshal([]byte(adapterConfig.AdapterSettings), adapterSettings)
	if err != nil {
		log.Fatalf("[FATAL] Failed to parse Adapter Settings %s\n", err.Error())
	}

	//
	// The adapter will support the following topics:
	//
	//	write
	//	method
	//  subscribe
	//  alarm???
	//

	err = adapter_library.ConnectMQTT(adapterConfig.TopicRoot+"/+", cbMessageHandler)
	if err != nil {
		log.Fatalf("[FATAL] Failed to connect MQTT: %s\n", err.Error())
	}

	// initialize OPC UA connection
	opcuaClient = initializeOPCUA()
	defer opcuaClient.Close()

	// start polling of node_ids specified in adapter settings
	log.Println("[INFO] Start polling of provided node ids")

	readReq := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        []*ua.ReadValueID{},
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	for _, nodeid := range adapterSettings.NodeIDs {
		parsedID, err := ua.ParseNodeID(nodeid)
		if err != nil {
			log.Fatalf("[FATAL] Failed to parse node id %s: %s", nodeid, err.Error())
		}
		readReq.NodesToRead = append(readReq.NodesToRead, &ua.ReadValueID{
			NodeID: parsedID,
		})
	}

	ticker := time.NewTicker(time.Duration(adapterSettings.PollInterval) * time.Second)

	for {
		select {
		case _ = <-ticker.C:
			resp, err := opcuaClient.Read(readReq)
			if err != nil {
				log.Printf("[ERROR] Read request failed: %s\n", err.Error())
				break
			}
			mqttMessage := opcuaReadResponseMQTTMessage{
				Data: make(map[string]interface{}),
			}
			for idx, result := range resp.Results {
				if result.Status == ua.StatusOK {
					mqttMessage.Timestamp = result.ServerTimestamp.Format(time.RFC3339)
					mqttMessage.Data[adapterSettings.NodeIDs[idx]] = result.Value.Value()
				} else {
					log.Printf("[ERROR] Read Status not OK for node id %s: %v\n", adapterSettings.NodeIDs[idx], result.Status)
				}
			}
			if len(mqttMessage.Data) == 0 {
				log.Println("[IFNO] No data received, nothing to publish")
				continue
			}

			publishJson(adapterConfig.TopicRoot+"/"+readTopic, mqttMessage)
		}
	}
}

func initializeOPCUA() *opcua.Client {
	log.Println("[INFO] initializeOPCUAPolling - Creating OPC UA Session")

	if adapter_library.Args.LogLevel == "debug" {
		debug.Enable = true
	}

	opcuaOpts := []opcua.Option{}

	// get a list of endpoints for target server
	endpoints, err := opcua.GetEndpoints(context.Background(), adapterSettings.EndpointURL)
	if err != nil {
		log.Fatalf("[FATAL] Failed to get OPC UA Server endpoints: %s\n", err.Error())
	}

	var authMode ua.UserTokenType
	// determine auth type Anonymous, UserName, Certificate
	switch strings.ToLower(adapterSettings.Authentication.Type) {
	case "anonymous":
		authMode = ua.UserTokenTypeAnonymous
		opcuaOpts = append(opcuaOpts, opcua.AuthAnonymous())
	case "username":
		authMode = ua.UserTokenTypeUserName
		opcuaOpts = append(opcuaOpts, opcua.AuthUsername(adapterSettings.Authentication.Username, adapterSettings.Authentication.Password))
	case "certificate":
		log.Fatalln("[FATAL] Certificate auth type not implemented yet")
	default:
		log.Fatalf("[FATAL] Invalid auth type: %s\n", adapterSettings.Authentication.Type)
	}

	var secMode ua.MessageSecurityMode
	// set security mode None, Sign, SignAndEncrypt
	switch strings.ToLower(adapterSettings.SecurityMode) {
	case "none":
		secMode = ua.MessageSecurityModeNone
	case "sign":
		secMode = ua.MessageSecurityModeSign
	case "signandencrypt":
		secMode = ua.MessageSecurityModeSignAndEncrypt
	default:
		log.Fatalf("[FATAL] Invalid security mode: %s\n", adapterSettings.SecurityMode)
	}

	// set security policy
	var secPolicy string
	certsRequired := false
	switch strings.ToLower(adapterSettings.SecurityPolicy) {
	case "none":
		secPolicy = ua.SecurityPolicyURIPrefix + "None"
	case "basic256":
		secPolicy = ua.SecurityPolicyURIPrefix + "Basic256"
		certsRequired = true
	case "basic256sha256":
		secPolicy = ua.SecurityPolicyURIPrefix + "Basic256Sha256"
		certsRequired = true
	default:
		log.Fatalf("[FATAL] Invalid security policy: %s\n", adapterSettings.SecurityPolicy)
	}

	if certsRequired {
		generateCert(appuri, 2048, "cert.pem", "key.pem")
		c, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
		if err != nil {
			log.Fatalf("[FATAL] Failed to load certificates: %s\n", err.Error())
		}
		pk, ok := c.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			log.Fatalf("[FATAL] Invalid Private key: %s\n", err.Error())
		}
		opcuaOpts = append(opcuaOpts, opcua.PrivateKey(pk), opcua.Certificate(c.Certificate[0]))
	}

	var serverEndpoint *ua.EndpointDescription
	for _, e := range endpoints {
		if e.SecurityMode == secMode && e.SecurityPolicyURI == secPolicy {

			serverEndpoint = e
		}
	}
	if serverEndpoint == nil {
		log.Fatalf("[FATAL] Failed to find a matching server endpoint with sec-policy %s and sec-mode %s\n", secMode, secMode)
	}

	opcuaOpts = append(opcuaOpts, opcua.SecurityFromEndpoint(serverEndpoint, authMode))
	opcuaOpts = append(opcuaOpts, opcua.AutoReconnect(true))

	ctx := context.Background()

	log.Printf("[INFO] Connecting to OPC server address %s\n", adapterSettings.EndpointURL)
	c := opcua.NewClient(adapterSettings.EndpointURL, opcuaOpts...)
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("[FATAL] Failed to connect to OPC UA Server: %s\n", err.Error())
	}
	return c
}

func cbMessageHandler(message *mqttTypes.Publish) {
	//Determine the type of request that was received
	if strings.HasSuffix(message.Topic.Whole, writeTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA write request")
		go handleWriteRequest(message)
	} else if strings.HasSuffix(message.Topic.Whole, methodTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA method request")
		go handleMethodRequest(message)
	} else if strings.HasSuffix(message.Topic.Whole, subscribeTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA subscription request")
		go handleSubscriptionRequest(message)
	} else {
		log.Printf("[ERROR] cbMessageHandler - Unknown request received: topic = %s, payload = %#v\n", message.Topic.Whole, message.Payload)
	}
}

//OPC UA Attribute Service Set - write
func handleWriteRequest(message *mqttTypes.Publish) {
	writeReq := opcuaWriteRequestMQTTMessage{}
	err := json.Unmarshal(message.Payload, &writeReq)
	if err != nil {
		log.Printf("[ERROR] Failed to unmarshal request JSON: %s\n", err.Error())
		return
	}
	id, err := ua.ParseNodeID(writeReq.NodeID)
	if err != nil {
		log.Printf("[ERROR] Failed to parse OPC UA Node ID: %s\n", err.Error())
		return
	}
	v, err := ua.NewVariant(writeReq.Value)
	if err != nil {
		log.Printf("[ERROR] Failed to parses OPC UA Value: %s\n", err.Error())
		return
	}
	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			&ua.WriteValue{
				NodeID:      id,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					EncodingMask: ua.DataValueValue,
					Value:        v,
				},
			},
		},
	}
	resp, err := opcuaClient.Write(req)

	mqttResp := opcuaWriteResponseMQTTMessage{
		NodeID:       writeReq.NodeID,
		Timestamp:    resp.ResponseHeader.Timestamp.Format(time.RFC3339),
		Success:      true,
		StatusCode:   uint32(resp.ResponseHeader.ServiceResult),
		ErrorMessage: "",
		Results:      resp.Results,
	}

	if err != nil {
		log.Printf("[ERROR] Failed to write OPC UA tag %s: %s\n", writeReq.NodeID, err.Error())
		mqttResp.Success = false
		mqttResp.ErrorMessage = err.Error()
	}
	log.Printf("[INFO] OPC UA write successful: %+v\n", resp.Results[0])

	publishJson(adapterConfig.TopicRoot+"/"+writeTopic+"/response", mqttResp)
}

//OPC UA Method Service Set
func handleMethodRequest(message *mqttTypes.Publish) {
	methodReq := opcuaMethodRequestMQTTMessage{}

	//Create and initialize the response
	mqttResp := opcuaMethodResponseMQTTMessage{
		ObjectID:       "",
		MethodID:       "",
		Timestamp:      "",
		Success:        true,
		StatusCode:     0,
		ErrorMessage:   "",
		InputArguments: []interface{}{},
		OutputValues:   []interface{}{},
	}

	//Unmarshal the incoming JSON
	err := json.Unmarshal(message.Payload, &methodReq)
	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Failed to unmarshal method request JSON: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	//Since we were able to parse the input, add fields to the response
	mqttResp.ObjectID = methodReq.ObjectID
	mqttResp.MethodID = methodReq.MethodID
	mqttResp.InputArguments = methodReq.InputArguments

	//Parse the incoming object ID
	objId, err := ua.ParseNodeID(methodReq.ObjectID)
	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Failed to parse OPC UA Object ID: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	//Parse the incoming method ID
	methodId, err := ua.ParseNodeID(methodReq.MethodID)
	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Failed to parse OPC UA Method ID: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	//Populate the opcua request structure
	req := &ua.CallMethodRequest{
		ObjectID:       objId,
		MethodID:       methodId,
		InputArguments: []*ua.Variant{},
	}

	//We need to loop through the input arguments and create variants for each one
	for _, element := range methodReq.InputArguments {
		req.InputArguments = append(req.InputArguments, ua.MustVariant(element))
	}

	//Invoke the opcua method
	resp, err := opcuaClient.Call(req)

	//Populate the MQTT response and publish to the platform
	mqttResp.Timestamp = time.Now().Format(javascriptISOString)
	mqttResp.StatusCode = uint32(resp.StatusCode)

	//Check for errors while invoking the method
	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Error invoking OPC UA method: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	//Check for bad status codes
	if resp.StatusCode != ua.StatusOK {
		log.Printf("[ERROR] handleMethodRequest - Bad status code returned invoking OPC UA method: %s\n", resp.StatusCode)
		returnMethodError("Bad status code returned", &mqttResp)
		return
	}

	//We need to loop through the input arguments and create variants for each one
	for _, element := range resp.OutputArguments {
		mqttResp.OutputValues = append(mqttResp.OutputValues, element.Value())
	}

	//Publish the response to the platform
	publishJson(adapterConfig.TopicRoot+"/"+methodTopic+"/response", &mqttResp)
}

//OPC UA Subscription Service Set
func handleSubscriptionRequest(message *mqttTypes.Publish) {
	subReq := opcuaSubscriptionRequestMQTTMessage{}

	//Unmarshal the incoming JSON
	err := json.Unmarshal(message.Payload, &subReq)
	if err != nil {
		log.Printf("[ERROR] handleSubscriptionRequest - Failed to unmarshal subscription request JSON: %s\n", err.Error())
		returnSubscribeError(err.Error(), &opcuaSubscriptionResponseMQTTMessage{})
		return
	}

	switch strings.ToLower(string(subReq.RequestType)) {
	case string(SubscriptionCreate):
		handleSubscriptionCreate(&subReq)
	// TODO - Need to research what publish is for. I believe it is a "republish" request
	// case string(SubscriptionPublish):
	// 	handleSubscriptionPublish(&subReq)
	case string(SubscriptionDelete):
		handleSubscriptionDelete(&subReq)
	default:
		log.Printf("[ERROR] Invalid subscription request type: %s\n", subReq.RequestType)
		returnSubscribeError("Invalid subscription request type", &opcuaSubscriptionResponseMQTTMessage{
			NodeID:      subReq.NodeID,
			RequestType: subReq.RequestType,
		})
	}
}

//OPC UA Subscription Service Set - Create
func handleSubscriptionCreate(subReq *opcuaSubscriptionRequestMQTTMessage) {
	jsonString, _ := json.Marshal(*subReq.RequestParams)
	parms := opcuaSubscriptionCreateParmsMQTTMessage{}
	json.Unmarshal(jsonString, &parms)
	subParms := &opcua.SubscriptionParameters{}

	if parms.PublishInterval != nil {
		subParms.Interval = time.Duration(uint64(*parms.PublishInterval)) * time.Millisecond
	}

	if parms.LifetimeCount != nil {
		subParms.LifetimeCount = *parms.LifetimeCount
	}

	if parms.MaxKeepAliveCount != nil {
		subParms.MaxKeepAliveCount = *parms.MaxKeepAliveCount
	}

	if parms.MaxNotificationsPerPublish != nil {
		subParms.MaxNotificationsPerPublish = *parms.MaxNotificationsPerPublish
	}

	if parms.Priority != nil {
		subParms.Priority = *parms.Priority
	}

	//Create the subscription in a goroutine since we could have more than one subscription created
	go createSubscription(subReq, subParms)

}

func createSubscription(subReq *opcuaSubscriptionRequestMQTTMessage, subParms *opcua.SubscriptionParameters) {
	//https://medium.com/vacatronics/how-to-connect-with-opc-ua-using-go-5d7fdcac6217

	resp := opcuaSubscriptionResponseMQTTMessage{
		NodeID:      subReq.NodeID,
		RequestType: SubscriptionCreate,
	}

	jsonString, _ := json.Marshal(*subReq.RequestParams)
	parms := opcuaSubscriptionCreateParmsMQTTMessage{}
	json.Unmarshal(jsonString, &parms)

	notifyCh := make(chan *opcua.PublishNotificationData)

	sub, err := opcuaClient.Subscribe(subParms, notifyCh)
	if err != nil {
		log.Printf("[ERROR] createSubscription - Error occurred while subscribing: %s\n", err.Error())
		returnSubscribeError(err.Error(), &resp)
	}

	//Store the subscription in the openSubscriptions map, use the SubscriptionID as the key
	openSubscriptions[sub.SubscriptionID] = sub

	defer sub.Cancel()
	log.Printf("[INFO] createSubscription - Created subscription with id %v", sub.SubscriptionID)

	//Now that we have a subscription, we need to add the monitored items
	var miCreateRequests []*ua.MonitoredItemCreateRequest
	// TODO - clean this up, just hardcoded some stuff to get it working
	nodeid, _ := ua.ParseNodeID(subReq.NodeID)
	//for _, item := range *parms.MonitoredItems {
	miCreateRequests = append(miCreateRequests, opcua.NewMonitoredItemCreateRequestWithDefaults(nodeid, ua.AttributeIDValue, getClientHandle()))
	//}
	res, err := sub.Monitor(ua.TimestampsToReturnBoth, miCreateRequests...)
	if err != nil {
		log.Printf("[ERROR] createSubscription - Error occurred while adding monitored items: %s\n", err.Error())
		returnSubscribeError(err.Error(), &resp)
	}
	for _, result := range res.Results {
		if result.StatusCode != ua.StatusOK {
			log.Printf("[ERROR] createSubscription - Failed to add monitor item with status code: %d\n", result.StatusCode)
			returnSubscribeError(fmt.Errorf("Failed to add monitor item with status code: %d", result.StatusCode).Error(), &resp)
		}
	}
	log.Printf("[INFO] createSubscription - Added all monitored items")

	for {
		select {
		case res := <-notifyCh:
			if res.Error != nil {
				log.Printf("[ERROR] createSubscription - Unexpected error onsubscription: %s\n", res.Error.Error())
				// TODO - should we publish a subscribe error here?
				continue
			}
			switch x := res.Value.(type) {
			case *ua.DataChangeNotification:
				// TODO - need to publish this data via MQTT, but looks like we may need to track client handles to know how to map this to the monitored item/nodeid
				for _, item := range x.MonitoredItems {
					log.Printf("[INFO] MonitoredItem with client handle %v=%v", item.ClientHandle, item.Value.Value.Value())
				}
			default:
				log.Printf("[INFO] createSubscription - Unimplemented response type on subscription: %s\n", res.Value)
				// TODO - should we publish a subscribe error here?
			}
		}
	}
}

// OPC UA Subscription Service Set - Publish
// func handleSubscriptionPublish(subReq *opcuaSubscriptionRequestMQTTMessage) {
// TODO - We need to figure out how publish works with a client. It would make sense that publish
// is only a server function.
//
// }

//OPC UA Subscription Service Set - Delete
func handleSubscriptionDelete(subReq *opcuaSubscriptionRequestMQTTMessage) {
	jsonString, _ := json.Marshal(*subReq.RequestParams)
	parms := opcuaSubscriptionDeleteParmsMQTTMessage{}
	json.Unmarshal(jsonString, &parms)

	resp := opcuaSubscriptionResponseMQTTMessage{
		NodeID:       subReq.NodeID,
		RequestType:  SubscriptionDelete,
		Timestamp:    time.Now().Format(javascriptISOString),
		Success:      true,
		StatusCode:   0,
		ErrorMessage: "",
	}

	//Get the open subscription from the map in storage, using the incoming subscription ID
	err := openSubscriptions[parms.SubscriptionID].Cancel()
	if err != nil {
		log.Printf("[ERROR] handleSubscriptionDelete - Error occurred while deleting subscription: %s\n", err.Error())

		//Publish error to platform
		returnSubscribeError(err.Error(), &resp)
	}

	//Publish response to platform
	publishJson(adapterConfig.TopicRoot+"/"+subscribeTopic+"/response", resp)

	//Delete open subscription from map
	delete(openSubscriptions, parms.SubscriptionID)
}

func returnMethodError(errMsg string, resp *opcuaMethodResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().Format(javascriptISOString)
	publishJson(adapterConfig.TopicRoot+"/"+methodTopic+"/response", resp)
}

func returnSubscribeError(errMsg string, resp *opcuaSubscriptionResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().Format(javascriptISOString)
	publishJson(adapterConfig.TopicRoot+"/"+subscribeTopic+"/response", resp)
}

// Publishes data to a topic
func publishJson(topic string, data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return
	}

	log.Printf("[DEBUG] publish - Publishing to topic %s\n", topic)
	err = adapter_library.Publish(topic, b)
	if err != nil {
		log.Printf("[ERROR] Failed to publish MQTT message to topic %s: %s\n", topic, err.Error())
	}
}

func getClientHandle() uint32 {
	clientHandle++
	return clientHandle
}
