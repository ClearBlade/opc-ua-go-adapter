package main

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	adapter_library "github.com/clearblade/adapter-go-library"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	mqtt "github.com/clearblade/paho.mqtt.golang"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/debug"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	"github.com/pkg/errors"
)

// TODO
//  * Implement alarms (events??)
//  * Implement ModifySubscription when implemented by github.com/gopcua
//  * Implement SetPublishingMode when implemented by github.com/gopcua
//  * Implement Republish when implemented by github.com/gopcua
//  * Implement TransferSubscriptions when implemented by github.com/gopcua
//

const (
	adapterName       = "opc-ua-adapter"
	appuri            = "urn:cb-opc-ua-adapter:client"
	readTopic         = "read"
	writeTopic        = "write"
	methodTopic       = "method"
	subscribeTopic    = "subscribe"
	publishTopic      = "publish"
	browseTopic       = "browse"
	connectTopic      = "connect"
	ConnectionPending = "ConnectionPending"
	ConnectionFailed  = "ConnectionFailed"
	ConnectionSuccess = "ConnectionSuccess"
	BrowsePending     = "BrowsePending"
	BrowseFailed      = "BrowseFailed"
	BrowseSuccess     = "BrowseSuccess"
)

var (
	adapterSettings        *opcuaAdapterSettings
	adapterConfig          *adapter_library.AdapterConfig
	opcuaClient            *opcua.Client
	openSubscriptions      = make(map[uint32]*opcua.Subscription)
	clientHandle           uint32
	clientHandleRequestMap = make(map[uint32]map[uint32]interface{})
	eventFieldNames        = []string{"EventId", "EventType", "SourceNode", "SourceName", "Time", "ReceiveTime", "LocalTime", "Message", "Severity"}
	opcuaConnected         = false
)

type NodeDef struct {
	NodeID      *ua.NodeID
	NodeClass   ua.NodeClass
	BrowseName  string
	Description string
	AccessLevel ua.AccessLevelType
	Path        string
	DataType    string
	Writable    bool
	Unit        string
	Scale       string
	Min         string
	Max         string
}

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

	err = adapter_library.ConnectMQTT(adapterConfig.TopicRoot+"/#", cbMessageHandler)
	if err != nil {
		log.Fatalf("[FATAL] Failed to connect MQTT: %s\n", err.Error())
	}

	// initialize OPC UA connection
	opcuaClient = initializeOPCUA()

	// wait for signal to stop/kill process to allow for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	sig := <-c

	log.Printf("[INFO] OS signal %s received, gracefully shutting down adapter.\n", sig)

	err = opcuaClient.Close()
	if err != nil {
		log.Printf("[ERROR] Failed to close OPC UA Session: %s\n", err.Error())
		os.Exit(1)
	}

	os.Exit(0)

}

func initializeOPCUA() *opcua.Client {
	log.Println("[INFO] initializeOPCUA - Creating OPC UA Session")

	opcuaConnected = false

	if adapter_library.Args.LogLevel == "debug" {
		debug.Enable = true
	}

	connectionStatus := adapter_library.ConnectionStatus{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Status:    ConnectionPending,
	}

	mqttResp := opcuaConnectionResponseMQTTMessage{
		ConnectionStatus: connectionStatus,
	}

	opcuaOpts := []opcua.Option{}

	token, pubErr := returnConnectionMessage(&mqttResp)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}

	// get a list of endpoints for target server
	endpoints, err := opcua.GetEndpoints(context.Background(), adapterSettings.EndpointURL)
	if err != nil {
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to get OPC UA Server endpoints: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr = returnConnectionMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
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
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Certificate auth type not implemented yet"
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr = returnConnectionMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		log.Fatalln("[FATAL] Certificate auth type not implemented yet")
	default:
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Invalid auth type: " + adapterSettings.Authentication.Type
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr = returnConnectionMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
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
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Invalid security mode: " + adapterSettings.SecurityMode
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr = returnConnectionMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
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
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Invalid security policy: " + adapterSettings.SecurityPolicy
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr = returnConnectionMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		log.Fatalf("[FATAL] Invalid security policy: %s\n", adapterSettings.SecurityPolicy)
	}

	if certsRequired {
		generateCert(appuri, 2048, "cert.pem", "key.pem")
		c, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
		if err != nil {
			mqttResp.ConnectionStatus.Status = ConnectionFailed
			mqttResp.ConnectionStatus.ErrorMessage = "Failed to load certificates: " + err.Error()
			mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr = returnConnectionMessage(&mqttResp)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			token.Wait()
			log.Fatalf("[FATAL] Failed to load certificates: %s\n", err.Error())
		}
		pk, ok := c.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			mqttResp.ConnectionStatus.Status = ConnectionFailed
			mqttResp.ConnectionStatus.ErrorMessage = "Invalid Private key: " + err.Error()
			mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr = returnConnectionMessage(&mqttResp)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			token.Wait()
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
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to find a matching server endpoint with sec-policy " + secMode.String() + " and sec-mode " + secMode.String()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr = returnConnectionMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		log.Fatalf("[FATAL] Failed to find a matching server endpoint with sec-policy %s and sec-mode %s\n", secPolicy, secMode)
	}

	opcuaOpts = append(opcuaOpts, opcua.SecurityFromEndpoint(serverEndpoint, authMode))
	opcuaOpts = append(opcuaOpts, opcua.AutoReconnect(true))
	//opcuaOpts = append(opcuaOpts, opcua.SessionTimeout(0))

	ctx := context.Background()

	log.Printf("[INFO] Connecting to OPC server address %s\n", adapterSettings.EndpointURL)
	c := opcua.NewClient(adapterSettings.EndpointURL, opcuaOpts...)
	if err := c.Connect(ctx); err != nil {
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to connect to OPC UA Server: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr = returnConnectionMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		log.Fatalf("[FATAL] Failed to connect to OPC UA Server: %s\n", err.Error())
	}
	mqttResp.ConnectionStatus.Status = ConnectionSuccess
	mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
	_, pubErr = returnConnectionMessage(&mqttResp)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}
	opcuaConnected = true
	return c
}

func cbMessageHandler(message *mqttTypes.Publish) {
	//Determine the type of request that was received
	if strings.Contains(message.Topic.Whole, "response") {
		log.Println("[DEBUG] cbMessageHandler - Received response, ignoring")
	} else if strings.Contains(message.Topic.Whole, readTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA read request")
		if opcuaConnected {
			go handleReadRequest(message)
		} else {
			log.Println("[INFO] cbMessageHandler - not connected to OPC UA, ignoring")
		}
	} else if strings.Contains(message.Topic.Whole, writeTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA write request")
		if opcuaConnected {
			go handleWriteRequest(message)
		} else {
			log.Println("[INFO] cbMessageHandler - not connected to OPC UA, ignoring")
		}
	} else if strings.Contains(message.Topic.Whole, methodTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA method request")
		if opcuaConnected {
			go handleMethodRequest(message)
		} else {
			log.Println("[INFO] cbMessageHandler - not connected to OPC UA, ignoring")
		}
	} else if strings.Contains(message.Topic.Whole, subscribeTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA subscription request")
		if opcuaConnected {
			go handleSubscriptionRequest(message)
		} else {
			log.Println("[INFO] cbMessageHandler - not connected to OPC UA, ignoring")
		}
	} else if strings.Contains(message.Topic.Whole, browseTopic) {
		log.Println("[INFO] cbMessageHandler - Recieved OPC UA browse request")
		if opcuaConnected {
			go handleBrowseRequest(message)
		} else {
			log.Println("[INFO] cbMessageHandler - not connected to OPC UA, ignoring")
		}

	} else if strings.Contains(message.Topic.Whole, connectTopic) {
		log.Println("[INFO] cbMessageHandler - Received OPC UA connect request")
		go handleConnectRequest(message)
	} else {
		log.Printf("[ERROR] cbMessageHandler - Unknown request received: topic = %s, payload = %#v\n", message.Topic.Whole, message.Payload)
	}
}

// OPC UA Attribute Service Set - read
func handleReadRequest(message *mqttTypes.Publish) {

	mqttResp := opcuaReadResponseMQTTMessage{
		ServerTimestamp: "",
		Data:            make(map[string]opcuaReadResponseData),
		Success:         true,
		StatusCode:      0,
		ErrorMessage:    "",
	}

	readReq := opcuaReadRequestMQTTMessage{}
	err := json.Unmarshal(message.Payload, &readReq)
	if err != nil {
		log.Printf("[ERROR] Failed to unmarshal request JSON: %s\n", err.Error())
		returnReadError(err.Error(), &mqttResp)
		return
	}

	opcuaReadReq := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        []*ua.ReadValueID{},
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	for _, nodeid := range readReq.NodeIDs {
		parsedID, err := ua.ParseNodeID(nodeid)
		if err != nil {
			log.Printf("[ERROR] Failed to parse node id %s: %s", nodeid, err.Error())
			returnReadError(err.Error(), &mqttResp)
			return
		}
		opcuaReadReq.NodesToRead = append(opcuaReadReq.NodesToRead, &ua.ReadValueID{
			NodeID: parsedID,
		})
	}

	opcuaResp, err := opcuaClient.Read(opcuaReadReq)
	if err != nil {
		log.Printf("[ERROR] Read request failed: %s\n", err.Error())
		returnReadError(err.Error(), &mqttResp)
		return
	}

	for idx, result := range opcuaResp.Results {
		if result.Status == ua.StatusOK {
			mqttResp.ServerTimestamp = result.ServerTimestamp.Format(time.RFC3339)
			mqttResp.Data[readReq.NodeIDs[idx]] = opcuaReadResponseData{
				Value:           result.Value.Value(),
				SourceTimestamp: result.SourceTimestamp.Format(time.RFC3339),
			}
		} else {
			log.Printf("[ERROR] Read Status not OK for node id %s: %+v\n", readReq.NodeIDs[idx], result.Status)
			returnReadError(fmt.Sprintf("Read Status not OK for node id %s: %+v\n", readReq.NodeIDs[idx], result.Status), &mqttResp)
			return
		}
	}

	if len(mqttResp.Data) == 0 {
		log.Println("[IFNO] No data received, nothing to publish")
		return
	}

	publishJson(adapterConfig.TopicRoot+"/"+readTopic+"/response", mqttResp)
}

//OPC UA Attribute Service Set - write
func handleWriteRequest(message *mqttTypes.Publish) {

	mqttResp := opcuaWriteResponseMQTTMessage{
		NodeID:       "",
		Timestamp:    "",
		Success:      true,
		StatusCode:   0,
		ErrorMessage: "",
	}

	writeReq := opcuaWriteRequestMQTTMessage{}
	err := json.Unmarshal(message.Payload, &writeReq)
	if err != nil {
		log.Printf("[ERROR] Failed to unmarshal request JSON: %s\n", err.Error())
		returnWriteError(err.Error(), &mqttResp)
		return
	}

	id, err := ua.ParseNodeID(writeReq.NodeID)
	if err != nil {
		log.Printf("[ERROR] Failed to parse OPC UA Node ID: %s\n", err.Error())
		returnWriteError(err.Error(), &mqttResp)
		return
	}

	nodeType, err := getTagDataType(id)
	if err != nil {
		log.Printf("[ERROR] Failed to get type for Node ID %s: %s\n", id.String(), err.Error())
		returnWriteError(err.Error(), &mqttResp)
		return
	}

	switch val := writeReq.Value.(type) {
	case []interface{}:
		switch *nodeType {
		case ua.TypeIDBoolean:
			convertedArray := make([]bool, 0)
			for _, i := range val {
				v, err := getConvertedValue(nodeType, i)
				if err != nil {
					log.Println("[ERROR] " + err.Error())
					returnWriteError(err.Error(), &mqttResp)
					return
				}
				convertedArray = append(convertedArray, v.(bool))
			}
		case ua.TypeIDDouble:
			convertedArray := make([]float64, 0)
			for _, i := range val {
				v, err := getConvertedValue(nodeType, i)
				if err != nil {
					log.Println("[ERROR] " + err.Error())
					returnWriteError(err.Error(), &mqttResp)
					return
				}
				convertedArray = append(convertedArray, v.(float64))
			}
			writeReq.Value = convertedArray
		case ua.TypeIDInt16:
			convertedArray := make([]int16, 0)
			for _, i := range val {
				v, err := getConvertedValue(nodeType, i)
				if err != nil {
					log.Println("[ERROR] " + err.Error())
					returnWriteError(err.Error(), &mqttResp)
					return
				}
				convertedArray = append(convertedArray, v.(int16))
			}
			writeReq.Value = convertedArray
		case ua.TypeIDInt32:
			convertedArray := make([]int32, 0)
			for _, i := range val {
				v, err := getConvertedValue(nodeType, i)
				if err != nil {
					log.Println("[ERROR] " + err.Error())
					returnWriteError(err.Error(), &mqttResp)
					return
				}
				convertedArray = append(convertedArray, v.(int32))
			}
			writeReq.Value = convertedArray
		case ua.TypeIDInt64:
			convertedArray := make([]int64, 0)
			for _, i := range val {
				v, err := getConvertedValue(nodeType, i)
				if err != nil {
					log.Println("[ERROR] " + err.Error())
					returnWriteError(err.Error(), &mqttResp)
					return
				}
				convertedArray = append(convertedArray, v.(int64))
			}
		case ua.TypeIDString:
			convertedArray := make([]string, 0)
			for _, i := range val {
				v, err := getConvertedValue(nodeType, i)
				if err != nil {
					log.Println("[ERROR] " + err.Error())
					returnWriteError(err.Error(), &mqttResp)
					return
				}
				convertedArray = append(convertedArray, v.(string))
			}
			writeReq.Value = convertedArray
		case ua.TypeIDFloat:
			convertedArray := make([]float32, 0)
			for _, i := range val {
				v, err := getConvertedValue(nodeType, i)
				if err != nil {
					log.Println("[ERROR] " + err.Error())
					returnWriteError(err.Error(), &mqttResp)
					return
				}
				convertedArray = append(convertedArray, v.(float32))
			}
			writeReq.Value = convertedArray
		default:
			log.Printf("[ERROR] Unhandled node type: " + nodeType.String())
			return
		}
	case interface{}:
		val, err = getConvertedValue(nodeType, val)
		if err != nil {
			log.Println("[ERROR] " + err.Error())
			returnWriteError(err.Error(), &mqttResp)
			return
		}
	default:
		log.Printf("[ERROR] Unexpected type for write value: %T\n", val)
		returnWriteError(fmt.Sprintf("Unexpected type for write value: %T", val), &mqttResp)
		return
	}

	variant, err := ua.NewVariant(writeReq.Value)
	if err != nil {
		log.Printf("[ERROR] Failed to create new variant: %s\n", err.Error())
		returnWriteError(err.Error(), &mqttResp)
		return
	}

	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			&ua.WriteValue{
				NodeID:      id,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					EncodingMask: ua.DataValueValue,
					Value:        variant,
				},
			},
		},
	}

	resp, err := opcuaClient.Write(req)
	if err != nil {
		log.Printf("[ERROR] Failed to write OPC UA tag %s: %s\n", writeReq.NodeID, err.Error())
		returnWriteError(err.Error(), &mqttResp)
		return
	}

	if resp.Results[0] != ua.StatusOK {
		log.Printf("[ERROR] non ok status returned from write: %s\n", resp.Results[0].Error())
		mqttResp.StatusCode = uint32(resp.Results[0])
		returnWriteError(fmt.Sprintf("Non OK status code returned from write: %s\n", resp.Results[0].Error()), &mqttResp)
		return
	}

	mqttResp.NodeID = writeReq.NodeID
	mqttResp.Timestamp = resp.ResponseHeader.Timestamp.UTC().Format(time.RFC3339)
	mqttResp.StatusCode = uint32(resp.ResponseHeader.ServiceResult)

	log.Printf("[INFO] OPC UA write successful: %+v\n", resp.Results[0])

	publishJson(adapterConfig.TopicRoot+"/"+writeTopic+"/response", mqttResp)
}

func getConvertedValue(nodeType *ua.TypeID, value interface{}) (interface{}, error) {
	switch *nodeType {
	case ua.TypeIDBoolean:
		return value.(bool), nil
	case ua.TypeIDDateTime:
		return value.(string), nil
	case ua.TypeIDDouble:
		return value.(float64), nil
	case ua.TypeIDFloat:
		return float32(value.(float64)), nil
	case ua.TypeIDGUID:
		return value.(string), nil
	case ua.TypeIDInt16:
		return int16(value.(float64)), nil
	case ua.TypeIDInt32:
		return int32(value.(float64)), nil
	case ua.TypeIDInt64:
		return int64(value.(float64)), nil
	case ua.TypeIDLocalizedText:
		return value.(string), nil
	case ua.TypeIDNodeID:
		return value.(string), nil
	case ua.TypeIDQualifiedName:
		return value.(string), nil
	case ua.TypeIDString:
		return value.(string), nil
	case ua.TypeIDUint16:
		return uint16(value.(float64)), nil
	case ua.TypeIDUint32:
		return uint32(value.(float64)), nil
	case ua.TypeIDUint64:
		return uint64(value.(float64)), nil
	case ua.TypeIDVariant:
		return value.(bool), nil
	case ua.TypeIDXMLElement:
		return value.(string), nil
	default:
		return nil, fmt.Errorf("Unhandled node type: " + nodeType.String())
	}
}

func getTagDataType(nodeid *ua.NodeID) (*ua.TypeID, error) {
	log.Printf("[INFO] getTagDataType - checking type for node id: %s\n", nodeid.String())

	req := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        []*ua.ReadValueID{},
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	req.NodesToRead = append(req.NodesToRead, &ua.ReadValueID{
		NodeID: nodeid,
	})

	opcuaResp, err := opcuaClient.Read(req)
	if err != nil {
		log.Printf("[ERROR] Read type request failed: %s\n", err.Error())
		return nil, err
	}

	if opcuaResp.Results[0].Status != ua.StatusOK {
		return nil, fmt.Errorf("read type status not OK for node id %s: %+v", nodeid.String(), opcuaResp.Results[0].Status)
	}

	log.Printf("[INFO] getTagDataType - type for node id %s: %s\n", nodeid.String(), opcuaResp.Results[0].Value.Type().String())

	nodeType := opcuaResp.Results[0].Value.Type()

	return &nodeType, nil
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
	mqttResp.Timestamp = time.Now().UTC().Format(time.RFC3339)
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
		log.Println("[INFO] handleSubscriptionRequest - received create subscription request")
		handleSubscriptionCreate(&subReq)
	case string(SubscriptionDelete):
		log.Println("[INFO] handleSubscriptionRequest - received delete subscription request")
		handleSubscriptionDelete(&subReq)
	default:
		log.Printf("[ERROR] Invalid subscription request type: %s\n", subReq.RequestType)
		returnSubscribeError("Invalid subscription request type", &opcuaSubscriptionResponseMQTTMessage{
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
		RequestType:  SubscriptionCreate,
		Timestamp:    "",
		Success:      true,
		StatusCode:   0,
		ErrorMessage: "",
		Results:      []interface{}{},
	}

	log.Printf("[DEBUG] createSubscription - opcuaSubscriptionRequestMQTTMessage: %+v\n", subReq)
	log.Printf("[DEBUG] createSubscription - opcua.SubscriptionParameters: %+v\n", subParms)

	jsonString, _ := json.Marshal(*subReq.RequestParams)
	parms := opcuaSubscriptionCreateParmsMQTTMessage{}
	json.Unmarshal(jsonString, &parms)

	notifyCh := make(chan *opcua.PublishNotificationData)

	sub, err := opcuaClient.Subscribe(subParms, notifyCh)
	if err != nil {
		log.Printf("[ERROR] createSubscription - Error occurred while subscribing: %s\n", err.Error())
		returnSubscribeError(err.Error(), &resp)
		return
	}

	//Store the subscription in the openSubscriptions map, use the SubscriptionID as the key
	openSubscriptions[sub.SubscriptionID] = sub
	clientHandleRequestMap[sub.SubscriptionID] = make(map[uint32]interface{})

	log.Printf("[DEBUG] createSubscription - Subscription ID: %+v\n", sub)

	// add subscription id to response
	resp.SubscriptionID = sub.SubscriptionID

	defer sub.Cancel()
	log.Printf("[INFO] createSubscription - Created subscription with id %d", sub.SubscriptionID)

	//Now that we have a subscription, we need to add the monitored items
	var miCreateRequests []*ua.MonitoredItemCreateRequest

	errors := false
	for _, item := range *parms.MonitoredItems {

		log.Printf("[DEBUG] createSubscription - Item to monitor: %+v\n", item)

		nodeId, err := ua.ParseNodeID(item.NodeID)
		if err != nil {
			log.Printf("[ERROR] createSubscription - Failed to parse OPC UA Node ID: %s\n", err.Error())
			errors = true
			resp.Results = append(resp.Results, opcuaMonitoredItemCreateResultMQTTMessage{
				NodeID:     item.NodeID,
				StatusCode: uint32(ua.StatusBadNodeIDInvalid),
			})
			continue
		}

		// TODO - Handle all attribute values, ex. AttributeIDEventNotifier
		if item.Values {
			log.Println("[ERROR] createSubscription - creating monitored item value request")
			miCreateRequests = append(miCreateRequests, opcua.NewMonitoredItemCreateRequestWithDefaults(nodeId, ua.AttributeIDValue, getClientHandle()))
			clientHandleRequestMap[sub.SubscriptionID][clientHandle] = item
		}

		if item.Events {
			log.Println("[ERROR] createSubscription - creating monitored item event request")
			selects := make([]*ua.SimpleAttributeOperand, len(eventFieldNames))

			for i, name := range eventFieldNames {
				selects[i] = &ua.SimpleAttributeOperand{
					TypeDefinitionID: ua.NewNumericNodeID(0, id.BaseEventType),
					BrowsePath:       []*ua.QualifiedName{{NamespaceIndex: 0, Name: name}},
					AttributeID:      ua.AttributeIDValue,
				}
			}

			wheres := &ua.ContentFilter{
				Elements: []*ua.ContentFilterElement{
					{
						FilterOperator: ua.FilterOperatorGreaterThanOrEqual,
						FilterOperands: []*ua.ExtensionObject{
							{
								EncodingMask: 1,
								TypeID: &ua.ExpandedNodeID{
									NodeID: ua.NewNumericNodeID(0, id.SimpleAttributeOperand_Encoding_DefaultBinary),
								},
								Value: ua.SimpleAttributeOperand{
									TypeDefinitionID: ua.NewNumericNodeID(0, id.BaseEventType),
									BrowsePath:       []*ua.QualifiedName{{NamespaceIndex: 0, Name: "Severity"}},
									AttributeID:      ua.AttributeIDValue,
								},
							},
							{
								EncodingMask: 1,
								TypeID: &ua.ExpandedNodeID{
									NodeID: ua.NewNumericNodeID(0, id.LiteralOperand_Encoding_DefaultBinary),
								},
								Value: ua.LiteralOperand{
									Value: ua.MustVariant(uint16(0)),
								},
							},
						},
					},
				},
			}

			filter := ua.EventFilter{
				SelectClauses: selects,
				WhereClause:   wheres,
			}

			filterExtObj := ua.ExtensionObject{
				EncodingMask: ua.ExtensionObjectBinary,
				TypeID: &ua.ExpandedNodeID{
					NodeID: ua.NewNumericNodeID(0, id.EventFilter_Encoding_DefaultBinary),
				},
				Value: filter,
			}

			handle := getClientHandle()
			req := &ua.MonitoredItemCreateRequest{
				ItemToMonitor: &ua.ReadValueID{
					NodeID:       nodeId,
					AttributeID:  ua.AttributeIDEventNotifier,
					DataEncoding: &ua.QualifiedName{},
				},
				MonitoringMode: ua.MonitoringModeReporting,
				RequestedParameters: &ua.MonitoringParameters{
					ClientHandle:     handle,
					DiscardOldest:    true,
					Filter:           &filterExtObj,
					QueueSize:        10,
					SamplingInterval: 1.0,
				},
			}
			miCreateRequests = append(miCreateRequests, req)
			clientHandleRequestMap[sub.SubscriptionID][clientHandle] = item
		}

		//Add the client handle and request to the map
		clientHandleRequestMap[sub.SubscriptionID][clientHandle] = item
	}

	log.Printf("[DEBUG] createSubscription - Monitored item create requests: %+v\n", miCreateRequests)

	res, err := sub.Monitor(ua.TimestampsToReturnBoth, miCreateRequests...)
	if err != nil {
		log.Printf("[ERROR] createSubscription - Error occurred while adding monitored items: %s\n", err.Error())
		returnSubscribeError(err.Error(), &resp)
		return //TODO - Should we do this? Need to research whether or not a partial success is possible
	}

	//See if any errors were encountered
	for _, result := range res.Results {
		if result.StatusCode != ua.StatusOK {
			log.Printf("[ERROR] createSubscription - Failed to add monitor item with status code: %d\n", result.StatusCode)
			errors = true
		}
	}
	log.Printf("[INFO] createSubscription - Added all monitored items")

	//Publish create response
	resp.Timestamp = time.Now().UTC().Format(time.RFC3339)

	if errors {
		resp.ErrorMessage = "Failed to add all monitor items, see results"
		resp.Success = false
	}

	publishJson(adapterConfig.TopicRoot+"/"+subscribeTopic+"/response", &resp)

	for {
		select {
		case res := <-notifyCh:
			if res.Error != nil {
				log.Printf("[ERROR] createSubscription - Unexpected error onsubscription: %s\n", res.Error.Error())
				returnSubscribeError(fmt.Errorf("unexpected error onsubscription: %s", res.Error.Error()).Error(), &resp)
				continue
			}
			log.Printf("[DEBUG] received message on subscription response channel of type %T\n", res.Value)
			switch x := res.Value.(type) {
			case *ua.DataChangeNotification:
				resp := opcuaSubscriptionResponseMQTTMessage{
					RequestType:    SubscriptionPublish,
					Timestamp:      time.Now().UTC().Format(time.RFC3339),
					Success:        true,
					StatusCode:     uint32(ua.StatusOK),
					ErrorMessage:   "",
					Results:        []interface{}{},
					SubscriptionID: sub.SubscriptionID,
				}
				for _, item := range x.MonitoredItems {
					//Get the NodeId from the clientHandleRequestMap
					if item.Value == nil {
						log.Printf("[ERROR] item.Value is nil\n")
						continue
					}
					if item.Value.Value == nil {
						log.Println("[ERROR] item.Value.Value is nil")
						continue
					}
					resp.Results = append(resp.Results, opcuaMonitoredItemNotificationMQTTMessage{
						NodeID: (clientHandleRequestMap[sub.SubscriptionID][item.ClientHandle].(opcuaMonitoredItemCreateMQTTMessage)).NodeID,
						Value:  item.Value.Value.Value(),
					})
				}

				publishJson(adapterConfig.TopicRoot+"/"+publishTopic+"/response", &resp)
			case *ua.EventNotificationList:
				resp := opcuaSubscriptionResponseMQTTMessage{
					RequestType:    SubscriptionPublish,
					Timestamp:      time.Now().UTC().Format(time.RFC3339),
					Success:        true,
					StatusCode:     uint32(ua.StatusOK),
					ErrorMessage:   "",
					Results:        []interface{}{},
					SubscriptionID: sub.SubscriptionID,
				}
				for _, item := range x.Events {
					resp.Results = append(resp.Results, opcuaMonitoredItemNotificationMQTTMessage{
						NodeID: (clientHandleRequestMap[sub.SubscriptionID][item.ClientHandle].(opcuaMonitoredItemCreateMQTTMessage)).NodeID,
						Event: opcuaEventMessage{
							EventID:     hex.EncodeToString(item.EventFields[0].Value().([]uint8)),
							EventType:   id.Name(item.EventFields[1].Value().(*ua.NodeID).IntID()),
							SourceNode:  item.EventFields[2].Value().(*ua.NodeID).String(),
							SourceName:  item.EventFields[3].Value().(string),
							Time:        item.EventFields[4].Value().(time.Time).UTC().Format(time.RFC3339),
							ReceiveTime: item.EventFields[5].Value().(time.Time).UTC().Format(time.RFC3339),
							LocalTime:   item.EventFields[6].Value(),
							Message:     item.EventFields[7].Value().(*ua.LocalizedText).Text,
							Severity:    uint32(item.EventFields[8].Value().(uint16)),
						},
					})
				}

				publishJson(adapterConfig.TopicRoot+"/"+publishTopic+"/response", &resp)
			default:
				log.Printf("[INFO] createSubscription - Unimplemented response type on subscription: %s\n", res.Value)
				returnSubscribeError("Unimplemented response type on subscription", &resp)
			}
		}
	}
}

//OPC UA Subscription Service Set - Delete
func handleSubscriptionDelete(subReq *opcuaSubscriptionRequestMQTTMessage) {
	jsonString, _ := json.Marshal(*subReq.RequestParams)
	parms := opcuaSubscriptionDeleteParmsMQTTMessage{}
	json.Unmarshal(jsonString, &parms)

	resp := opcuaSubscriptionResponseMQTTMessage{
		//NodeID:       subReq.NodeID,
		RequestType:    SubscriptionDelete,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		Success:        true,
		StatusCode:     0,
		ErrorMessage:   "",
		SubscriptionID: parms.SubscriptionID,
	}

	//Get the open subscription from the map in storage, using the incoming subscription ID
	if _, ok := openSubscriptions[parms.SubscriptionID]; !ok {
		log.Printf("[ERROR] handleSubscriptionDelete - No active subscription for ID: %d\n", parms.SubscriptionID)
		returnSubscribeError(fmt.Sprintf("No active subscription for ID: %d", parms.SubscriptionID), &resp)
		return
	}

	log.Printf("[DEBUG] handleSubscriptionDelete - Deleting subscription: %d\n", parms.SubscriptionID)
	err := openSubscriptions[parms.SubscriptionID].Cancel()

	if err != nil && err != ua.StatusOK {
		//handleSubscriptionDelete - Error occurred while deleting subscription:  OK (0x0)
		log.Printf("[ERROR] handleSubscriptionDelete - Error occurred while deleting subscription: %s\n", err.Error())

		//Publish error to platform
		returnSubscribeError(err.Error(), &resp)
		return
	}

	log.Println("[DEBUG] handleSubscriptionDelete - Subscription deleted")

	//Publish response to platform
	publishJson(adapterConfig.TopicRoot+"/"+subscribeTopic+"/response", resp)

	//Empty the clientHandleRequestMap for the current subscription
	delete(clientHandleRequestMap, parms.SubscriptionID)

	//Delete open subscription from map
	delete(openSubscriptions, parms.SubscriptionID)
}

func handleBrowseRequest(message *mqttTypes.Publish) {

	connectionStatus := adapter_library.ConnectionStatus{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Status:    BrowsePending,
	}

	mqttResp := opcuaBrowseResponseMQTTMessage{
		NodeIDs:          make([]string, 0),
		ConnectionStatus: connectionStatus,
	}

	_, pubErr := returnBrowseMessage(&mqttResp)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}

	browseReq := opcuaBrowseRequestMQTTMessage{}
	err := json.Unmarshal(message.Payload, &browseReq)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to unmarshal request JSON: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowseMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Failed to unmarshal request JSON: %s\n", err.Error())
		return
	}

	if browseReq.RootNode == "" {
		browseReq.RootNode = "i=85"
	}

	id, err := ua.ParseNodeID(browseReq.RootNode)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Invalid node id: " + err.Error() + ", RootId: " + browseReq.RootNode
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowseMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Invalid node id: %s, RootId: %s", err.Error(), browseReq.RootNode)
		return
	}

	nodeList, err := browse(opcuaClient.Node(id), "", 0)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to browse nodes: " + err.Error() + ", RootId: " + browseReq.RootNode
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowseMessage(&mqttResp)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Failed to browse nodes: %s, RootId: %s", err.Error(), browseReq.RootNode)
		return
	}

	for _, s := range nodeList {
		log.Println("[DEBUG] NodeID: " + s.NodeID.String())
		mqttResp.NodeIDs = append(mqttResp.NodeIDs, s.NodeID.String())
	}

	mqttResp.ConnectionStatus.Status = BrowseSuccess
	mqttResp.ConnectionStatus.ErrorMessage = ""
	mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
	_, pubErr = returnBrowseMessage(&mqttResp)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}
}

func handleConnectRequest(message *mqttTypes.Publish) {
	log.Println("[DEBUG] handleConnectRequest: Killing OPCUA connection and exiting to trigger automatic restart")

	err := opcuaClient.Close()
	if err != nil {
		log.Printf("[ERROR] Failed to close OPC UA Session: %s\n", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}

func returnReadError(errMsg string, resp *opcuaReadResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.ServerTimestamp = time.Now().UTC().Format(time.RFC3339)
	publishJson(adapterConfig.TopicRoot+"/"+readTopic+"/response", resp)
}

func returnWriteError(errMsg string, resp *opcuaWriteResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().UTC().Format(time.RFC3339)
	publishJson(adapterConfig.TopicRoot+"/"+writeTopic+"/response", resp)
}

func returnMethodError(errMsg string, resp *opcuaMethodResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().UTC().Format(time.RFC3339)
	publishJson(adapterConfig.TopicRoot+"/"+methodTopic+"/response", resp)
}

func returnSubscribeError(errMsg string, resp *opcuaSubscriptionResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().UTC().Format(time.RFC3339)
	publishJson(adapterConfig.TopicRoot+"/"+subscribeTopic+"/response", resp)
}

func returnBrowseMessage(resp *opcuaBrowseResponseMQTTMessage) (mqtt.Token, error) {
	log.Printf("[DEBUG] returnBrowseMessage - publishing to topic %s with message %s\n", adapterConfig.TopicRoot+"/"+browseTopic+"/response", resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(adapterConfig.TopicRoot+"/"+browseTopic+"/response", json)
}

func returnConnectionMessage(resp *opcuaConnectionResponseMQTTMessage) (mqtt.Token, error) {
	log.Printf("[DEBUG] returnConnectionMessage - publishing to topic %s with message %s\n", adapterConfig.TopicRoot+"/"+connectTopic+"/response", resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(adapterConfig.TopicRoot+"/"+connectTopic+"/response", json)
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

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

func browse(n *opcua.Node, path string, level int) ([]NodeDef, error) {
	log.Printf("[DEBUG] node:%s path:%q level:%d\n", n, path, level)
	if level > 10 {
		return nil, nil
	}

	attrs, err := n.Attributes(ua.AttributeIDNodeClass, ua.AttributeIDBrowseName, ua.AttributeIDDescription, ua.AttributeIDAccessLevel, ua.AttributeIDDataType)
	if err != nil {
		return nil, err
	}

	var def = NodeDef{
		NodeID: n.ID,
	}

	switch err := attrs[0].Status; err {
	case ua.StatusOK:
		def.NodeClass = ua.NodeClass(attrs[0].Value.Int())
	default:
		return nil, err
	}

	switch err := attrs[1].Status; err {
	case ua.StatusOK:
		def.BrowseName = attrs[1].Value.String()
	default:
		return nil, err
	}

	switch err := attrs[2].Status; err {
	case ua.StatusOK:
		def.Description = attrs[2].Value.String()
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	default:
		return nil, err
	}

	switch err := attrs[3].Status; err {
	case ua.StatusOK:
		def.AccessLevel = ua.AccessLevelType(attrs[3].Value.Int())
		def.Writable = def.AccessLevel&ua.AccessLevelTypeCurrentWrite == ua.AccessLevelTypeCurrentWrite
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	default:
		return nil, err
	}

	switch err := attrs[4].Status; err {
	case ua.StatusOK:
		switch v := attrs[4].Value.NodeID().IntID(); v {
		case id.DateTime:
			def.DataType = "time.Time"
		case id.Boolean:
			def.DataType = "bool"
		case id.SByte:
			def.DataType = "int8"
		case id.Int16:
			def.DataType = "int16"
		case id.Int32:
			def.DataType = "int32"
		case id.Byte:
			def.DataType = "byte"
		case id.UInt16:
			def.DataType = "uint16"
		case id.UInt32:
			def.DataType = "uint32"
		case id.UtcTime:
			def.DataType = "time.Time"
		case id.String:
			def.DataType = "string"
		case id.Float:
			def.DataType = "float32"
		case id.Double:
			def.DataType = "float64"
		default:
			def.DataType = attrs[4].Value.NodeID().String()
		}
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	default:
		return nil, err
	}

	def.Path = join(path, def.BrowseName)
	log.Printf("[DEBUG] %d: def.Path:%s def.NodeClass:%s\n", level, def.Path, def.NodeClass)

	var nodes []NodeDef
	if def.NodeClass == ua.NodeClassVariable {
		nodes = append(nodes, def)
	}

	browseChildren := func(refType uint32) error {
		refs, err := n.ReferencedNodes(refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil {
			return errors.Errorf("References: %d: %s", refType, err)
		}
		log.Printf("[DEBUG] found %d child refs\n", len(refs))
		for _, rn := range refs {
			children, err := browse(rn, def.Path, level+1)
			if err != nil {
				return errors.Errorf("browse children: %s", err)
			}
			nodes = append(nodes, children...)
		}
		return nil
	}

	if err := browseChildren(id.HasComponent); err != nil {
		return nil, err
	}
	if err := browseChildren(id.Organizes); err != nil {
		return nil, err
	}
	if err := browseChildren(id.HasProperty); err != nil {
		return nil, err
	}
	return nodes, nil
}
