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
	"sync"
	"syscall"
	"time"

	adapter_library "github.com/clearblade/adapter-go-library"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	mqtt "github.com/clearblade/paho.mqtt.golang"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/debug"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// TODO
//  * Implement alarms (events??)
//  * Implement ModifySubscription when implemented by github.com/gopcua
//  * Implement SetPublishingMode when implemented by github.com/gopcua
//  * Implement Republish when implemented by github.com/gopcua
//  * Implement TransferSubscriptions when implemented by github.com/gopcua
//

const (
	adapterName        = "opc-ua-adapter"
	appuri             = "urn:cb-opc-ua-adapter:client"
	readTopic          = "read"
	writeTopic         = "write"
	methodTopic        = "method"
	subscribeTopic     = "subscribe"
	publishTopic       = "publish"
	browseTopic        = "browse"
	browsePathTopic    = "browsePath"
	connectTopic       = "connect"
	errorTopic         = "error"
	browseTagNameTopic = "discover"
	ConnectionPending  = "ConnectionPending"
	ConnectionFailed   = "ConnectionFailed"
	ConnectionSuccess  = "ConnectionSuccess"
	BrowsePending      = "BrowsePending"
	BrowseFailed       = "BrowseFailed"
	BrowseSuccess      = "BrowseSuccess"
	RFC3339Milli       = "2006-01-02T15:04:05.000Z07:00"
)

var (
	adapterSettings        *opcuaAdapterSettings
	adapterConfig          *adapter_library.AdapterConfig
	opcuaClient            *opcua.Client
	openSubscriptions      = make(map[uint32]*opcua.Subscription)
	openSubscriptionsMutex sync.Mutex
	clientHandle           uint32
	clientHandleMutex      sync.Mutex
	clientHandleRequestMap = make(map[uint32]map[uint32]interface{})
	clientHandleMapMutex   sync.Mutex
	eventFieldNames        = []string{"EventId", "EventType", "SourceNode", "SourceName", "Time", "ReceiveTime", "LocalTime", "Message", "Severity"}
	opcuaConnected         = false
	stateMutex             sync.Mutex
	retryCounter           = 0
	keepAliveRetries       = 3
)

type NodeDef struct {
	NodeID                  *ua.NodeID
	ParentNodeID            *ua.NodeID
	Level                   int
	NodeClass               ua.NodeClass
	BrowseName              string
	Description             string
	AccessLevel             ua.AccessLevelType
	Path                    string
	DataType                string
	Writable                bool
	Unit                    string
	Scale                   string
	Min                     string
	Max                     string
	DisplayName             string
	WriteMask               string
	UserWriteMask           string
	IsAbstract              bool
	Symmetric               string
	InverseName             string
	ContainsNoLoops         bool
	EventNotifier           []byte
	Value                   string
	ValueRank               int64
	ArrayDimensions         string
	UserAccessLevel         string
	MinimumSamplingInterval string
	Historizing             string
	Executable              bool
	UserExecutable          bool
	DataTypeDefinition      string
	RolePermissions         string
	UserRolePermissions     string
	AccessRestrictions      string
	AccessLevelEx           string
}

func main() {
	var err error

	err = adapter_library.ParseArguments(adapterName)
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

	// Handle older systems without relay setting in adapter_config
	if adapterSettings.UseRelay == nil {
		useRelayDefault := true
		adapterSettings.UseRelay = &useRelayDefault
	}

	// Handle newer systems with keepAliveRetries setting in adapter_config
	if adapterSettings.KeepAliveRetries != nil {
		keepAliveRetries = *adapterSettings.KeepAliveRetries
	}

	connectMQTTWithBackoff()

	opcuaClient = initializeOPCUA()

	// create a dummy subscription to allow a single subscription to be deleted without error
	nc := make(chan *opcua.PublishNotificationData)

	sp := &opcua.SubscriptionParameters{
		Interval: time.Minute,
	}
	ds, err := opcuaClient.SubscribeWithContext(context.Background(), sp, nc)
	if err != nil {
		log.Fatalf("[FATAL] Failure creating dummy subscription: %s\n", err.Error())
	}

	sc := make(chan struct{})
	var dummyWg sync.WaitGroup
	dummyWg.Add(1)

	go func(notifyCh <-chan *opcua.PublishNotificationData, stopChan chan struct{}, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			case <-notifyCh: // drop notifications
			}
		}
	}(nc, sc, &dummyWg)

	go checkStateAndKeepAlive()

	// wait for signal to stop/kill process to allow for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	sig := <-c

	log.Printf("[INFO] OS signal %s received, gracefully shutting down adapter.\n", sig)

	close(sc)

	dummyWg.Wait()

	err = ds.Cancel(context.Background())
	if err != nil {
		log.Printf("[ERROR] Failed to cancel dummy subscription: %s\n", err.Error())
	}

	err = opcuaClient.Close()
	if err != nil {
		log.Printf("[ERROR] Failed to close OPC UA Session: %s\n", err.Error())
		os.Exit(1)
	}

	os.Exit(0)

}

func connectMQTTWithBackoff() {
	baseDelay := 100 * time.Millisecond
	maxDelay := 10 * time.Second
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := adapter_library.ConnectMQTT(adapterConfig.TopicRoot+"/#", cbMessageHandler)
		if err == nil {
			log.Printf("[INFO] Successfully connected to MQTT broker on attempt %d", attempt+1)
			return
		}

		if attempt == maxAttempts-1 {
			log.Fatalf("[FATAL] Failed to connect MQTT after %d attempts: %s\n", maxAttempts, err.Error())
		}

		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > maxDelay {
			delay = maxDelay
		}

		log.Printf("[INFO] Connection attempt %d failed: %s. Retrying in %v",
			attempt+1, err.Error(), delay)
		time.Sleep(delay)
	}
}

func checkStateAndKeepAlive() {
	for {
		time.Sleep(time.Second * 5)

		connectionStatus := adapter_library.ConnectionStatus{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Status:    ConnectionFailed,
		}

		mqttConnectionResp := opcuaConnectionResponseMQTTMessage{
			ConnectionStatus: connectionStatus,
		}
		var nodeID = "i=85"

		id, err := ua.ParseNodeID(nodeID)
		if err != nil {
			mqttConnectionResp.ConnectionStatus.ErrorMessage = "KeepAlive invalid node id: " + err.Error()
			mqttConnectionResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr := returnConnectionMessage(&mqttConnectionResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			token.Wait()
			time.Sleep(time.Second * 2)

			stateMutex.Lock()
			retryCounter++
			stateMutex.Unlock()

			if retryCounter > keepAliveRetries {
				log.Fatalf("[FATAL] KeepAlive invalid node id: %v", err)
			} else {
				log.Printf("[ERROR] KeepAlive invalid node id, retry count %d of %d", retryCounter, keepAliveRetries)
				continue
			}
		}

		req := &ua.ReadRequest{
			MaxAge:             2000,
			NodesToRead:        []*ua.ReadValueID{{NodeID: id, AttributeID: 1}},
			TimestampsToReturn: ua.TimestampsToReturnBoth,
		}

		resp, err := opcuaClient.Read(req)
		if err != nil {
			mqttConnectionResp.ConnectionStatus.ErrorMessage = "KeepAlive Read failed: " + err.Error()
			mqttConnectionResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr := returnConnectionMessage(&mqttConnectionResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}

			sourceDown := false
			networkDown := false
			invalidSession := false

			if strings.Contains(err.Error(), "StatusBadServerNotConnected") {
				sourceDown = true
			} else if strings.Contains(err.Error(), "StatusBadTimeout") {
				networkDown = true
			} else if strings.Contains(err.Error(), "StatusBadSessionIDInvalid") {
				invalidSession = true
			}
			errorResp := errorDownMQTTMessage{
				SourceDown:     sourceDown,
				NetworkDown:    networkDown,
				InvalidSession: invalidSession,
				Message:        err.Error(),
			}
			returnErrorMessage(&errorResp, *adapterSettings.UseRelay)
			token.Wait()
			time.Sleep(time.Second * 2)
			stateMutex.Lock()
			retryCounter++
			stateMutex.Unlock()

			if retryCounter > keepAliveRetries {
				log.Fatalf("[FATAL] KeepAlive Read failed: %s", err)
			} else {
				log.Printf("[ERROR] Keepalive Read failed, retry count %d of %d", retryCounter, keepAliveRetries)
				continue
			}
		}
		if resp.Results[0].Status != ua.StatusOK {
			mqttConnectionResp.ConnectionStatus.ErrorMessage = "KeepAlive Status not OK: " + fmt.Sprint(resp.Results[0].Status)
			mqttConnectionResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr := returnConnectionMessage(&mqttConnectionResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			token.Wait()
			time.Sleep(time.Second * 2)
			stateMutex.Lock()
			retryCounter++
			stateMutex.Unlock()

			if retryCounter > keepAliveRetries {
				log.Fatalf("[FATAL] KeepAlive Status not OK: %v", resp.Results[0].Status)
			} else {
				log.Printf("[ERROR] KeepAlive Status not OK, retry count %d of %d", retryCounter, keepAliveRetries)
				continue
			}
		}
		log.Printf("[DEBUG] KeepAlive at server Timestamp %v", resp.Results[0].ServerTimestamp)

		if opcuaClient.State() == opcua.Closed || opcuaClient.State() == opcua.Disconnected || opcuaClient.State() == opcua.Reconnecting {
			mqttConnectionResp.ConnectionStatus.ErrorMessage = "OPCUA Client state: " + opcuaClient.State().String()
			mqttConnectionResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr := returnConnectionMessage(&mqttConnectionResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			token.Wait()
			time.Sleep(time.Second * 2)
			stateMutex.Lock()
			retryCounter++
			stateMutex.Unlock()

			if retryCounter > keepAliveRetries {
				log.Fatalf("[FATAL] OPCUA Client state: %s", opcuaClient.State().String())
			} else {
				log.Printf("[ERROR] OPCUA Client state: %s, retry count %d of %d", opcuaClient.State().String(), retryCounter, keepAliveRetries)
				continue
			}
		}
	}
}

func initializeOPCUA() *opcua.Client {
	log.Println("[INFO] initializeOPCUA - Creating OPC UA Session")

	stateMutex.Lock()
	opcuaConnected = false
	stateMutex.Unlock()

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

	_, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}

	ctx := context.Background()

	endpoints, err := opcua.GetEndpoints(ctx, adapterSettings.EndpointURL)
	if err != nil {
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to get OPC UA Server endpoints: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		time.Sleep(time.Second * 2)
		log.Fatalf("[FATAL] Failed to get OPC UA Server endpoints: %s\n", err.Error())
	}

	var authMode ua.UserTokenType

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
		token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		time.Sleep(time.Second * 2)
		log.Fatalln("[FATAL] Certificate auth type not implemented yet")
	default:
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Invalid auth type: " + adapterSettings.Authentication.Type
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		time.Sleep(time.Second * 2)
		log.Fatalf("[FATAL] Invalid auth type: %s\n", adapterSettings.Authentication.Type)
	}

	var secMode ua.MessageSecurityMode

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
		token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		time.Sleep(time.Second * 2)
		log.Fatalf("[FATAL] Invalid security mode: %s\n", adapterSettings.SecurityMode)
	}

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
		token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		time.Sleep(time.Second * 2)
		log.Fatalf("[FATAL] Invalid security policy: %s\n", adapterSettings.SecurityPolicy)
	}

	if certsRequired {
		generateCert(appuri, 2048, "cert.pem", "key.pem")
		c, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
		if err != nil {
			mqttResp.ConnectionStatus.Status = ConnectionFailed
			mqttResp.ConnectionStatus.ErrorMessage = "Failed to load certificates: " + err.Error()
			mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			token.Wait()
			time.Sleep(time.Second * 2)
			log.Fatalf("[FATAL] Failed to load certificates: %s\n", err.Error())
		}
		pk, ok := c.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			mqttResp.ConnectionStatus.Status = ConnectionFailed
			mqttResp.ConnectionStatus.ErrorMessage = "Invalid Private key: " + err.Error()
			mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			token.Wait()
			time.Sleep(time.Second * 2)
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
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to find a matching server endpoint with sec-policy " + secPolicy + " and sec-mode " + secMode.String()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		time.Sleep(time.Second * 2)
		log.Fatalf("[FATAL] Failed to find a matching server endpoint with sec-policy %s and sec-mode %s\n", secPolicy, secMode)
	}

	opcuaOpts = append(opcuaOpts, opcua.SecurityFromEndpoint(serverEndpoint, authMode))
	opcuaOpts = append(opcuaOpts, opcua.AutoReconnect(true))

	log.Printf("[INFO] Connecting to OPC server address %s\n", adapterSettings.EndpointURL)
	c := opcua.NewClient(adapterSettings.EndpointURL, opcuaOpts...)
	if err := c.Connect(ctx); err != nil {
		mqttResp.ConnectionStatus.Status = ConnectionFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to connect to OPC UA Server: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr := returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()
		time.Sleep(time.Second * 2)
		log.Fatalf("[FATAL] Failed to connect to OPC UA Server: %s\n", err.Error())
	}
	mqttResp.ConnectionStatus.Status = ConnectionSuccess
	mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
	_, pubErr = returnConnectionMessage(&mqttResp, *adapterSettings.UseRelay)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}

	stateMutex.Lock()
	opcuaConnected = true
	stateMutex.Unlock()

	if adapterSettings.RegisterCustomObjects != nil {
		if *adapterSettings.RegisterCustomObjects {
			registerCustomObjects()
		}
	}

	return c
}

func registerCustomObjects() {
	log.Println("[DEBUG] Registering Custom Objects")

	// Create whatever structs are required
	// EXAMPLE:

	// type customStruct struct {
	// 	Foo string
	// 	Bar uint32
	// 	Baz bool
	// }

	// type SteeringBehaviourData struct {
	// 	AzimuthBias   float64
	// 	BuildRate     float64
	// 	KiBuild       float64
	// 	KiWalk        float64
	// 	TendancyBuild float64
	// 	TendancyWalk  float64
	// 	TurnRate      float64
	// }

	type customStruct struct {
		Foo string
		Bar uint32
		Baz bool
	}

	ua.RegisterExtensionObject(ua.NewStringNodeID(2, "ComplexTypes/CustomStructTypeVariable"), new([]byte))
	ua.RegisterExtensionObject(ua.NewStringNodeID(2, "DataType.CustomStructType.BinaryEncoding"), new(customStruct))

	// Register them with the server

	// EXAMPLE:

	// ua.RegisterExtensionObject(ua.NewStringNodeID(2, "ComplexTypes/CustomStructTypeVariable"), new([]byte))
	// ua.RegisterExtensionObject(ua.NewStringNodeID(2, "DataType.CustomStructType.BinaryEncoding"), new(customStruct))
	// ua.RegisterExtensionObject(ua.NewStringNodeID(2, "Test/SteeringWhelBehaviorDataVariable"), new(SteeringBehaviourData))
}

func cbMessageHandler(message *mqttTypes.Publish) {
	//Determine the type of request that was received
	stateMutex.Lock()
	isConnected := opcuaConnected
	stateMutex.Unlock()

	if isConnected {
		if strings.Contains(message.Topic.Whole, "response") {
			log.Println("[DEBUG] cbMessageHandler - Received response, ignoring")
		} else if strings.Contains(message.Topic.Whole, readTopic) {
			log.Println("[INFO] cbMessageHandler - Received OPC UA read request")
			go handleReadRequest(message)
		} else if strings.Contains(message.Topic.Whole, writeTopic) {
			log.Println("[INFO] cbMessageHandler - Received OPC UA write request")
			go handleWriteRequest(message)
		} else if strings.Contains(message.Topic.Whole, methodTopic) {
			log.Println("[INFO] cbMessageHandler - Received OPC UA method request")
			go handleMethodRequest(message)
		} else if strings.Contains(message.Topic.Whole, subscribeTopic) {
			log.Println("[INFO] cbMessageHandler - Received OPC UA subscription request")
			go handleSubscriptionRequest(message)
		} else if strings.Contains(message.Topic.Whole, browsePathTopic) {
			log.Println("[INFO] cbMessageHandler - Recieved OPC UA browse path request")
			go handleBrowsePathRequest(message)
		} else if strings.Contains(message.Topic.Whole, browseTopic) {
			log.Println("[INFO] cbMessageHandler - Recieved OPC UA browse request")
			go handleBrowseRequest(message)
		} else if strings.Contains(message.Topic.Whole, connectTopic) {
			log.Println("[INFO] cbMessageHandler - Received OPC UA connect request")
			go handleConnectRequest(message)
		} else if strings.Contains(message.Topic.Whole, browseTagNameTopic) {
			log.Println("[INFO] cbMessageHandler - Recieved OPC UA browse by tag name request")
			go handleBrowseByTagNameRequest(message)
		} else {
			log.Printf("[ERROR] cbMessageHandler - Unknown request received: topic = %s, payload = %#v\n", message.Topic.Whole, message.Payload)
		}
	} else {
		if strings.Contains(message.Topic.Whole, "response") {
			log.Println("[DEBUG] cbMessageHandler - Received response, ignoring")
		} else if strings.Contains(message.Topic.Whole, connectTopic) {
			log.Println("[INFO] cbMessageHandler - Received OPC UA connect request")
			go handleConnectRequest(message)
		} else {
			log.Printf("[ERROR] cbMessageHandler - Unknown request received: topic = %s, payload = %#v\n", message.Topic.Whole, message.Payload)
		}
	}

}

func handleReadRequest(message *mqttTypes.Publish) {

	mqttResp := opcuaReadResponseMQTTMessage{
		EdgeId:          adapter_library.Args.EdgeName,
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

	// Track if any nodes failed
	anyFailed := false
	var errorMessages []string

	for idx, result := range opcuaResp.Results {
		if result.Status == ua.StatusOK {
			mqttResp.ServerTimestamp = result.ServerTimestamp.Format(RFC3339Milli)
			mqttResp.Data[readReq.NodeIDs[idx]] = opcuaReadResponseData{
				Value:           result.Value.Value(),
				SourceTimestamp: result.SourceTimestamp.Format(RFC3339Milli),
			}
		} else {
			anyFailed = true
			errorMsg := fmt.Sprintf("Read Status not OK for node id %s: %+v", readReq.NodeIDs[idx], result.Status)
			log.Printf("[ERROR] %s", errorMsg)
			errorMessages = append(errorMessages, errorMsg)
		}
	}

	if anyFailed {
		mqttResp.Success = false
		mqttResp.ErrorMessage = strings.Join(errorMessages, "; ")
	}

	if len(mqttResp.Data) == 0 {
		log.Println("[INFO] No data received, nothing to publish")
		return
	}

	publishJson(adapterConfig.TopicRoot+"/"+readTopic+"/response", mqttResp)
}

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
			writeReq.Value = convertedArray
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
			writeReq.Value = convertedArray
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
		_, err = getConvertedValue(nodeType, val)
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
			{
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
	mqttResp.Timestamp = resp.ResponseHeader.Timestamp.UTC().Format(RFC3339Milli)
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

func handleMethodRequest(message *mqttTypes.Publish) {
	methodReq := opcuaMethodRequestMQTTMessage{}

	mqttResp := opcuaMethodResponseMQTTMessage{
		ObjectID:       "",
		MethodID:       "",
		Timestamp:      "",
		Success:        true,
		StatusCode:     0,
		ErrorMessage:   "",
		InputArguments: []opcuaMethodInputArguments{},
		OutputValues:   []interface{}{},
	}

	err := json.Unmarshal(message.Payload, &methodReq)
	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Failed to unmarshal method request JSON: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	mqttResp.ObjectID = methodReq.ObjectID
	mqttResp.MethodID = methodReq.MethodID
	mqttResp.InputArguments = methodReq.InputArguments

	objId, err := ua.ParseNodeID(methodReq.ObjectID)
	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Failed to parse OPC UA Object ID: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	methodId, err := ua.ParseNodeID(methodReq.MethodID)
	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Failed to parse OPC UA Method ID: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	req := &ua.CallMethodRequest{
		ObjectID:       objId,
		MethodID:       methodId,
		InputArguments: []*ua.Variant{},
	}

	// We need to loop through the input arguments and create variants for each one
	for _, element := range methodReq.InputArguments {
		switch element.Type {
		case "boolean":
			v, err := ua.NewVariant(element.Value.(bool))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create boolean variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "int16":
			v, err := ua.NewVariant(int16(element.Value.(float64)))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create int16 variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "uint16":
			v, err := ua.NewVariant(uint16(element.Value.(float64)))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create uint16 variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "int32":
			v, err := ua.NewVariant(int32(element.Value.(float64)))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create int32 variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "uint32":
			v, err := ua.NewVariant(uint32(element.Value.(float64)))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create uint32 variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "int64":
			v, err := ua.NewVariant(int64(element.Value.(float64)))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create int64 variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "uint64":
			v, err := ua.NewVariant(uint64(element.Value.(float64)))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create uint64 variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "float":
			v, err := ua.NewVariant(float32(element.Value.(float64)))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create float variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "double":
			v, err := ua.NewVariant(element.Value.(float64))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create double variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "string":
			v, err := ua.NewVariant(element.Value.(string))
			if err != nil {
				log.Printf("[ERROR] handleMethodRequest - Failed to create string variant: %s\n", err.Error())
				returnMethodError(err.Error(), &mqttResp)
				return
			}
			req.InputArguments = append(req.InputArguments, v)
		case "node":
			nodeDetails := opcuaMethodInputArgumentNodeType{
				Namespace:      uint16(element.Value.(map[string]interface{})["namespace"].(float64)),
				IdentifierType: element.Value.(map[string]interface{})["identifier_type"].(string),
			}
			switch nodeDetails.IdentifierType {
			case "numeric":
				nodeDetails.Identifier = uint32(element.Value.(map[string]interface{})["identifier"].(float64))
				nodeID := ua.NewNumericNodeID(nodeDetails.Namespace, nodeDetails.Identifier.(uint32))
				v, err := ua.NewVariant(nodeID)
				if err != nil {
					log.Printf("[ERROR] handleMethodRequest - Failed to create numeric node id variant: %s\n", err.Error())
					returnMethodError(err.Error(), &mqttResp)
					return
				}
				req.InputArguments = append(req.InputArguments, v)
			case "string":
				nodeDetails.Identifier = element.Value.(map[string]interface{})["identifier"].(string)
				nodeID := ua.NewStringNodeID(nodeDetails.Namespace, nodeDetails.Identifier.(string))
				v, err := ua.NewVariant(nodeID)
				if err != nil {
					log.Printf("[ERROR] handleMethodRequest - Failed to create string node id variant: %s\n", err.Error())
					returnMethodError(err.Error(), &mqttResp)
					return
				}
				req.InputArguments = append(req.InputArguments, v)
			default:
				log.Printf("[ERROR] handleMethodRequest - Unsupported node identifier type provided: %s\n", nodeDetails.IdentifierType)
				returnMethodError("Invalid node identifier", &mqttResp)
				return
			}
		default:
			log.Printf("[ERROR] handleMethodRequest - Unimplemented input argument type provided: %s\n", element.Type)
			returnMethodError("Invalid input argument type", &mqttResp)
			return
		}
	}

	resp, err := opcuaClient.Call(req)

	if err != nil {
		log.Printf("[ERROR] handleMethodRequest - Error invoking OPC UA method: %s\n", err.Error())
		returnMethodError(err.Error(), &mqttResp)
		return
	}

	mqttResp.Timestamp = time.Now().UTC().Format(RFC3339Milli)
	mqttResp.StatusCode = uint32(resp.StatusCode)
	if resp.StatusCode != ua.StatusOK {
		log.Printf("[ERROR] handleMethodRequest - Bad status code returned invoking OPC UA method: %s\n", resp.StatusCode)
		returnMethodError("Bad status code returned", &mqttResp)
		return
	}

	for _, element := range resp.OutputArguments {
		mqttResp.OutputValues = append(mqttResp.OutputValues, element.XMLElement())
	}

	publishJson(adapterConfig.TopicRoot+"/"+methodTopic+"/response", &mqttResp)
}

// OPC UA Subscription Service Set
func handleSubscriptionRequest(message *mqttTypes.Publish) {
	subReq := opcuaSubscriptionRequestMQTTMessage{}

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

// OPC UA Subscription Service Set - Create
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
		RequestType: SubscriptionCreate,
		Timestamp:   "",
		Success:     true,
		// StatusCode:   0,
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

	openSubscriptionsMutex.Lock()
	openSubscriptions[sub.SubscriptionID] = sub
	openSubscriptionsMutex.Unlock()

	clientHandleMapMutex.Lock()
	clientHandleRequestMap[sub.SubscriptionID] = make(map[uint32]interface{})
	clientHandleMapMutex.Unlock()

	log.Printf("[DEBUG] createSubscription - Subscription ID: %+v\n", sub)

	resp.SubscriptionID = sub.SubscriptionID

	defer sub.Cancel(context.Background())
	log.Printf("[INFO] createSubscription - Created subscription with id %d", sub.SubscriptionID)

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
			log.Println("[DEBUG] createSubscription - creating monitored item value request")

			miCreateRequests = append(miCreateRequests, opcua.NewMonitoredItemCreateRequestWithDefaults(nodeId, ua.AttributeIDValue, getClientHandle()))
			clientHandleMapMutex.Lock()
			clientHandleRequestMap[sub.SubscriptionID][clientHandle] = item
			clientHandleMapMutex.Unlock()
		}

		if item.Events {
			log.Println("[DEBUG] createSubscription - creating monitored item event request")
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
			clientHandleMapMutex.Lock()
			clientHandleRequestMap[sub.SubscriptionID][clientHandle] = item
			clientHandleMapMutex.Unlock()
		}

		clientHandleMapMutex.Lock()
		clientHandleRequestMap[sub.SubscriptionID][clientHandle] = item
		clientHandleMapMutex.Unlock()
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
	resp.Timestamp = time.Now().UTC().Format(RFC3339Milli)

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
					Timestamp:      time.Now().UTC().Format(RFC3339Milli),
					Success:        true,
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
					clientHandleMapMutex.Lock()
					monitoredItem := clientHandleRequestMap[sub.SubscriptionID][item.ClientHandle].(opcuaMonitoredItemCreateMQTTMessage)
					clientHandleMapMutex.Unlock()
					resp.Results = append(resp.Results, opcuaMonitoredItemNotificationMQTTMessage{
						NodeID:     monitoredItem.NodeID,
						StatusCode: uint32(item.Value.Status),
						Value:      item.Value.Value.Value(),
					})
				}

				publishJson(adapterConfig.TopicRoot+"/"+publishTopic+"/response", &resp)
			case *ua.EventNotificationList:
				resp := opcuaSubscriptionResponseMQTTMessage{
					RequestType:    SubscriptionPublish,
					Timestamp:      time.Now().UTC().Format(RFC3339Milli),
					Success:        true,
					ErrorMessage:   "",
					Results:        []interface{}{},
					SubscriptionID: sub.SubscriptionID,
				}
				for _, item := range x.Events {

					timeVal := ""
					receiveTimeVal := ""
					if item.EventFields[4].Value() != nil {
						timeVal = item.EventFields[4].Value().(time.Time).UTC().Format(RFC3339Milli)
					}
					if item.EventFields[5].Value() != nil {
						receiveTimeVal = item.EventFields[5].Value().(time.Time).UTC().Format(RFC3339Milli)
					}

					clientHandleMapMutex.Lock()
					monitoredItem := clientHandleRequestMap[sub.SubscriptionID][item.ClientHandle].(opcuaMonitoredItemCreateMQTTMessage)
					clientHandleMapMutex.Unlock()

					resp.Results = append(resp.Results, opcuaMonitoredItemNotificationMQTTMessage{
						NodeID: monitoredItem.NodeID,
						Event: opcuaEventMessage{
							EventID:     hex.EncodeToString(item.EventFields[0].Value().([]uint8)),
							EventType:   id.Name(item.EventFields[1].Value().(*ua.NodeID).IntID()),
							SourceNode:  item.EventFields[2].Value().(*ua.NodeID).String(),
							SourceName:  item.EventFields[3].Value().(string),
							Time:        timeVal,
							ReceiveTime: receiveTimeVal,
							LocalTime:   item.EventFields[6].Value(),
							Message:     item.EventFields[7].Value().(*ua.LocalizedText).Text,
							Severity:    uint32(item.EventFields[8].Value().(uint16)),
							// StatusCode:  uint32(item),
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

// OPC UA Subscription Service Set - Delete
func handleSubscriptionDelete(subReq *opcuaSubscriptionRequestMQTTMessage) {
	jsonString, _ := json.Marshal(*subReq.RequestParams)
	parms := opcuaSubscriptionDeleteParmsMQTTMessage{}
	json.Unmarshal(jsonString, &parms)

	resp := opcuaSubscriptionResponseMQTTMessage{
		//NodeID:       subReq.NodeID,
		RequestType: SubscriptionDelete,
		Timestamp:   time.Now().UTC().Format(RFC3339Milli),
		Success:     true,
		// StatusCode:     0,
		ErrorMessage:   "",
		SubscriptionID: parms.SubscriptionID,
	}

	openSubscriptionsMutex.Lock()
	subscription, ok := openSubscriptions[parms.SubscriptionID]
	openSubscriptionsMutex.Unlock()

	if !ok {
		log.Printf("[ERROR] handleSubscriptionDelete - No active subscription for ID: %d\n", parms.SubscriptionID)
		returnSubscribeError(fmt.Sprintf("No active subscription for ID: %d", parms.SubscriptionID), &resp)
		return
	}

	log.Printf("[DEBUG] handleSubscriptionDelete - Deleting subscription: %d\n", parms.SubscriptionID)
	err := subscription.Cancel(context.Background())

	if err != nil && err != ua.StatusOK {
		log.Printf("[ERROR] handleSubscriptionDelete - Error occurred while deleting subscription: %s\n", err.Error())
		returnSubscribeError(err.Error(), &resp)
		return
	}

	log.Println("[DEBUG] handleSubscriptionDelete - Subscription deleted")

	publishJson(adapterConfig.TopicRoot+"/"+subscribeTopic+"/response", resp)

	clientHandleMapMutex.Lock()
	delete(clientHandleRequestMap, parms.SubscriptionID)
	clientHandleMapMutex.Unlock()

	openSubscriptionsMutex.Lock()
	delete(openSubscriptions, parms.SubscriptionID)
	openSubscriptionsMutex.Unlock()
}

func handleBrowseByTagNameRequest(message *mqttTypes.Publish) {
	connectionStatus := adapter_library.ConnectionStatus{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Status:    BrowsePending,
	}

	mqttResp := opcuaBrowseTagNameResponseMQTTMessage{
		NodeIDs:          "",
		ConnectionStatus: connectionStatus,
	}

	_, pubErr := returnBrowseNameMessage(&mqttResp, *adapterSettings.UseRelay)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}

	browseNameReq := opcuaBrowseTagNameRequestMQTTMessage{}
	err := json.Unmarshal(message.Payload, &browseNameReq)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to unmarshal request JSON: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowseNameMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Failed to unmarshal request JSON: %s\n", err.Error())
		return
	}
	nid, err := ua.ParseNodeID(browseNameReq.RootNode)
	if err != nil {
		log.Fatalf("invalid node id: %s\n", err.Error())
	}

	n := opcuaClient.Node(nid)
	rootNodeBrowseName, err := n.BrowseName()
	if err != nil {
		log.Printf("[ERROR] Failed to get browse name for root node id : %s\n", err)
	}
	log.Printf("Root Node = %s, BrowseName = %s\n", nid, rootNodeBrowseName.Name)
	ns, err := n.TranslateBrowsePathInNamespaceToNodeID(browseNameReq.NamespaceIndex, browseNameReq.TagName)
	if err != nil {
		log.Fatalf("invalid  TranslateBrowsePathInNamespaceToNodeID: %s", err.Error())
	}
	log.Printf("Tag Name = %s, Node ID = %s \n", browseNameReq.TagName, ns.String())
	mqttResp.NodeIDs = ns.String()
	mqttResp.ConnectionStatus.Status = BrowseSuccess
	mqttResp.ConnectionStatus.ErrorMessage = ""
	mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
	_, pubErr = returnBrowseNameMessage(&mqttResp, *adapterSettings.UseRelay)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish browse name message: %s\n", pubErr.Error())
	}
}

// buildNestedNodeTree converts a flat list of nodes into a hierarchical tree structure
func buildNestedNodeTree(nodeList []NodeDef) []NestedNode {
	nodeDataMap := make(map[string]NodeDef)

	childrenMap := make(map[string][]string)

	// First pass: build node data map and parent-child relationships
	for _, node := range nodeList {
		nodeID := node.NodeID.String()
		nodeDataMap[nodeID] = node

		// Record parent-child relationship if parent exists
		if node.ParentNodeID != nil {
			parentID := node.ParentNodeID.String()
			childrenMap[parentID] = append(childrenMap[parentID], nodeID)
		}
	}

	// Function to recursively build nested nodes
	var buildNode func(nodeID string, visited map[string]bool) NestedNode
	buildNode = func(nodeID string, visited map[string]bool) NestedNode {
		// Prevent infinite recursion due to circular references
		if visited[nodeID] {
			return NestedNode{
				NodeId:     nodeID,
				BrowseName: "Circular Reference",
			}
		}

		visited[nodeID] = true

		nodeData, exists := nodeDataMap[nodeID]
		if !exists {
			return NestedNode{
				NodeId:     nodeID,
				BrowseName: "Unknown Node",
			}
		}

		// Create nested node with only essential fields
		nestedNode := NestedNode{
			NodeId:     nodeID,
			BrowseName: nodeData.BrowseName,
			Children:   []NestedNode{},
		}

		// Recursively build children
		childIDs := childrenMap[nodeID]
		for _, childID := range childIDs {
			// Create a new visited map for each child to allow the same node to appear
			// in multiple branches of the tree
			childVisited := make(map[string]bool)
			for k, v := range visited {
				childVisited[k] = v
			}

			childNode := buildNode(childID, childVisited)
			nestedNode.Children = append(nestedNode.Children, childNode)
		}

		return nestedNode
	}

	// Build root nodes
	var rootNodes []NestedNode
	for _, node := range nodeList {
		nodeID := node.NodeID.String()

		// Check if this node is a root node (has no parent or parent is not in our list)
		isRoot := node.ParentNodeID == nil
		if !isRoot {
			parentID := node.ParentNodeID.String()
			_, parentExists := nodeDataMap[parentID]
			isRoot = !parentExists
		}

		if isRoot {
			visited := make(map[string]bool)
			rootNode := buildNode(nodeID, visited)
			rootNodes = append(rootNodes, rootNode)
		}
	}

	return rootNodes
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

	_, pubErr := returnBrowseMessage(&mqttResp, *adapterSettings.UseRelay)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}

	browseReq := opcuaBrowseRequestMQTTMessage{}
	err := json.Unmarshal(message.Payload, &browseReq)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to unmarshal request JSON: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowseMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Failed to unmarshal request JSON: %s\n", err.Error())
		return
	}

	if browseReq.RootNode == "" {
		browseReq.RootNode = "i=84"
	}

	if browseReq.LevelLimit == 0 {
		browseReq.LevelLimit = 10
	}

	id, err := ua.ParseNodeID(browseReq.RootNode)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Invalid node id: " + err.Error() + ", RootId: " + browseReq.RootNode
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowseMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Invalid node id: %s, RootId: %s", err.Error(), browseReq.RootNode)
		return
	}

	var nodeList []NodeDef
	var wg sync.WaitGroup

	wg.Add(1)
	go browse(&wg, &nodeList, opcuaClient.Node(id), nil, "", 0, browseReq.LevelLimit, &mqttResp)

	wg.Wait()

	if mqttResp.ConnectionStatus.Status != BrowseFailed {
		useNestedView := browseReq.NestedView

		if useNestedView {
			nestedResp := NestedBrowseResponseMQTTMessage{
				ConnectionStatus: connectionStatus,
			}

			nestedResp.Nodes = buildNestedNodeTree(nodeList)

			nestedResp.ConnectionStatus.Status = BrowseSuccess
			nestedResp.ConnectionStatus.ErrorMessage = ""
			nestedResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)

			_, pubErr = returnNestedBrowseMessage(&nestedResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish nested browse message: %s\n", pubErr.Error())
			}
		} else if browseReq.Attributes != nil {
			// Existing attributes-based response
			mqttResp := opcuaBrowseResponseWithAttrsMQTTMessage{
				Nodes:    make([]node, 0),
				NodeList: browseReq.NodeList,
			}

			if !contains(nodeList, browseReq.RootNode, 0) {
				mqttResp.Nodes = append(mqttResp.Nodes, node{
					NodeId:       browseReq.RootNode,
					Level:        0,
					BrowseName:   browseReq.RootNode,
					ParentNodeID: "",
				})
			}

			for _, s := range nodeList {
				log.Println("[DEBUG] NodeID: " + s.NodeID.String())

				node := node{}
				node.NodeId = s.NodeID.String()
				if s.ParentNodeID != nil {
					node.ParentNodeID = s.ParentNodeID.String()
				}
				node.BrowseName = s.BrowseName
				node.Level = s.Level
				for _, a := range *browseReq.Attributes {
					switch a {
					case "NodeClass":
						node.NodeClass = s.NodeClass.String()
					case "BrowseName":
						//already set
						// node.BrowseName = s.BrowseName
					case "Description":
						node.Description = s.Description
					case "AccessLevel":
						node.AccessLevel = s.AccessLevel.String()
					// case "Path":
					// 	node.Path = s.Path
					case "DataType":
						node.DataType = s.DataType
					// case "Writable":
					// 	node.Writable = s.Writable
					// case "Unit":
					// 	node.Unit = s.Unit
					// case "Scale":
					// 	node.Scale = s.Scale
					// case "Min":
					// 	node.Min = s.Min
					// case "Max":
					// 	node.Max = s.Max
					case "DisplayName":
						node.DisplayName = s.DisplayName
					// case "WriteMask":
					// 	node.WriteMask = s.WriteMask
					// case "UserWriteMask":
					// 	node.UserWriteMask = s.UserWriteMask
					// case "IsAbstract":
					// 	node.IsAbstract = s.IsAbstract
					// case "Symmetric":
					// 	node.Symmetric = s.Symmetric
					// case "InverseName":
					// 	node.InverseName = s.InverseName
					// case "ContainsNoLoops":
					// 	node.ContainsNoLoops = s.ContainsNoLoops
					// case "EventNotifier":
					// 	node.EventNotifier = string(s.EventNotifier)
					// case "Value":
					// 	node.Value = s.Value
					// case "ValueRank":
					// 	node.ValueRank = s.ValueRank
					// case "ArrayDimensions":
					// 	node.ArrayDimensions = string(s.ArrayDimensions)
					// case "UserAccessLevel":
					// 	node.UserAccessLevel = s.UserAccessLevel
					// case "MinimumSamplingInterval":
					// 	node.MinimumSamplingInterval = s.MinimumSamplingInterval
					// case "Historizing":
					// 	node.Historizing = s.Historizing
					// case "Executable":
					// 	node.Executable = s.Executable
					// case "UserExecutable":
					// 	node.UserExecutable = s.UserExecutable
					// case "DataTypeDefinition":
					// 	node.DataTypeDefinition = s.DataTypeDefinition
					// case "RolePermissions":
					// 	node.RolePermissions = s.RolePermissions
					// case "UserRolePermissions":
					// 	node.UserRolePermissions = s.UserRolePermissions
					// case "AccessRestrictions":
					// 	node.AccessRestrictions = s.AccessRestrictions
					// case "AccessLevelEx":
					// 	node.AccessLevelEx = s.AccessLevelEx
					default:
						log.Printf("[ERROR] Unknown Attribute type %s\n", a)
					}
				}

				mqttResp.Nodes = append(mqttResp.Nodes, node)
			}
			mqttResp.ConnectionStatus.Status = BrowseSuccess
			mqttResp.ConnectionStatus.ErrorMessage = ""
			mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			_, pubErr = returnBrowseMessageWithAttrs(&mqttResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}

		} else {
			// Original flat list of node IDs
			for _, s := range nodeList {
				log.Println("[DEBUG] NodeID: " + s.NodeID.String())
				mqttResp.NodeIDs = append(mqttResp.NodeIDs, s.NodeID.String())
			}
			mqttResp.ConnectionStatus.Status = BrowseSuccess
			mqttResp.ConnectionStatus.ErrorMessage = ""
			mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			_, pubErr = returnBrowseMessage(&mqttResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
		}
	}
}

func handleBrowsePathRequest(message *mqttTypes.Publish) {

	connectionStatus := adapter_library.ConnectionStatus{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Status:    BrowsePending,
	}

	mqttResp := opcuaBrowsePathResponseMQTTMessage{
		ConnectionStatus: connectionStatus,
	}

	_, pubErr := returnBrowsePathMessage(&mqttResp, *adapterSettings.UseRelay)
	if pubErr != nil {
		log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
	}

	browseReq := opcuaBrowseRequestMQTTMessage{}
	err := json.Unmarshal(message.Payload, &browseReq)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to unmarshal request JSON: " + err.Error()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowsePathMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Failed to unmarshal request JSON: %s\n", err.Error())
		return
	}

	if browseReq.RootNode == "" {
		browseReq.RootNode = "i=84"
	}

	browseReq.LevelLimit = 20 //arbitrary, to stop unreasonable recursion

	var wg sync.WaitGroup
	// We're using mutex protection inside the browse/browsePath functions

	for _, n := range browseReq.NodeList {
		log.Printf("[INFO] node in loop to browse: %s", n)
		id, err := ua.ParseNodeID(browseReq.RootNode)
		if err != nil {
			mqttResp.ConnectionStatus.Status = BrowseFailed
			mqttResp.ConnectionStatus.ErrorMessage = "Invalid node id: " + err.Error() + ", RootId: " + browseReq.RootNode
			mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			_, pubErr = returnBrowsePathMessage(&mqttResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}
			log.Printf("[ERROR] Invalid node id: %s, RootId: %s", err.Error(), browseReq.RootNode)
			return
		}

		splitPath := strings.Split(n.Path, ".")

		// Add to WaitGroup before launching the goroutine
		wg.Add(1)
		go func(n Node, id *ua.NodeID, splitPath []string) {
			browsePath(&wg, n, opcuaClient.Node(id), nil, 0, browseReq.LevelLimit, &mqttResp, splitPath)
		}(n, id, splitPath)
	}

	// Remove arbitrary sleep
	wg.Wait()

	if mqttResp.ConnectionStatus.Status != BrowseFailed {
		mqttResp.ConnectionStatus.Status = BrowseSuccess
		mqttResp.ConnectionStatus.ErrorMessage = ""
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnBrowsePathMessage(&mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
	}
}

func contains(n []NodeDef, str string, l int) bool {
	for _, v := range n {
		if v.NodeID.String() == str && v.Level == l {
			return true
		}
	}
	return false
}

func sliceContains(s []string, e string) bool {
	for _, a := range s {
		if strings.Contains(a, e) {
			return true
		}
	}
	return false
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
	resp.ServerTimestamp = time.Now().UTC().Format(RFC3339Milli)
	publishJson(adapterConfig.TopicRoot+"/"+readTopic+"/response", resp)
}

func returnWriteError(errMsg string, resp *opcuaWriteResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().UTC().Format(RFC3339Milli)
	publishJson(adapterConfig.TopicRoot+"/"+writeTopic+"/response", resp)
}

func returnMethodError(errMsg string, resp *opcuaMethodResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().UTC().Format(RFC3339Milli)
	publishJson(adapterConfig.TopicRoot+"/"+methodTopic+"/response", resp)
}

func returnSubscribeError(errMsg string, resp *opcuaSubscriptionResponseMQTTMessage) {
	resp.Success = false
	resp.ErrorMessage = errMsg
	resp.Timestamp = time.Now().UTC().Format(RFC3339Milli)
	publishJson(adapterConfig.TopicRoot+"/"+subscribeTopic+"/response", resp)
}

func returnBrowseMessage(resp *opcuaBrowseResponseMQTTMessage, useRelay bool) (mqtt.Token, error) {
	topic := adapterConfig.TopicRoot + "/" + browseTopic + "/response"
	if useRelay {
		topic = topic + "/_platform"
	}
	log.Printf("[DEBUG] returnBrowseMessage - publishing to topic %s with message %s\n", topic, resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(topic, json)
}

func returnBrowsePathMessage(resp *opcuaBrowsePathResponseMQTTMessage, useRelay bool) (mqtt.Token, error) {
	topic := adapterConfig.TopicRoot + "/" + browsePathTopic + "/response"
	if useRelay {
		topic = topic + "/_platform"
	}
	log.Printf("[DEBUG] returnBrowsePathMessage - publishing to topic %s with message %s\n", topic, resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(topic, json)
}

func returnBrowseMessageWithAttrs(resp *opcuaBrowseResponseWithAttrsMQTTMessage, useRelay bool) (mqtt.Token, error) {
	topic := adapterConfig.TopicRoot + "/" + browseTopic + "/response"
	if useRelay {
		topic = topic + "/_platform"
	}
	log.Printf("[DEBUG] returnBrowseMessageWithAttrs - publishing to topic %s with message %s\n", topic, resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(topic, json)
}

func returnBrowseNameMessage(resp *opcuaBrowseTagNameResponseMQTTMessage, useRelay bool) (mqtt.Token, error) {
	topic := adapterConfig.TopicRoot + "/" + browseTagNameTopic + "/response"
	if useRelay {
		topic = topic + "/_platform"
	}
	log.Printf("[DEBUG] returnBrowseNameMessage - publishing to topic %s with message %s\n", topic, resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(topic, json)
}

func returnConnectionMessage(resp *opcuaConnectionResponseMQTTMessage, useRelay bool) (mqtt.Token, error) {
	topic := adapterConfig.TopicRoot + "/" + connectTopic + "/response"
	if useRelay {
		topic = topic + "/_platform"
	}
	log.Printf("[DEBUG] returnConnectionMessage - publishing to topic %s with message %s\n", topic, resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(topic, json)
}

func returnErrorMessage(resp *errorDownMQTTMessage, useRelay bool) error {
	topic := adapterConfig.TopicRoot + "/" + errorTopic + "/response"
	if useRelay {
		topic = topic + "/_platform"
	}
	log.Printf("[DEBUG] returnErrorMessage - publishing to topic %s with message %s\n", topic, resp)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return err
	}

	_, err = adapter_library.PublishStatus(topic, json)
	return err
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
	clientHandleMutex.Lock()
	defer clientHandleMutex.Unlock()
	clientHandle++
	return clientHandle
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

func browse(wg *sync.WaitGroup, nodeList *[]NodeDef, n *opcua.Node, parentNode *opcua.Node, path string, level int, levelLimit int, mqttResp *opcuaBrowseResponseMQTTMessage) {
	defer wg.Done() // Ensure wg.Done() is called when this function returns

	if level > levelLimit {
		return
	}

	log.Printf("[DEBUG] node:%s path:%q level:%d\n", n, path, level)

	attrs, err := n.Attributes(
		ua.AttributeIDNodeClass,
		ua.AttributeIDBrowseName,
		ua.AttributeIDDescription,
		ua.AttributeIDAccessLevel,
		ua.AttributeIDDataType,
		ua.AttributeIDDisplayName,
		// ua.AttributeIDWriteMask,
		// ua.AttributeIDUserWriteMask,
		// ua.AttributeIDIsAbstract,
		// ua.AttributeIDSymmetric,
		// ua.AttributeIDInverseName,
		// ua.AttributeIDContainsNoLoops,
		// ua.AttributeIDEventNotifier,
		// ua.AttributeIDValue,
		// ua.AttributeIDValueRank,
		// ua.AttributeIDArrayDimensions,
		// ua.AttributeIDUserAccessLevel,
		// ua.AttributeIDMinimumSamplingInterval,
		// ua.AttributeIDHistorizing,
		// ua.AttributeIDExecutable,
		// ua.AttributeIDUserExecutable,
		// ua.AttributeIDDataTypeDefinition,
		// ua.AttributeIDRolePermissions,
		// ua.AttributeIDUserRolePermissions,
		// ua.AttributeIDAccessRestrictions,
		// ua.AttributeIDAccessLevelEx,
	)
	if err != nil {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		mqttResp.ConnectionStatus.ErrorMessage = "Failed to browse nodes: " + err.Error() + ", RootId: " + n.ID.String()
		mqttResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr := returnBrowseMessage(mqttResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		log.Printf("[ERROR] Failed to browse nodes: %s, RootId: %s", err.Error(), n.ID.String())

		stateMutex.Lock()
		opcuaConnected = false
		stateMutex.Unlock()

		connectionStatus := adapter_library.ConnectionStatus{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Status:    ConnectionFailed,
		}

		mqttConnectionResp := opcuaConnectionResponseMQTTMessage{
			ConnectionStatus: connectionStatus,
		}
		mqttConnectionResp.ConnectionStatus.ErrorMessage = "Error when creating node attributes array: " + err.Error()
		mqttConnectionResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		token, pubErr := returnConnectionMessage(&mqttConnectionResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		token.Wait()

		time.Sleep(time.Second * 2)
		opcuaClient = initializeOPCUA()

		mqttConnectionResp.ConnectionStatus.Status = ConnectionSuccess
		mqttConnectionResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
		_, pubErr = returnConnectionMessage(&mqttConnectionResp, *adapterSettings.UseRelay)
		if pubErr != nil {
			log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
		}
		stateMutex.Lock()
		opcuaConnected = true
		stateMutex.Unlock()

		return
	}

	if len(attrs) == 0 {
		mqttResp.ConnectionStatus.Status = BrowseFailed
		return
	}

	var def = NodeDef{
		NodeID: n.ID,
		Level:  level,
	}

	if parentNode != nil {
		def.ParentNodeID = parentNode.ID
	}

	switch err := attrs[0].Status; err {
	case ua.StatusOK:
		def.NodeClass = ua.NodeClass(attrs[0].Value.Int())
	default:
		log.Printf("[ERROR] %s", err)
		return
	}

	switch err := attrs[1].Status; err {
	case ua.StatusOK:
		def.BrowseName = attrs[1].Value.String()
	default:
		log.Printf("[ERROR] %s", err)
		return
	}

	switch err := attrs[2].Status; err {
	case ua.StatusOK:
		def.Description = attrs[2].Value.String()
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	default:
		log.Printf("[ERROR] %s", err)
		return
	}

	switch err := attrs[3].Status; err {
	case ua.StatusOK:
		def.AccessLevel = ua.AccessLevelType(attrs[3].Value.Int())
		def.Writable = def.AccessLevel&ua.AccessLevelTypeCurrentWrite == ua.AccessLevelTypeCurrentWrite
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	default:
		log.Printf("[ERROR] %s", err)
		return
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
		log.Printf("[ERROR] %s", err)
		return
	}

	switch err := attrs[5].Status; err {
	case ua.StatusOK:
		def.DisplayName = attrs[5].Value.String()
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	default:
		log.Printf("[ERROR] %s", err)
		return
	}

	// switch err := attrs[6].Status; err {
	// case ua.StatusOK:
	// 	def.WriteMask = attrs[6].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[7].Status; err {
	// case ua.StatusOK:
	// 	def.UserWriteMask = attrs[7].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[7].Status; err {
	// case ua.StatusOK:
	// 	def.IsAbstract = attrs[7].Value.Bool()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[8].Status; err {
	// case ua.StatusOK:
	// 	def.Symmetric = attrs[8].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[8].Status; err {
	// case ua.StatusOK:
	// 	def.InverseName = attrs[8].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[9].Status; err {
	// case ua.StatusOK:
	// 	def.ContainsNoLoops = attrs[9].Value.Bool()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[12].Status; err {
	// case ua.StatusOK:
	// 	def.EventNotifier = attrs[12].Value.ByteArray()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[9].Status; err {
	// case ua.StatusOK:
	// 	def.Value = attrs[9].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	return
	// }

	// switch err := attrs[8].Status; err {
	// case ua.StatusOK:
	// 	def.ValueRank = attrs[8].Value.Int()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[15].Status; err {
	// case ua.StatusOK:
	// 	def.ArrayDimensions = string(attrs[15].Value)
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	return
	// }

	// switch err := attrs[9].Status; err {
	// case ua.StatusOK:
	// 	def.UserAccessLevel = attrs[9].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[10].Status; err {
	// case ua.StatusOK:
	// 	def.MinimumSamplingInterval = attrs[10].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[11].Status; err {
	// case ua.StatusOK:
	// 	def.Historizing = attrs[11].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[11].Status; err {
	// case ua.StatusOK:
	// 	def.Executable = attrs[11].Value.Bool()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[11].Status; err {
	// case ua.StatusOK:
	// 	def.UserExecutable = attrs[11].Value.Bool()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[11].Status; err {
	// case ua.StatusOK:
	// 	def.DataTypeDefinition = attrs[11].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[12].Status; err {
	// case ua.StatusOK:
	// 	def.RolePermissions = attrs[12].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[13].Status; err {
	// case ua.StatusOK:
	// 	def.UserRolePermissions = attrs[13].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[13].Status; err {
	// case ua.StatusOK:
	// 	def.AccessRestrictions = attrs[13].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	// switch err := attrs[13].Status; err {
	// case ua.StatusOK:
	// 	def.AccessLevelEx = attrs[13].Value.String()
	// case ua.StatusBadAttributeIDInvalid:
	// 	// ignore
	// default:
	// 	log.Printf("[ERROR] %s", err)
	// 	return
	// }

	def.Path = join(path, def.BrowseName)
	log.Printf("[DEBUG] %d: def.Path:%s def.NodeClass:%s\n", level, def.Path, def.NodeClass)

	// Thread-safe append to nodeList using a mutex
	nodeMutex := &sync.Mutex{}
	nodeMutex.Lock()
	*nodeList = append(*nodeList, def)
	nodeMutex.Unlock()

	browseChildren := func(refType uint32) {
		defer wg.Done() // Ensure the WaitGroup counter is decremented when this function exits

		refs, err := n.ReferencedNodes(refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)

		if err != nil {
			log.Printf("[ERROR] References: %d: %s", refType, err)
			return
		}

		// Create a local WaitGroup to ensure all child goroutines complete before this function returns
		childWg := &sync.WaitGroup{}

		for _, rn := range refs {
			refNodeID := ua.MustParseNodeID(rn.ID.String())
			refNode := opcuaClient.Node(refNodeID)

			// Add to the WaitGroup before launching goroutine
			childWg.Add(1)
			wg.Add(1) // Add to parent WaitGroup

			// Launch goroutine with proper WaitGroup tracking
			go func(refNode *opcua.Node, parentNode *opcua.Node, path string, level int) {
				browse(wg, nodeList, refNode, parentNode, path, level, levelLimit, mqttResp)
				childWg.Done()
			}(refNode, n, def.Path, level+1)
		}

		// Wait for all child goroutines to complete
		childWg.Wait()
	}

	wg.Add(1)
	go browseChildren(id.HasComponent)
	wg.Add(1)
	go browseChildren(id.Organizes)
	wg.Add(1)
	go browseChildren(id.HasProperty)
}

func browsePath(wg *sync.WaitGroup, nodeToFilter Node, n *opcua.Node, parentNode *opcua.Node, level int, levelLimit int, mqttResp *opcuaBrowsePathResponseMQTTMessage, splitPath []string) {
	defer wg.Done() // Ensure wg.Done is called when this function returns

	if level > levelLimit {
		return
	}

	log.Printf("[DEBUG] node:%s level:%d\n", n, level)

	browseChildren := func(refType uint32) {
		defer wg.Done() // Ensure the WaitGroup counter is decremented when this function exits

		refs, err := n.ReferencedNodes(refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)

		if err != nil {
			connectionStatus := adapter_library.ConnectionStatus{
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Status:    ConnectionFailed,
			}

			mqttConnectionResp := opcuaConnectionResponseMQTTMessage{
				ConnectionStatus: connectionStatus,
			}
			mqttConnectionResp.ConnectionStatus.ErrorMessage = "Error when creating node attributes array: " + err.Error()
			mqttConnectionResp.ConnectionStatus.Timestamp = time.Now().UTC().Format(time.RFC3339)
			token, pubErr := returnConnectionMessage(&mqttConnectionResp, *adapterSettings.UseRelay)
			if pubErr != nil {
				log.Printf("[ERROR] Failed to publish connection message: %s\n", pubErr.Error())
			}

			sourceDown := false
			networkDown := false
			invalidSession := false

			if strings.Contains(err.Error(), "StatusBadServerNotConnected") {
				sourceDown = true
			} else if strings.Contains(err.Error(), "StatusBadTimeout") {
				networkDown = true
			} else if strings.Contains(err.Error(), "StatusBadSessionIDInvalid") {
				invalidSession = true
			}
			errorResp := errorDownMQTTMessage{
				SourceDown:     sourceDown,
				NetworkDown:    networkDown,
				InvalidSession: invalidSession,
				Message:        err.Error(),
			}
			returnErrorMessage(&errorResp, *adapterSettings.UseRelay)
			token.Wait()

			// Don't crash the entire application from a goroutine
			log.Printf("[ERROR] References: %d: %s", refType, err)
			return
		}

		// Create a local WaitGroup to ensure all child goroutines complete before this function returns
		childWg := &sync.WaitGroup{}
		nodeMutex := &sync.Mutex{}

		for _, rn := range refs {
			refNodeID := ua.MustParseNodeID(rn.ID.String())
			refNode := opcuaClient.Node(refNodeID)
			browseName, _ := refNode.BrowseName()

			if !sliceContains(splitPath, browseName.Name) {
				continue
			}

			if nodeToFilter.NodeName == browseName.Name {
				node := Node{
					NodeID:   refNodeID.String(),
					NodeName: browseName.Name,
					Path:     nodeToFilter.Path,
				}

				// Thread-safe append to results
				nodeMutex.Lock()
				mqttResp.Nodes = append(mqttResp.Nodes, node)
				nodeMutex.Unlock()

				return
			}

			// Add to the WaitGroup before launching goroutine
			childWg.Add(1)
			wg.Add(1) // Add to parent WaitGroup

			// Launch goroutine with proper WaitGroup tracking
			go func(refNode *opcua.Node, parentNode *opcua.Node, level int) {
				browsePath(wg, nodeToFilter, refNode, parentNode, level+1, levelLimit, mqttResp, splitPath)
				childWg.Done()
			}(refNode, n, level+1)
		}

		// Wait for all child goroutines to complete
		childWg.Wait()
	}

	wg.Add(1)
	go browseChildren(id.HasComponent)
	wg.Add(1)
	go browseChildren(id.Organizes)
	wg.Add(1)
	go browseChildren(id.HasProperty)
}

func returnNestedBrowseMessage(resp *NestedBrowseResponseMQTTMessage, useRelay bool) (mqtt.Token, error) {
	topic := adapterConfig.TopicRoot + "/" + browseTopic + "/nested_response"
	if useRelay {
		topic = topic + "/_platform"
	}
	log.Printf("[DEBUG] returnNestedBrowseMessage - publishing to topic %s\n", topic)
	json, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
		return nil, err
	}
	return adapter_library.PublishStatus(topic, json)
}
