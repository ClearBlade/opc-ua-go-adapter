package main

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"log"
	"strings"
	"time"

	adapter_library "github.com/clearblade/adapter-go-library"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/debug"
	"github.com/gopcua/opcua/ua"
)

const (
	adapterName = "opc-ua-adapter"
	appuri      = "urn:cb-opc-ua-adapter:client"
)

var (
	adapterSettings *opcuaAdapterSettings
	adapterConfig   *adapter_library.AdapterConfig
	opcuaClient     *opcua.Client
)

type opcuaAuthentication struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type opcuaAdapterSettings struct {
	EndpointURL    string              `json:"endpoint_url"`
	NodeIDs        []string            `json:"node_ids"`
	PollInterval   float64             `json:"poll_interval"`
	Authentication opcuaAuthentication `json:"authentication"`
	SecurityMode   string              `json:"security_mode"`
	SecurityPolicy string              `json:"security_policy"`
}

type opcuaMQTTMessage struct {
	Timestamp string                 `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

type opcuaWriteMQTTMessage struct {
	NodeID string      `json:"node_id"`
	Value  interface{} `json:"value"`
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

	err = adapter_library.ConnectMQTT(adapterConfig.TopicRoot+"/write", cbMessageHandler)
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
				continue
			}
			mqttMessage := opcuaMQTTMessage{
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
			}
			b, err := json.Marshal(mqttMessage)
			if err != nil {
				log.Printf("[ERROR] Failed to stringify JSON: %s\n", err.Error())
				continue
			}
			err = adapter_library.Publish(adapterConfig.TopicRoot+"/read", b)
			if err != nil {
				log.Printf("[ERROR] Failed to publish MQTT message: %s\n", err.Error())
			}
		}
	}

}

func cbMessageHandler(message *mqttTypes.Publish) {
	log.Println("[INFO] cbMessageHandler - Received OPC UA write request")
	writeReq := opcuaWriteMQTTMessage{}
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
	if err != nil {
		log.Printf("[ERROR] Failed to write OPC UA tag %s: %s\n", writeReq.NodeID, err.Error())
		return
	}
	log.Printf("[INFO] OPC UA write successful: %+v\n", resp.Results[0])
}

func initializeOPCUA() *opcua.Client {
	log.Println("[INFO] initializeOPCUAPolling - Creating OPC UA Session")

	if adapter_library.Args.LogLevel == "debug" {
		debug.Enable = true
	}

	opcuaOpts := []opcua.Option{}

	// get a list of endpoints for target server
	endpoints, err := opcua.GetEndpoints(adapterSettings.EndpointURL)
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
		log.Fatalln("[FATAL] Anonymous auth type not implemented yet")
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

	ctx := context.Background()

	log.Printf("[INFO] Connecting to OPC server address %s\n", adapterSettings.EndpointURL)
	c := opcua.NewClient(adapterSettings.EndpointURL, opcuaOpts...)
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("[FATAL] Failed to connect to OPC UA Server: %s\n", err.Error())
	}
	return c

}
