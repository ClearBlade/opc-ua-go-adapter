package main

type opcuaAuthentication struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type opcuaAdapterSettings struct {
	EndpointURL    string              `json:"endpoint_url"`
	Authentication opcuaAuthentication `json:"authentication"`
	SecurityMode   string              `json:"security_mode"`
	SecurityPolicy string              `json:"security_policy"`
}

type opcuaReadRequestMQTTMessage struct {
	NodeIDs []string `json:"node_ids"`
}

type opcuaReadResponseMQTTMessage struct {
	Timestamp    string                 `json:"timestamp"`
	Data         map[string]interface{} `json:"data"`
	Success      bool                   `json:"success"`
	StatusCode   uint32                 `json:"status_code"`
	ErrorMessage string                 `json:"error_message"`
}

type opcuaWriteRequestMQTTMessage struct {
	NodeID string      `json:"node_id"`
	Value  interface{} `json:"value"`
}

type opcuaWriteResponseMQTTMessage struct {
	NodeID       string `json:"node_id"`
	Timestamp    string `json:"timestamp"`
	Success      bool   `json:"success"`
	StatusCode   uint32 `json:"status_code"`
	ErrorMessage string `json:"error_message"`
}

type opcuaMethodRequestMQTTMessage struct {
	ObjectID       string        `json:"object_id"`
	MethodID       string        `json:"method_id"`
	InputArguments []interface{} `json:"arguments"`
}

type opcuaMethodResponseMQTTMessage struct {
	ObjectID       string        `json:"object_id"`
	MethodID       string        `json:"method_id"`
	Timestamp      string        `json:"timestamp"`
	Success        bool          `json:"success"`
	StatusCode     uint32        `json:"status_code"`
	ErrorMessage   string        `json:"error_message"`
	InputArguments []interface{} `json:"arguments"`
	OutputValues   []interface{} `json:"values"`
}

type SubscriptionOperationType string

// TODO - Add missing subscription operation types when they are implemented by github.com/gopcua
// * ModifySubscription
// * SetPublishingMode
// * Republish
// * TransferSubscriptions
const (
	SubscriptionCreate    SubscriptionOperationType = "create"
	SubscriptionRepublish SubscriptionOperationType = "republish"
	SubscriptionPublish   SubscriptionOperationType = "publish"
	SubscriptionDelete    SubscriptionOperationType = "delete"
)

// https://reference.opcfoundation.org/v104/Core/docs/Part4/5.13.2/
//
// publish_interval - The minimum amount of time (milliseconds) between updates
// lifetime - How long the connection to the OPC UA server is preserved in the absence of updates before it is killed and recreated.
// keepalive - The maximum number of times the publish timer expires without sending any notifications before sending a keepalive message.
// max_publish_notifications - The maximum number of notifications that the Client wishes to receive in a single Publish response.
// priority - Indicates the relative priority of the Subscription
//
type opcuaSubscriptionCreateParmsMQTTMessage struct {
	PublishInterval            *uint32                                `json:"publish_interval,omitempty"`
	LifetimeCount              *uint32                                `json:"lifetime,omitempty"`
	MaxKeepAliveCount          *uint32                                `json:"keepalive,omitempty"`
	MaxNotificationsPerPublish *uint32                                `json:"max_publish_notifications,omitempty"`
	Priority                   *uint8                                 `json:"priority,omitempty"`
	MonitoredItems             *[]opcuaMonitoredItemCreateMQTTMessage `json:"items_to_monitor,omitempty"`
}

//TODO - Republish has not been implemented by "github.com/gopcua/opcua/ua" yet
type opcuaSubscriptionRepublishParmsMQTTMessage struct {
	SubscriptionID uint32 `json:"subscription_id"`
}

type opcuaSubscriptionDeleteParmsMQTTMessage struct {
	SubscriptionID uint32 `json:"subscription_id"`
}

//TODO - MonitoringParameters - Build out structure, figure out how to implement Filter
//TODO - Uncomment AttributeID when we are ready to handle more than ua.AttributeIDValue
type opcuaMonitoredItemCreateMQTTMessage struct {
	NodeID string `json:"node_id"`
	//AttributeID uint32 `json:"attribute_id"` - For now, we will only use attribute id 13 (AttributeIDValue)
	//
	// TODO - Implement later
	//
	//Monitoring params - We will be using defaults for now
	// SamplingInterval float64
	// Filter           *ExtensionObject
	// QueueSize        uint32
	// DiscardOldest    bool
}

//TODO - Uncomment AttributeID when we are ready to handle more than ua.AttributeIDValue
type opcuaMonitoredItemCreateResultMQTTMessage struct {
	NodeID string `json:"node_id"`
	//AttributeID             uint32  `json:"attribute_id"`
	ClientHandle            uint32  `json:"client_handle"`
	DiscardOldest           bool    `json:"disgard_oldest"`
	StatusCode              uint32  `json:"status_code"`
	RevisedSamplingInterval float64 `json:"revised_sampling_interval"`
	RevisedQueueSize        uint32  `json:"revised_queue_size"`
	FilterResult            interface{}
	MonitoringMode          uint32 `json:"monitoring_mode"`
	TimestampsToReturn      uint32 `json:"timestamps_to_return,omitempty"`
}

//TODO - Uncomment AttributeID when we are ready to handle more than ua.AttributeIDValue
type opcuaMonitoredItemNotificationMQTTMessage struct {
	NodeID string `json:"node_id"`
	//AttributeID             uint32  `json:"attribute_id"`
	ClientHandle uint32      `json:"client_handle"`
	Value        interface{} `json:"value"`
}

type opcuaSubscriptionRequestMQTTMessage struct {
	RequestType   SubscriptionOperationType `json:"request_type"`
	RequestParams *interface{}              `json:"request_params,omitempty"`
}

type opcuaSubscriptionResponseMQTTMessage struct {
	RequestType  SubscriptionOperationType `json:"request_type"`
	Timestamp    string                    `json:"timestamp"`
	Success      bool                      `json:"success"`
	StatusCode   uint32                    `json:"status_code"`
	ErrorMessage string                    `json:"error_message"`
	Results      []interface{}             `json:"results"`
}
