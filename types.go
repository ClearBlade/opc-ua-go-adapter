package main

import (
	adapter_library "github.com/clearblade/adapter-go-library"
)

type opcuaAuthentication struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type opcuaAdapterSettings struct {
	EndpointURL           string              `json:"endpoint_url"`
	Authentication        opcuaAuthentication `json:"authentication"`
	SecurityMode          string              `json:"security_mode"`
	SecurityPolicy        string              `json:"security_policy"`
	UseRelay              *bool               `json:"use_relay,omitempty"`
	RegisterCustomObjects *bool               `json:"custom_objects,omitempty"`
	KeepAliveRetries      *int                `json:"keep_alive_retries,omitempty"`
}

type opcuaReadRequestMQTTMessage struct {
	NodeIDs []string `json:"node_ids"`
}

type opcuaReadResponseMQTTMessage struct {
	EdgeId          string                           `json:"edge_id"`
	ServerTimestamp string                           `json:"server_timestamp"`
	Data            map[string]opcuaReadResponseData `json:"data"`
	Success         bool                             `json:"success"`
	StatusCode      uint32                           `json:"status_code"`
	ErrorMessage    string                           `json:"error_message"`
}

type opcuaReadResponseData struct {
	Value           interface{} `json:"value"`
	SourceTimestamp string      `json:"source_timestamp"`
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
	ObjectID       string                      `json:"object_id"`
	MethodID       string                      `json:"method_id"`
	InputArguments []opcuaMethodInputArguments `json:"arguments"`
}

type opcuaMethodInputArguments struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type opcuaMethodInputArgumentNodeType struct {
	Namespace      uint16      `json:"namespace"`
	IdentifierType string      `json:"identifier_type"`
	Identifier     interface{} `json:"identifier"`
}

type opcuaMethodResponseMQTTMessage struct {
	ObjectID       string                      `json:"object_id"`
	MethodID       string                      `json:"method_id"`
	Timestamp      string                      `json:"timestamp"`
	Success        bool                        `json:"success"`
	StatusCode     uint32                      `json:"status_code"`
	ErrorMessage   string                      `json:"error_message"`
	InputArguments []opcuaMethodInputArguments `json:"arguments"`
	OutputValues   []interface{}               `json:"values"`
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
type opcuaSubscriptionCreateParmsMQTTMessage struct {
	PublishInterval            *uint32                                `json:"publish_interval,omitempty"`
	LifetimeCount              *uint32                                `json:"lifetime,omitempty"`
	MaxKeepAliveCount          *uint32                                `json:"keepalive,omitempty"`
	MaxNotificationsPerPublish *uint32                                `json:"max_publish_notifications,omitempty"`
	Priority                   *uint8                                 `json:"priority,omitempty"`
	MonitoredItems             *[]opcuaMonitoredItemCreateMQTTMessage `json:"items_to_monitor,omitempty"`
}

//TODO - Republish has not been implemented by "github.com/gopcua/opcua/ua" yet
// type opcuaSubscriptionRepublishParmsMQTTMessage struct {
// 	SubscriptionID uint32 `json:"subscription_id"`
// }

type opcuaSubscriptionDeleteParmsMQTTMessage struct {
	SubscriptionID uint32 `json:"subscription_id"`
}

// TODO - MonitoringParameters - Build out structure, figure out how to implement Filter
// TODO - Uncomment AttributeID when we are ready to handle more than ua.AttributeIDValue
type opcuaMonitoredItemCreateMQTTMessage struct {
	NodeID string `json:"node_id"`
	Values bool   `json:"values"`
	Events bool   `json:"events"`
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

// TODO - Uncomment AttributeID when we are ready to handle more than ua.AttributeIDValue
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

// TODO - Uncomment AttributeID when we are ready to handle more than ua.AttributeIDValue
type opcuaMonitoredItemNotificationMQTTMessage struct {
	NodeID string `json:"node_id"`
	//AttributeID             uint32  `json:"attribute_id"`
	ClientHandle uint32            `json:"client_handle"`
	StatusCode   uint32            `json:"status_code"`
	Value        interface{}       `json:"value,omitempty"`
	Event        opcuaEventMessage `json:"event,omitempty"`
}

// eventFieldNames        = []string{"EventId", "EventType", "Severity", "Time", "Message"}
type opcuaEventMessage struct {
	EventID     string      `json:"event_id"`
	EventType   string      `json:"event_type"`
	SourceNode  string      `json:"source_node"`
	SourceName  string      `json:"source_name"`
	Severity    uint32      `json:"severity"`
	Time        string      `json:"time"`
	ReceiveTime string      `json:"receive_time"`
	LocalTime   interface{} `json:"local_time"`
	Message     string      `json:"message"`
	StatusCode  uint32      `json:"status_code,omitempty"`
}

type opcuaSubscriptionRequestMQTTMessage struct {
	RequestType   SubscriptionOperationType `json:"request_type"`
	RequestParams *interface{}              `json:"request_params,omitempty"`
}

type opcuaSubscriptionResponseMQTTMessage struct {
	RequestType    SubscriptionOperationType `json:"request_type"`
	SubscriptionID uint32                    `json:"subscription_id"`
	Timestamp      string                    `json:"timestamp"`
	Success        bool                      `json:"success"`
	// StatusCode     uint32                    `json:"status_code"`
	ErrorMessage string        `json:"error_message"`
	Results      []interface{} `json:"results"`
}

type Node struct {
	NodeName string `json:"node_name"`
	Path     string `json:"path"`
	NodeID   string `json:"node_id,omitempty"`
}

type opcuaBrowseRequestMQTTMessage struct {
	RootNode   string    `json:"root_node"`
	NodeList   []Node    `json:"node_list,omitempty"`
	Attributes *[]string `json:"attributes,omitempty"`
	LevelLimit int       `json:"level_limit,omitempty"`
	NestedView bool      `json:"nested_view,omitempty"`
}

type opcuaBrowseResponseMQTTMessage struct {
	NodeIDs          []string                         `json:"node_ids"`
	ConnectionStatus adapter_library.ConnectionStatus `json:"connection_status"`
}

type opcuaBrowseResponseWithAttrsMQTTMessage struct {
	Nodes            []node                           `json:"nodes"`
	ConnectionStatus adapter_library.ConnectionStatus `json:"connection_status"`
	NodeList         []Node                           `json:"node_list,omitempty"`
}

type opcuaBrowsePathResponseMQTTMessage struct {
	Nodes            []Node                           `json:"nodes"`
	ConnectionStatus adapter_library.ConnectionStatus `json:"connection_status"`
}

type node struct {
	NodeId                  string `json:"node_id"`
	ParentNodeID            string `json:"parent_node_id"`
	Level                   int    `json:"level"`
	NodeClass               string `json:"node_class,omitempty"`
	BrowseName              string `json:"browse_name,omitempty"`
	Description             string `json:"description,omitempty"`
	AccessLevel             string `json:"access_level,omitempty"`
	DataType                string `json:"data_type,omitempty"`
	DisplayName             string `json:"display_name,omitempty"`
	WriteMask               string `json:"write_mask,omitempty"`
	UserWriteMask           string `json:"user_write_mask,omitempty"`
	IsAbstract              bool   `json:"is_abstract,omitempty"`
	Symmetric               string `json:"symmetric,omitempty"`
	InverseName             string `json:"inverse_name,omitempty"`
	ContainsNoLoops         bool   `json:"contains_no_loops,omitempty"`
	EventNotifier           string `json:"event_notifier,omitempty"`
	Value                   string `json:"value,omitempty"`
	ValueRank               int64  `json:"value_rank,omitempty"`
	ArrayDimensions         string `json:"array_dimensions,omitempty"`
	UserAccessLevel         string `json:"user_acces_level,omitempty"`
	MinimumSamplingInterval string `json:"minimum_sampling_interval,omitempty"`
	Historizing             string `json:"historizing,omitempty"`
	Executable              bool   `json:"executable,omitempty"`
	UserExecutable          bool   `json:"user_executable,omitempty"`
	DataTypeDefinition      string `json:"data_type_definition,omitempty"`
	RolePermissions         string `json:"role_permissions,omitempty"`
	UserRolePermissions     string `json:"user_role_permissions,omitempty"`
	AccessRestrictions      string `json:"access_restriction,omitempty"`
	AccessLevelEx           string `json:"access_level_ex,omitempty"`
	Writable                bool   `json:"writable,omitempty"`
	Unit                    string `json:"Unit,omitempty"`
	Scale                   string `json:"Scale,omitempty"`
	Min                     string `json:"min,omitempty"`
	Max                     string `json:"max,omitempty"`
}

type opcuaConnectionResponseMQTTMessage struct {
	ConnectionStatus adapter_library.ConnectionStatus `json:"connection_status"`
}

type opcuaBrowseTagNameRequestMQTTMessage struct {
	RootNode       string `json:"root_node"`
	TagName        string `json:"tag_name"`
	NamespaceIndex uint16 `json:"ns_index"`
}
type opcuaBrowseTagNameResponseMQTTMessage struct {
	NodeIDs          string                           `json:"node_ids"`
	ConnectionStatus adapter_library.ConnectionStatus `json:"connection_status"`
}

type errorDownMQTTMessage struct {
	SourceDown     bool   `json:"source_down"`
	NetworkDown    bool   `json:"network_down"`
	InvalidSession bool   `json:"invalid_sessionkey"`
	Message        string `json:"message"`
}

// NestedNode represents a node in a hierarchical tree structure
type NestedNode struct {
	NodeId     string       `json:"node_id"`
	BrowseName string       `json:"browse_name"`
	Children   []NestedNode `json:"children,omitempty"`
}

// NestedBrowseResponseMQTTMessage represents a hierarchical browse response
type NestedBrowseResponseMQTTMessage struct {
	Nodes            []NestedNode                     `json:"nodes"`
	ConnectionStatus adapter_library.ConnectionStatus `json:"connection_status"`
}
