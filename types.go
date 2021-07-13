package main

import (
	"github.com/gopcua/opcua/ua"
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

// type opcuaSubscription struct {
// 	SubscriptionID            uint32                        `json:"endpoint_url"`
// 	RevisedPublishingInterval time.Duration                 `json:"endpoint_url"`
// 	RevisedLifetimeCount      uint32                        `json:"endpoint_url"`
// 	RevisedMaxKeepAliveCount  uint32                        `json:"endpoint_url"`
// 	Notifs                    chan *PublishNotificationData `json:"endpoint_url"`
// 	params                    *SubscriptionParameters       `json:"endpoint_url"`
// 	items                     []*monitoredItem              `json:"endpoint_url"`
// 	lastSeq                   uint32                        `json:"endpoint_url"`
// 	nextSeq                   uint32                        `json:"endpoint_url"`
// 	c                         *Client                       `json:"endpoint_url"`
// }

type opcuaReadResponseMQTTMessage struct {
	Timestamp string                 `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

type opcuaWriteRequestMQTTMessage struct {
	NodeID string      `json:"node_id"`
	Value  interface{} `json:"value"`
}

type opcuaWriteResponseMQTTMessage struct {
	NodeID       string          `json:"node_id"`
	Timestamp    string          `json:"timestamp"`
	Success      bool            `json:"success"`
	StatusCode   uint32          `json:"status_code"`
	ErrorMessage string          `json:"error_message"`
	Results      []ua.StatusCode `json:"results"`
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

type SubscriptionRequestType string

// TODO - Add missing subscription request types when they are implemented by github.com/gopcua
const (
	SubscriptionCreate  SubscriptionRequestType = "create"
	SubscriptionPublish SubscriptionRequestType = "publish"
	SubscriptionDelete  SubscriptionRequestType = "delete"
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
	PublishInterval            *uint32                          `json:"publish_interval,omitempty"`
	LifetimeCount              *uint32                          `json:"lifetime,omitempty"`
	MaxKeepAliveCount          *uint32                          `json:"keepalive,omitempty"`
	MaxNotificationsPerPublish *uint32                          `json:"max_publish_notifications,omitempty"`
	Priority                   *uint8                           `json:"priority,omitempty"`
	MonitoredItems             *[]opcuaMonitoredItemMQTTMessage `json:"items_to_monitor,omitempty"`
}

//TODO
type opcuaSubscriptionPublishParmsMQTTMessage struct {
	PublishInterval            uint32 `json:"publish_interval,omitempty"`
	LifetimeCount              uint32 `json:"lifetime,omitempty"`
	MaxKeepAliveCount          uint32 `json:"keepalive,omitempty"`
	MaxNotificationsPerPublish uint32 `json:"max_publish_notifications,omitempty"`
	Priority                   uint8  `json:"priority,omitempty"`
}

//TODO
type opcuaSubscriptionDeleteParmsMQTTMessage struct {
	SubscriptionID uint32 `json:"subscription_id"`
}

type opcuaMonitoredItemMQTTMessage struct {
	ItemToMonitor        *ua.ReadValueID               `json:"item,omitempty"`
	MonitoringParameters *ua.MonitoringParameters      `json:"monitor_params,omitempty"`
	MonitoringMode       ua.MonitoringMode             `json:"monitor_mode,omitempty"`
	TimestampsToReturn   ua.TimestampsToReturn         `json:"timestamps_to_return,omitempty"`
	createResult         *ua.MonitoredItemCreateResult `json:"create_result,omitempty"`
}

type opcuaSubscriptionRequestMQTTMessage struct {
	NodeID        string                  `json:"node_id"`
	RequestType   SubscriptionRequestType `json:"request_type"`
	RequestParams *interface{}            `json:"request_params,omitempty"`
}

type opcuaSubscriptionResponseMQTTMessage struct {
	NodeID       string                  `json:"node_id"`
	RequestType  SubscriptionRequestType `json:"request_type"`
	Timestamp    string                  `json:"timestamp"`
	Success      bool                    `json:"success"`
	StatusCode   uint32                  `json:"status_code"`
	ErrorMessage string                  `json:"error_message"`
}
