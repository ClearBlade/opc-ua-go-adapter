# OPC UA Go adapter

The OPC UA adapter functions as an OPC UA client and allows an IoT gateway (or any other client) to interact with an OPC UA server.

Communication with the OPC UA adapter is enabled through MQTT and the configuration collections detailed below.

Currently, the adapter supports OPC UA read and write, with more options to follow.

## ClearBlade Platform dependencies

The OPC UA adapter was constructed to communicate with a _system_ defined in a ClearBlade Platform instance. Therefore, the adapter requires a _system_ to have been created within a ClearBlade Platform instance.

Once a system has been created, artifacts must be defined within the ClearBlade Platform system to allow the adapter to function properly. At a minimum:

- An adapter configuration data collection named `adapter_config` needs to be created in the ClearBlade Platform _system_ and populated with data appropriate to the Modbus client adapter. The data collection schema should be as follows:

| Column name      | Column datatype |
| ---------------- | --------------- |
| adapter_name     | string          |
| adapter_settings | string          |
| topic_root       | string          |

## Adapter settings structure

The `adapter_settings` JSON string provided in the `adapter_config` collection is expected to have the following structure. This JSON is how you provide the adapter with the specific OPC UA server connection details.

```json
{
  "endpoint_url": "opc.tcp://clearblades-mbp.lan:53530/OPCUA/SimulationServer",
  "authentication": {
    "type": "username",
    "username": "testing",
    "password": "testing"
  },
  "security_mode": "signandencrypt",
  "security_policy": "Basic256"
}
```

### Supported authentication types

| Authentication type | Details                                                                                                                |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `Anonymous`         | Use anonymous authentication; no other fields are required in the `authentication` object                              |
| `Username`          | Use username and password authentication, must include `username` and `password` key/values in the `authentication` object |
| `Certificate`       | Not yet implemented                                                                                                    |

### Supported security mode

| Security mode    |
| ---------------- |
| `None`           |
| `Sign`           |
| `SignAndEncrypt` |

### Supported security policies

| Security mode    |
| ---------------- |
| `none`           |
| `Basic256`       |
| `Basic256SHA256` |

### Supported OPC UA operations

| Operations  |
| ----------- |
| `read`      |
| `write`     |
| `method`    |
| `subscribe` |

## MQTT topic structure

The OPC UA adapter will subscribe to specific topics to handle OPC UA operations. Additionally, the adapter will publish messages to MQTT topics for the OPC UA operation results. The topic structures utilized are as follows:

- OPC UA read request: {**TOPIC ROOT**}/read
- OPC UA read results: {**TOPIC ROOT**}/read/response
- OPC UA write request: {**TOPIC ROOT**}/write
- OPC UA write response: {**TOPIC ROOT**}/write/response
- OPC UA method request: {**TOPIC ROOT**}/method
- OPC UA method response: {**TOPIC ROOT**}/method/response
- OPC UA subscribe request: {**TOPIC ROOT**}/subscribe
  \*\* create, publish, and delete are the only supported services in the OPC UA library being utilized
- OPC UA subscribe response: {**TOPIC ROOT**}/subscribe/response
- OPC UA publish: {**TOPIC ROOT**}/publish/response
- OPC UA browse request: {**TOPIC_ROOT**}/browse/\_edge/{EDGE_ID}
- OPC UA browse results: {**TOPIC_ROOT**}/browse/response
- OPC UA connect request: {**TOPIC_ROOT**}/connect/\_edge/{EDGE_ID}
- OPC UA connect results: {**TOPIC_ROOT**}/connect/response

## MQTT message structure

### OPC UA read request payload format

```json
{
  "node_ids": [
    "ns=3;i=1001",
    "ns=3;i=1002",
    "ns=3;i=1003",
    "ns=3;i=1004",
    "ns=3;i=1005",
    "ns=3;i=1006"
  ]
}
```

### OPC UA read results payload format

```json
{
  "server_timestamp": "2021-07-30T05:04:55Z",
  "data": {
    "ns=3;i=1001": {
      "value": 6,
      "source_timestamp": "2021-07-30T05:04:55Z"
    },
    "ns=3;i=1002": {
      "value": -1.698198,
      "source_timestamp": "2021-07-30T05:04:55Z"
    },
    "ns=3;i=1003": {
      "value": -2,
      "source_timestamp": "2021-07-30T05:04:55Z"
    },
    "ns=3;i=1004": {
      "value": -1.732051,
      "source_timestamp": "2021-07-30T05:04:55Z"
    },
    "ns=3;i=1005": {
      "value": -2,
      "source_timestamp": "2021-07-30T05:04:55Z"
    },
    "ns=3;i=1006": {
      "value": -1.333333,
      "source_timestamp": "2021-07-30T05:04:55Z"
    }
  },
  "success": true,
  "status_code": 0,
  "error_message": ""
}
```

### OPC UA write request payload format

```json
{
  "node_id": "ns=3;i=1001",
  "value": 25 // can be string or int/float
}
```

### OPC UA write response payload format

```json
{
    "node_id": "ns=3;i=1001",
    "timestamp": "", //ISO formatted timestamp
    "success": true|false,
    "status_code": 0, //Integer
    "error_message": ""
}
```

### OPC UA method request payload format

```json
{
  "object_id": "ns=3;i=1001",
  "method_id": "",
  "arguments": [
    {
      "type": "uint32", // boolean, int16, uint16, int32, uint32, int64, uint64, float, double, string, node
      "value": 5
    },
    {
      "type": "node", // note that node type has a custom value structure to allow you to specify different node types
      "value": {
        "namespace": 0,
        "identifier_type": "numeric", //also can be string
        "identifier": 85
      }
    }
  ] //Array of arguments (with types)
}
```

### OPC UA method response payload format

```json
{
    "object_id": "ns=3;i=1001",
    "method_id": 25,
    "timestamp": "", //ISO formatted timestamp
    "success": true|false,
    "status_code": 0, //Integer
    "error_message": "",
    "arguments": [],
    "values": []
}
```

### OPC UA subscription request payload format

```json
{
  "request_type": "create|republish|delete",
  "request_params": {
    //One of the following: (see below for schema)
    //
    //OPC UA subscription create request
    //OPC UA subscription delete request
    //OPC UA subscription republish request
  }
}
```

### OPC UA subscription create request payload format

```json
{
    "publish_interval": 1000,
    "lifetime": 25,
    "keepalive": "", //ISO formatted timestamp
    "max_publish_notifications": true|false,
    "priority": 0, //Integer
    "items_to_monitor": [
      {
        "node_id": "ns=2;i=1001",
        "events": true|false,
        "values": true|false
      },
      {
        "node_id": "ns=3;i=1001",
        "events": true|false,
        "values": true|false
      },
      ...
    ],
}
```

- The events flag in each _item_to_monitor_ indicates whether or not events should be subscribed to
- The value flag in each _item_to_monitor_ indicates whether or not value changes should be subscribed to

### OPC UA subscription delete request payload format

```json
{
    "subscription_id": {the_subscription_id}, //INTEGER
}
```

### OPC UA subscribe response payload format

```json
{
    "request_type": "create|publish|delete",
    "timestamp": "", //ISO formatted timestamp
    "success": true|false,
    "status_code": 0, //Integer
    "error_message": "",
    "results": [ //Will only contain data when request_type = "publish"
      {
        "node_id": "ns=2;i=1001",
        "client_handle": 1,
        "value": 5,
        "event" : {
          "event_id": "",
          "event_type": "SystemEventType",
          "severity": 2, //1-1000
          "time": "", //ISO formatted timestamp
          "message": "A system event has occurred",
        }
      },
      {
        "node_id": "ns=3;i=1001",
        "client_handle": 2,
        "value": "hello",
        "event" : {
          "event_id": "",
          "event_type": "SystemEventType",
          "severity": 2, //1-1000
          "time": "", //ISO formatted timestamp
          "message": "A system event has occurred",
        }
      },
      ...
    ],
}
```

### OPC UA browse request payload format

```json
{
  "root_node": "ns=2;s=Dynamic" //the root node, STRING
}
```

### OPC UA browse response payload format

```json
{
  "node_ids": [
    "ns=2;s=Dynamic/RandomInt32",
    "ns=2;s=Dynamic/RandomInt64",
    "ns=2;s=Dynamic/RandomFloat",
    "ns=2;s=Dynamic/RandomDouble"
  ],
  "connection_status": {
    "timestamp": "2021-11-16T17:18:02Z",
    "status": "BrowseSuccess", //BrowseSuccess, BrowseFailed, or BrowsePending
    "error_message": "" //will not be returned if empty
  }
}
```

### OPC UA browse request payload format with attributes

```json
{
  "root_node": "ns=2;s=Dynamic", //the root node, STRING (optional)
  "level_limit": 10, //Limit the depth of the search (optional)
  "node_list": [
    {
      "node_name": "RandomDouble",
      "path": "ns=2;s=Dynamic.ns=2;s=Dynamic/RandomDouble"
    },
    {
      "node_name": "RandomInt64",
      "path": "ns=2;s=Dynamic.ns=2;s=Dynamic/RandomInt64"
    }
  ],
  "attributes": [
    "NodeClass",
    "BrowseName",
    "Description",
    "AccessLevel"
    "DataType",
    "DisplayName",
  ] //node attributes array to return (all supported shown)
}
```

### OPC UA browse response payload format with attributes

```json
{
  "nodes": [
    {
      "node_id": "ns=2;s=Dynamic/RandomInt64",
      "parent_node_id": "ns=2;s=Dynamic",
      "node_class": "NodeClassVariable",
      "browse_name": "RandomInt64",
      "data_type": "i=8"
    },
    {
      "node_id": "ns=2;s=Dynamic/RandomDouble",
      "parent_node_id": "ns=2;s=Dynamic",
      "node_class": "NodeClassVariable",
      "browse_name": "RandomDouble",
      "data_type": "float64"
    }
  ],
  "node_list": [
    {
      "node_name": "RandomDouble",
      "path": "ns=2;s=Dynamic.ns=2;s=Dynamic/RandomDouble"
    },
    {
      "node_name": "RandomInt64",
      "path": "ns=2;s=Dynamic.ns=2;s=Dynamic/RandomInt64"
    }
  ],
  "connection_status": {
    "timestamp": "2021-11-16T17:18:02Z",
    "status": "BrowseSuccess", //BrowseSuccess, BrowseFailed, or BrowsePending
    "error_message": "" //will not be returned if empty
  }
}
```

### OPC UA browse request payload format with path

```json
{
  "root_node": "i=85", //the root node, STRING (optional)
  "node_list": [
    {
      "node_name": "Real-Time",
      "path": "AggregationServer.AggregatedServers.bhi-acquisition.Objects.Real-Time"
    },
    {
      "node_name": "PredictedTrajectoryStations",
      "path": "AggregationServer.AggregatedServers.bhi-atdprocessing.Objects.ServiceApplication.PredictedTrajectoryStations"
    },
    {
      "node_name": "InputData",
      "path": "AggregationServer.AggregatedServers.bhi-atdprocessing.objects.automatedtrajectorydrilling.service monitoring.InputData"
    }
  ]
}
```

### OPC UA browse response payload format with path

```json
{
  "nodes": [
    {
      "node_id": "ns=51;i=5006",
      "node_name": "Real-Time",
      "path": "AggregationServer.AggregatedServers.bhi-acquisition.Objects.Real-Time"
    },
    {
      "node_id": "ns=27;i=5015",
      "node_name": "PredictedTrajectoryStations",
      "path": "AggregationServer.AggregatedServers.bhi-atdprocessing.Objects.ServiceApplication.PredictedTrajectoryStations"
    },
    {
      "node_id": "ns=27;i=5006",
      "node_name": "InputData",
      "path": "AggregationServer.AggregatedServers.bhi-atdprocessing.objects.automatedtrajectorydrilling.service monitoring.InputData"
    }
  ],
  "connection_status": {
    "status": "BrowseSuccess",
    "timestamp": "2022-10-07T06:21:44Z"
  }
}
```

### OPC UA connect request payload format

```json
{
  //nothing! (empty message will suffice)
}
```

### OPC UA connect response payload format

```json
{
  "connection_status": {
    "timestamp": "2021-11-16T17:18:02Z",
    "status": "ConnectionSuccess", //ConnectionSuccess, ConnectionFailed, or ConnectionPending
    "error_message": "" //will not be returned if empty
  }
}
```

### OPC UA error response payload format

```json
{
  "source_down": false,
  "network_down": false,
  "invalid_sessionkey": false,
  "message": ""
}
```

## Starting the adapter

This adapter is built using the [adapter-go-library](https://github.com/ClearBlade/adapter-go-library), allowing multiple options for starting the adapter, including CLI flags and environment variables. Using a device service account for authentication with this adapter is recommended. See the below chart for available start options and their defaults.

All ClearBlade adapters require a certain set of system specific variables to start and connect with the ClearBlade Platform/Edge. This library allows these to be passed in by command line arguments or environment variables. Command line arguments take precedence over environment variables.

| Name                                         | CLI flag                  | Environment variable       | Default                                                              |
| -------------------------------------------- | ------------------------- | -------------------------- | -------------------------------------------------------------------- |
| System Key                                   | `systemKey`               | `CB_SYSTEM_KEY`            | N/A                                                                  |
| System Secret                                | `systemSecret`            | `CB_SYSTEM_SECRET`         | N/A                                                                  |
| Platform/Edge URL                            | `platformURL`             | N/A                        | `http://localhost:9000`                                              |
| Platform/Edge Messaging URL                  | `messagingURL`            | N/A                        | `localhost:1883`                                                     |
| Device Name (**depreciated**)                | `deviceName`              | N/A                        | `adapterName` provided when calling `adapter_library.ParseArguments` |
| Device Password/Active Key (**depreciated)** | `password`                | N/A                        | N/A                                                                  |
| Device Service Account                       | N/A                       | `CB_SERVICE_ACCOUNT`       | N/A                                                                  |
| Device Service Account Token                 | N/A                       | `CB_SERVICE_ACCOUNT_TOKEN` | N/A                                                                  |
| Log Level                                    | `logLevel`                | N/A                        | `info`                                                               |
| Adapter Config Collection Name               | `adapterConfigCollection` | N/A                        | `adapter_config`                                                     |

`opc-ua-go-adapter -systemKey=<SYSTEM_KEY> -systemSecret=<SYSTEM_SECRET> -platformURL=<PLATFORM_URL> -messagingURL=<MESSAGING_URL> -deviceName=<DEVICE_NAME> -password=<DEVICE_ACTIVE_KEY> -adapterConfigCollection=<COLLECTION_NAME> -logLevel=<LOG_LEVEL>`

A system key and system secret will always be required to start the adapter, and it's recommended to always use a device service account and token for adapters.

Device name and password for adapters are **depreciated** and only provided for backward compatibility and should not be used for any new adapters.

## Setup

---

The OPC UA Go adapter depends on the ClearBlade Go SDK and its dependent libraries being installed. The OPC UA Go adapter was written in Go and therefore requires Go to be installed (https://golang.org/doc/install).

### Adapter compilation

To compile the adapter for execution, the following steps need to be performed:

1.  Retrieve the adapter source code
    - `git clone git@github.com:ClearBlade/opc-ua-go-adapter.git`
2.  Navigate to the _opc-ua-go-adapter_ directory
    - `cd opc-ua-go-adapter`
3.  Compile the adapter for your needed architecture and OS
    - `GOARCH=arm GOARM=5 GOOS=linux go build`
