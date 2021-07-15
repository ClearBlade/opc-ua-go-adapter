# OPC UA Go Adapter
The OPC UA adapter function as a OPC UA Client and allows an IoT Gateway (or any other client) to interact with an OPC UA server.

Communication with the OPC UA Adapter is enabled through MQTT and configuration Collections which are detailed below.

Currently OPC UA Read and Write are supported by the adapter, with more options to follow.

## ClearBlade Platform Dependencies
The OPC UA adapter adapter was constructed to provide the ability to communicate with a _System_ defined in a ClearBlade Platform instance. Therefore, the adapter requires a _System_ to have been created within a ClearBlade Platform instance.

Once a System has been created, artifacts must be defined within the ClearBlade Platform system to allow the adapter to function properly. At a minimum: 

  * An adapter configuration data collection named `adapter_config` needs to be created in the ClearBlade Platform _system_ and populated with data appropriate to the modbus client adapter adapter. The schema of the data collection should be as follows:

| Column Name      | Column Datatype |
| ---------------- | --------------- |
| adapter_name     | string          |
| adapter_settings | string          |
| topic_root       | string          |

## Adapter Settings Structure
The `adapter_settings` JSON string provided in the `adapter_config` collection is expected to have the following structure. This JSON is how you provide the Adapter with the specific OPC UA Server connection details.

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

### Supported Authentication types
| Authentication Type      | Details |
| ---------------- | --------------- |
| `Anonymous`     | Use anonymous authentiation, no other fields required in `authentication` object          |
| `Username` | Use username and password authentication, must include `username` and `password` key/values in `authentication` object  |
| `Certificate`       | Not Yet Implemented |

### Supported Security Mode
| Security Mode |
| ---------------- |
| `None` |
| `Sign` | 
| `SignAndEncrypt` |

### Supported Security Policies
| Security Mode |
| ---------------- |
| `none` |
| `Basic256` | 
| `Basic256SHA256` |

### Supported OPC UA Operations
| Security Mode |
| ---------------- |
| `read` |
| `write` | 
| `method` |
| `subscribe` |

## MQTT Topic Structure
The OPC UA adapter will subscribe to a specific topics in order to handle OPC UA operations. Additionally, the adapter will publish messages to MQTT topics for results of the OPC UA operations. The topic structures utilized are as follows:

 * OPC UA Read Request: {__TOPIC ROOT__}/read
 * OPC UA Read Results: {__TOPIC ROOT__}/read/response
 * OPC UA Write Request: {__TOPIC ROOT__}/write
 * OPC UA Write Response: {__TOPIC ROOT__}/write/response
 * OPC UA Method Request: {__TOPIC ROOT__}/method
 * OPC UA Method Response: {__TOPIC ROOT__}/method/response
 * OPC UA Subscribe Request: {__TOPIC ROOT__}/subscribe
   ** create, publish, and delete are the only supported services in the opcua library being utilized
 * OPC UA Subscribe Response: {__TOPIC ROOT__}/subscribe/response
 * OPC UA Publish: {__TOPIC ROOT__}/publish/response
## MQTT Message Structure

### OPC UA Read Request Payload Format
```json
{
  "node_ids": ["ns=3;i=1001", "ns=3;i=1002", "ns=3;i=1003", "ns=3;i=1004", "ns=3;i=1005", "ns=3;i=1006"]
}
```

### OPC UA Read Results Payload Format
 ```json
 {
  "timestamp": "2021-07-09T00:39:25Z",
  "data": {
    "ns=3;i=1001": 20,
    "ns=3;i=1002": 0.3681985,
    "ns=3;i=1003": -2,
    "ns=3;i=1004": 1.732051,
    "ns=3;i=1005": 2,
    "ns=3;i=1006": 1.333333
  }
}
 ```

### OPC UA Write Request Payload Format
```json
{
    "node_id": "ns=3;i=1001",
    "value": 25 // can be string or int/float
}
```

### OPC UA Write Response Payload Format
```json
{
    "node_id": "ns=3;i=1001",
    "timestamp": "", //ISO formatted timestamp
    "success": true|false,
    "status_code": 0, //Integer
    "error_message": "",
    "results": [0] //Array of status codes (integers)
}
```

### OPC UA Method Request Payload Format
```json
{
    "object_id": "ns=3;i=1001",
    "method_id": "", 
    "arguments":[] //Array of arguments
}
```

### OPC UA Method Response Payload Format
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

### OPC UA Subscription Request Payload Format
```json
{
    "request_type": "create|republish|delete", 
    "request_params": {
      //One of the following: (see below for schema)
      //
      //OPC UA Subscription Create Request
      //OPC UA Subscription Delete Request
      //OPC UA Subscription Republish Request
    },  
}
```

### OPC UA Subscription Create Request Payload Format
```json
{
    "publish_interval": ,
    "lifetime": 25,
    "keepalive": "", //ISO formatted timestamp 
    "max_publish_notifications": true|false,
    "priority": 0, //Integer
    "items_to_monitor": [
      {
        "node_id": "ns=2;i=1001"
      },
            {
        "node_id": "ns=3;i=1001"
      },
      ...
    ],
}
```


### OPC UA Subscription Delete Request Payload Format
```json
{
    "subscription_id": "the_subscription_id",
}
```

### OPC UA Subscribe Response Payload Format
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
        "value": 5
      },
      {
        "node_id": "ns=3;i=1001",
        "client_handle": 2,
        "value": "hello"
      },
      ...
    ],
}
```

## Starting the adapter
This adapter is built using the [adapter-go-library](https://github.com/ClearBlade/adapter-go-library) which allows for multiple options for starting the adapter, including CLI flags and environment variables. It is recommended to use a device service account for authentication with this adapter. See the below chart for available start options as well as their defaults.

All ClearBlade Adapters require a certain set of System specific variables to start and connect with the ClearBlade Platform/Edge. This library allows these to be passed in either by command line arguments, or environment variables. Note that command line arguments take precedence over environment variables.

| Name | CLI Flag | Environment Variable | Default |
| --- | --- | --- | --- |
| System Key | `systemKey` | `CB_SYSTEM_KEY` | N/A |
| System Secret | `systemSecret` | `CB_SYSTEM_SECRET` | N/A |
| Platform/Edge URL | `platformURL` | N/A | `http://localhost:9000` |
| Platform/Edge Messaging URL | `messagingURL` | N/A | `localhost:1883` |
| Device Name (**depreciated**) | `deviceName` | N/A | `adapterName` provided when calling `adapter_library.ParseArguments` |
| Device Password/Active Key (**depreciated)** | `password` | N/A | N/A |
| Device Service Account | N/A | `CB_SERVICE_ACCOUNT` | N/A |
| Device Service Account Token | N/A | `CB_SERVICE_ACCOUNT_TOKEN` | N/A |
| Log Level | `logLevel` | N/A | `info` |
| Adapter Config Collection Name | `adapterConfigCollection` | N/A | `adapter_config` |

A System Key and System Secret will always be required to start the adapter, and it's recommended to always use a Device Service Account & Token for Adapters. 

Device Name and Password for Adapters are **depreciated** and only provided for backwards compatibility and should not be used for any new adapters.

## Setup
---
The OPC UA Go adapter is dependent upon the ClearBlade Go SDK and its dependent libraries being installed. The OPC UA Go adapter was written in Go and therefore requires Go to be installed (https://golang.org/doc/install).

### Adapter compilation
In order to compile the adapter for execution, the following steps need to be performed:

 1. Retrieve the adapter source code  
    * ```git clone git@github.com:ClearBlade/opc-ua-go-adapter.git```
 2. Navigate to the _opc-ua-go-adapter_ directory  
    * ```cd opc-ua-go-adapter```
 3. Compile the adapter for your needed architecture and OS
    * ```GOARCH=arm GOARM=5 GOOS=linux go build```

