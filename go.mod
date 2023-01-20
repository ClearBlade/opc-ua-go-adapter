module opc-ua-go-adapter

go 1.17

require github.com/clearblade/adapter-go-library v0.0.1

require github.com/clearblade/Go-SDK v0.0.0-20211102204418-b07beccdda68 // indirect

require (
	github.com/clearblade/go-utils v1.1.4 // indirect
	github.com/clearblade/mqtt_parsing v0.0.0-20160301165118-6ae49eac0961
	github.com/clearblade/paho.mqtt.golang v1.1.1
	github.com/gopcua/opcua v0.3.7
	github.com/hashicorp/logutils v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/net v0.0.0-20220121210141-e204ce36a2ba // indirect
)

replace github.com/clearblade/adapter-go-library => /Users/weston/go/src/adapter-go-library

replace github.com/clearblade/Go-SDK => /Users/weston/go/src/Go-SDK

replace github.com/gopcua/opcua => github.com/clearblade/opcua v0.0.0-20230117232703-7f6ab74913da
