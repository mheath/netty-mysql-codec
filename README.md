# Netty MySQL Codec (Alpha)

Netty MySQL Codec provides Netty based MySQL/MariaDB encoders and decoders for use with both clients and servers.

## Implemented Features
* Capability negotiation
* Secure password authentication
* Text Protocol (query/result set encoding/decoding and misc. server commands)

## Road Map
* Binary protocol support (prepared statements)
* Security
  * SSL
  * SHA256 authentication
* Protocol level compression
* Stored procedures
* Replication protocols

## How to build

This project uses [Maven Wrapper](https://github.com/takari/maven-wrapper). To build it, ensure you have at least JDK
8 installed and simply run `./mvnw install` or `./mvnw.cmd install` on Windows.

## Running integration tests

TODO Document how to run the integration tests

## Examples
TODO Implement examples - client and server
