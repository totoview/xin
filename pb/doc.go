// Package pb contains protobuf definitions of all the messages and services.
// The common definitions shared by different services are located in this
// directory. Service specific definitions are located in their own subdirectories
// respectively.
//
// protobuf related interface definitions, utilies and code generated protobuf
// artifacts are also located under this directory based on their package groupings.
//
// Note that we use camel-case for enum values instead of the recommended snake-case
// in uppercase to make it easy for code generation tools to map enum values to
// other type names that are in camel-case.
//
// All messages sent to Kafka are encoded in protobuf. Avro is the most popular format
// in the Kafka community. We use protobuf instead to be consistent with the RPC framework
// we chose.
package pb
