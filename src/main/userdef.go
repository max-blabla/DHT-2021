package main

import (
	"DHT-2021/src/main/dht"
	)
/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */


func NewNode(port int) dhtNode {
	// Todo: create a node and then return it.
	return dht.NewSurface(port)
}

// Todo: implement a struct which implements the interface "dhtNode".
