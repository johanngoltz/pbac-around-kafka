package main

import (
	"inet.af/tcpproxy"
	"log"
)

func main() {
	var p tcpproxy.Proxy
	p.AddRoute(":9093", tcpproxy.To("localhost:9092"))
	log.Fatal(p.Run())
}
