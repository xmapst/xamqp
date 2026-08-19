// Command cluster demonstrates connecting xamqp to a RabbitMQ cluster
// through the IResolver abstraction instead of a single fixed URL.
//
// Two variants are shown:
//
//  1. NewStaticResolver over a fixed list of node URLs with shuffle=true,
//     which gives simple client-side load spreading: each call to
//     Resolve() (i.e. each initial connect and each reconnect) returns the
//     node list in a random order, so connections aren't always pinned to
//     the first node in the list.
//
//  2. A custom type implementing IResolver's single method,
//     Resolve() ([]string, error), to illustrate how a real deployment
//     could plug in dynamic node selection - e.g. querying a service
//     registry (Consul/etcd/DNS SRV/k8s Endpoints) at connect/reconnect
//     time instead of relying on a static list.
package main

import (
	"log"

	"github.com/xmapst/xamqp"
)

// serviceDiscoveryResolver is a toy stand-in for a resolver backed by a
// service discovery system. It implements xamqp.IResolver by exposing a
// Resolve() ([]string, error) method; a real implementation would replace
// the body below with a lookup against Consul, etcd, DNS SRV records, a
// Kubernetes Endpoints/EndpointSlice watch, etc.
type serviceDiscoveryResolver struct {
	nodes []string
}

// Resolve satisfies xamqp.IResolver. It is called once on the initial
// connection attempt and again on every reconnect, so it's the right place
// to re-query a discovery backend for the currently healthy node set.
func (r *serviceDiscoveryResolver) Resolve() ([]string, error) {
	log.Println("service discovery: resolving current cluster nodes...")
	return r.nodes, nil
}

func main() {
	staticClusterExample()
	serviceDiscoveryClusterExample()
}

// staticClusterExample connects using a fixed list of node URLs, shuffled
// on every resolve for simple client-side load spreading across nodes.
func staticClusterExample() {
	resolver := xamqp.NewStaticResolver(
		[]string{
			"amqp://guest:guest@rabbit-node1:5672/",
			"amqp://guest:guest@rabbit-node2:5672/",
			"amqp://guest:guest@rabbit-node3:5672/",
		},
		true, /* shuffle: randomize node order on each connect/reconnect */
	)

	conn, err := xamqp.NewClusterConn(
		resolver,
		xamqp.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatalf("static resolver: failed to connect to cluster: %v", err)
	}
	defer conn.Close()

	log.Println("static resolver: connected to cluster successfully")
}

// serviceDiscoveryClusterExample connects using a custom IResolver
// implementation, showing where service-discovery-based node selection
// would plug in.
func serviceDiscoveryClusterExample() {
	resolver := &serviceDiscoveryResolver{
		nodes: []string{
			"amqp://guest:guest@rabbit-node1:5672/",
			"amqp://guest:guest@rabbit-node2:5672/",
		},
	}

	conn, err := xamqp.NewClusterConn(
		resolver,
		xamqp.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatalf("custom resolver: failed to connect to cluster: %v", err)
	}
	defer conn.Close()

	log.Println("custom resolver: connected to cluster successfully")
}
