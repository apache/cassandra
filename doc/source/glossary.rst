.. glossary::

	Cassandra
	   Apache Cassandra is a distributed, high-available, eventually consistent NoSQL open-source database.
	
	cluster
	   Two or more database instances that exchange messages using the gossip protocol.

	commitlog
	   A file to which the database appends changed data for recovery in the event of a hardware failure.
	
	listen address
	   Address or interface to bind to and tell other Cassandra nodes to connect to

	seed node
	   A seed node is used to bootstrap the gossip process for new nodes joining a cluster. To learn the topology of the ring, a joining node contacts one of the nodes in the -seeds list in cassandra. yaml. The first time you bring up a node in a new cluster, only one node is the seed node.
