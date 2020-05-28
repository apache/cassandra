.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Improved Internode Messaging
------------------------------


Apache Cassandra 4.0 has added several new improvements to internode messaging.

Optimized Internode Messaging Protocol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The internode messaging protocol has been optimized (`CASSANDRA-14485
<https://issues.apache.org/jira/browse/CASSANDRA-14485>`_). Previously the ``IPAddressAndPort`` of the sender was included with each message that was sent even though the ``IPAddressAndPort`` had already been sent once when the initial connection/session was established. In Cassandra 4.0 ``IPAddressAndPort`` has been removed from every separate message sent  and only sent when connection/session is initiated.

Another improvement is that at several instances (listed) a fixed 4-byte integer value has been replaced with ``vint`` as a ``vint`` is almost always less than 1 byte:

-          The ``paramSize`` (the number of parameters in the header)
-          Each individual parameter value
-          The ``payloadSize``


NIO Messaging
^^^^^^^^^^^^^^^
In Cassandra 4.0 peer-to-peer (internode) messaging has been switched to non-blocking I/O (NIO) via Netty (`CASSANDRA-8457
<https://issues.apache.org/jira/browse/CASSANDRA-8457>`_).

As serialization format,  each message contains a header with several fixed fields, an optional key-value parameters section, and then the message payload itself. Note: the IP address in the header may be either IPv4 (4 bytes) or IPv6 (16 bytes).

  The diagram below shows the IPv4 address for brevity.

::

             1 1 1 1 1 2 2 2 2 2 3 3 3 3 3 4 4 4 4 4 5 5 5 5 5 6 6
   0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                       PROTOCOL MAGIC                          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         Message ID                            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         Timestamp                             |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |  Addr len |           IP Address (IPv4)                       /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /           |                 Verb                              /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /           |            Parameters size                        /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /           |             Parameter data                        /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                                                               |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        Payload size                           |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                                                               /
  /                           Payload                             /
  /                                                               |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

An individual parameter has a String key and a byte array value. The key is serialized with its length, encoded as two bytes, followed by the UTF-8 byte encoding of the string. The body is serialized with its length, encoded as four bytes, followed by the bytes of the value.

Resource limits on Queued Messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
System stability is improved by enforcing strict resource limits (`CASSANDRA-15066
<https://issues.apache.org/jira/browse/CASSANDRA-15066>`_) on the number of outbound messages that are queued, measured by the ``serializedSize`` of the message. There are three separate limits imposed simultaneously to ensure that progress is always made without any reasonable combination of failures impacting a node’s stability.

1. Global, per-endpoint and per-connection limits are imposed on messages queued for delivery to other nodes and waiting to be processed on arrival from other nodes in the cluster.  These limits are applied to the on-wire size of the message being sent or received.
2. The basic per-link limit is consumed in isolation before any endpoint or global limit is imposed. Each node-pair has three links: urgent, small and large.  So any given node may have a maximum of ``N*3 * (internode_application_send_queue_capacity_in_bytes + internode_application_receive_queue_capacity_in_bytes)`` messages queued without any coordination between them although in practice, with token-aware routing, only RF*tokens nodes should need to communicate with significant bandwidth.
3. The per-endpoint limit is imposed on all messages exceeding the per-link limit, simultaneously with the global limit, on all links to or from a single node in the cluster. The global limit is imposed on all messages exceeding the per-link limit, simultaneously with the per-endpoint limit, on all links to or from any node in the cluster. The following configuration settings have been added to ``cassandra.yaml`` for resource limits on queued messages.

::

 internode_application_send_queue_capacity_in_bytes: 4194304 #4MiB
 internode_application_send_queue_reserve_endpoint_capacity_in_bytes: 134217728  #128MiB
 internode_application_send_queue_reserve_global_capacity_in_bytes: 536870912    #512MiB
 internode_application_receive_queue_capacity_in_bytes: 4194304                  #4MiB
 internode_application_receive_queue_reserve_endpoint_capacity_in_bytes: 134217728 #128MiB
 internode_application_receive_queue_reserve_global_capacity_in_bytes: 536870912   #512MiB

Virtual Tables for Messaging Metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Metrics is improved by keeping metrics using virtual tables for inter-node inbound and outbound messaging (`CASSANDRA-15066
<https://issues.apache.org/jira/browse/CASSANDRA-15066>`_). For inbound messaging a  virtual table (``internode_inbound``) has been added to keep metrics for:

- Bytes and count of messages that could not be serialized or flushed due to an error
- Bytes and count of messages scheduled
- Bytes and count of messages successfully processed
- Bytes and count of messages successfully received
- Nanos and count of messages throttled
- Bytes and count of messages expired
- Corrupt frames recovered and unrecovered

A separate virtual table (``internode_outbound``) has been added for outbound inter-node messaging. The outbound virtual table keeps metrics for:

-          Bytes and count of messages  pending
-          Bytes and count of messages  sent
-          Bytes and count of messages  expired
-          Bytes and count of messages that could not be sent due to an error
-          Bytes and count of messages overloaded
-          Active Connection Count
-          Connection Attempts
-          Successful Connection Attempts

Hint Messaging
^^^^^^^^^^^^^^

A specialized version of hint message that takes an already encoded in a ``ByteBuffer`` hint and sends it verbatim has been added. It is an optimization for when dispatching a hint file of the current messaging version to a node of the same messaging version, which is the most common case. It saves on extra ``ByteBuffer`` allocations one redundant hint deserialization-serialization cycle.

Internode Application Timeout
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A configuration setting has been added to ``cassandra.yaml`` for the maximum continuous period a connection may be unwritable in application space.

::

# internode_application_timeout_in_ms = 30000

Some other new features include logging of message size to trace message for tracing a query.

Paxos prepare and propose stage for local requests optimized
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In pre-4.0 Paxos prepare and propose messages always go through entire ``MessagingService`` stack in Cassandra even if request is to be served locally, we can enhance and make local requests severed w/o involving ``MessagingService``. Similar things are done elsewhere in Cassandra which skips ``MessagingService`` stage for local requests.

This is what it looks like in pre 4.0 if we have tracing on and run a light-weight transaction:

::

 Sending PAXOS_PREPARE message to /A.B.C.D [MessagingService-Outgoing-/A.B.C.D] | 2017-09-11
 21:55:18.971000 | A.B.C.D | 15045
 … REQUEST_RESPONSE message received from /A.B.C.D [MessagingService-Incoming-/A.B.C.D] |
 2017-09-11 21:55:18.976000 | A.B.C.D | 20270
 … Processing response from /A.B.C.D [SharedPool-Worker-4] | 2017-09-11 21:55:18.976000 |
 A.B.C.D | 20372

Same thing applies for Propose stage as well.

In version 4.0 Paxos prepare and propose stage for local requests are optimized (`CASSANDRA-13862
<https://issues.apache.org/jira/browse/CASSANDRA-13862>`_).

Quality Assurance
^^^^^^^^^^^^^^^^^

Several other quality assurance improvements have been made in version 4.0 (`CASSANDRA-15066
<https://issues.apache.org/jira/browse/CASSANDRA-15066>`_).

Framing
*******
Version 4.0 introduces framing to all internode messages, i.e. the grouping of messages into a single logical payload with headers and trailers; these frames are guaranteed to either contain at most one message, that is split into its own unique sequence of frames (for large messages), or that a frame contains only complete messages.

Corruption prevention
*********************
Previously, intra-datacenter internode messages would be unprotected from corruption by default, as only LZ4 provided any integrity checks. All messages to post 4.0 nodes are written to explicit frames, which may be:

- LZ4 encoded
- CRC protected

The Unprotected option is still available.

Resilience
**********
For resilience, all frames are written with a separate CRC protected header, of 8 and 6 bytes respectively. If corruption occurs in this header, the connection must be reset, as before. If corruption occurs anywhere outside of the header, the corrupt frame will be skipped, leaving the connection intact and avoiding the loss of any messages unnecessarily.

Previously, any issue at any point in the stream would result in the connection being reset, with the loss of any in-flight messages.

Efficiency
**********
The overall memory usage, and number of byte shuffles, on both inbound and outbound messages is reduced.

Outbound the Netty LZ4 encoder maintains a chunk size buffer (64KiB), that is filled before any compressed frame can be produced. Our frame encoders avoid this redundant copy, as well as freeing 192KiB per endpoint.

Inbound, frame decoders guarantee only to copy the number of bytes necessary to parse a frame, and to never store more bytes than necessary. This improvement applies twice to LZ4 connections, improving both the message decode and the LZ4 frame decode.

Inbound Path
************
Version 4.0 introduces several improvements to the inbound path.

An appropriate message handler is used based on whether large or small messages are expected on a particular connection as set in a flag. ``NonblockingBufferHandler``, running on event loop, is used for small messages, and ``BlockingBufferHandler``, running off event loop, for large messages. The single implementation of ``InboundMessageHandler`` handles messages of any size effectively by deriving size of the incoming message from the byte stream. In addition to deriving size of the message from the stream, incoming message expiration time is proactively read, before attempting to deserialize the entire message. If it’s expired at the time when a message is encountered the message is just skipped in the byte stream altogether.
And if a message fails to be deserialized while still on the receiving side - say, because of table id or column being unknown - bytes are skipped, without dropping the entire connection and losing all the buffered messages. An immediately reply back is sent to the coordinator node with the failure reason, rather than waiting for the coordinator callback to expire. This logic is extended to a corrupted frame; a corrupted frame is safely skipped over without dropping the connection.

Inbound path imposes strict limits on memory utilization. Specifically, the memory occupied by all parsed, but unprocessed messages is bound - on per-connection, per-endpoint, and global basis. Once a connection exceeds its local unprocessed capacity and cannot borrow any permits from per-endpoint and global reserve, it simply stops processing further messages, providing natural backpressure - until sufficient capacity is regained.

Outbound Connections
********************

Opening a connection
++++++++++++++++++++
A consistent approach is adopted for all kinds of failure to connect, including: refused by endpoint, incompatible versions, or unexpected exceptions;

- Retry forever, until either success or no messages waiting to deliver.
- Wait incrementally longer periods before reconnecting, up to a maximum of 1s.
- While failing to connect, no reserve queue limits are acquired.

Closing a connection
++++++++++++++++++++
- Correctly drains outbound messages that are waiting to be delivered (unless disconnected and fail to reconnect).
- Messages written to a closing connection are either delivered or rejected, with a new connection being opened if the old is irrevocably closed.
- Unused connections are pruned eventually.

Reconnecting
++++++++++++

We sometimes need to reconnect a perfectly valid connection, e.g. if the preferred IP address changes. We ensure that the underlying connection has no in-progress operations before closing it and reconnecting.

Message Failure
++++++++++++++++
Propagates to callbacks instantly, better preventing overload by reclaiming committed memory.

Expiry
~~~~~~~~
- No longer experiences head-of-line blocking (e.g. undroppable message preventing all droppable messages from being expired).
- While overloaded, expiry is attempted eagerly on enqueuing threads.
- While disconnected we schedule regular pruning, to handle the case where messages are no longer being sent, but we have a large backlog to expire.

Overload
~~~~~~~~~
- Tracked by bytes queued, as opposed to number of messages.

Serialization Errors
~~~~~~~~~~~~~~~~~~~~~
- Do not result in the connection being invalidated; the message is simply completed with failure, and then erased from the frame.
- Includes detected mismatch between calculated serialization size to actual.

Failures to flush to network, perhaps because the connection has been reset are not currently notified to callback handlers, as the necessary information has been discarded, though it would be possible to do so in future if we decide it is worth our while.

QoS
+++++
"Gossip" connection has been replaced with a general purpose "Urgent" connection, for any small messages impacting system stability.

Metrics
+++++++
We track, and expose via Virtual Table and JMX, the number of messages and bytes that: we could not serialize or flush due to an error, we dropped due to overload or timeout, are pending, and have successfully sent.

Added a Message size limit
^^^^^^^^^^^^^^^^^^^^^^^^^^

Cassandra pre-4.0 doesn't protect the server from allocating huge buffers for the inter-node Message objects. Adding a message size limit would be good to deal with issues such as a malfunctioning cluster participant. Version 4.0 introduced max message size config param, akin to max mutation size - set to endpoint reserve capacity by default.

Recover from unknown table when deserializing internode messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
As discussed in (`CASSANDRA-9289
<https://issues.apache.org/jira/browse/CASSANDRA-9289>`_) it would be nice to gracefully recover from seeing an unknown table in a message from another node. Pre-4.0, we close the connection and reconnect, which can cause other concurrent queries to fail.
Version 4.0  fixes the issue by wrapping message in-stream with
``TrackedDataInputPlus``, catching
``UnknownCFException``, and skipping the remaining bytes in this message. TCP won't be closed and it will remain connected for other messages.
