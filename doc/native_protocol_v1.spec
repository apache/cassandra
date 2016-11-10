
                             CQL BINARY PROTOCOL v1


Table of Contents

  1. Overview
  2. Frame header
    2.1. version
    2.2. flags
    2.3. stream
    2.4. opcode
    2.5. length
  3. Notations
  4. Messages
    4.1. Requests
      4.1.1. STARTUP
      4.1.2. CREDENTIALS
      4.1.3. OPTIONS
      4.1.4. QUERY
      4.1.5. PREPARE
      4.1.6. EXECUTE
      4.1.7. REGISTER
    4.2. Responses
      4.2.1. ERROR
      4.2.2. READY
      4.2.3. AUTHENTICATE
      4.2.4. SUPPORTED
      4.2.5. RESULT
        4.2.5.1. Void
        4.2.5.2. Rows
        4.2.5.3. Set_keyspace
        4.2.5.4. Prepared
        4.2.5.5. Schema_change
      4.2.6. EVENT
  5. Compression
  6. Data Type Serialization Formats
  7. Error codes


1. Overview

  The CQL binary protocol is a frame based protocol. Frames are defined as:

      0         8        16        24        32
      +---------+---------+---------+---------+
      | version |  flags  | stream  | opcode  |
      +---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
      |                                       |
      .            ...  body ...              .
      .                                       .
      .                                       .
      +----------------------------------------

  The protocol is big-endian (network byte order).

  Each frame contains a fixed size header (8 bytes) followed by a variable size
  body. The header is described in Section 2. The content of the body depends
  on the header opcode value (the body can in particular be empty for some
  opcode values). The list of allowed opcode is defined Section 2.3 and the
  details of each corresponding message is described Section 4.

  The protocol distinguishes 2 types of frames: requests and responses. Requests
  are those frame sent by the clients to the server, response are the ones sent
  by the server. Note however that while communication are initiated by the
  client with the server responding to request, the protocol may likely add
  server pushes in the future, so responses does not obligatory come right after
  a client request.

  Note to client implementors: clients library should always assume that the
  body of a given frame may contain more data than what is described in this
  document. It will however always be safe to ignore the remaining of the frame
  body in such cases. The reason is that this may allow to sometimes extend the
  protocol with optional features without needing to change the protocol
  version.


2. Frame header

2.1. version

  The version is a single byte that indicate both the direction of the message
  (request or response) and the version of the protocol in use. The up-most bit
  of version is used to define the direction of the message: 0 indicates a
  request, 1 indicates a responses. This can be useful for protocol analyzers to
  distinguish the nature of the packet from the direction which it is moving.
  The rest of that byte is the protocol version (1 for the protocol defined in
  this document). In other words, for this version of the protocol, version will
  have one of:
    0x01    Request frame for this protocol version
    0x81    Response frame for this protocol version


2.2. flags

  Flags applying to this frame. The flags have the following meaning (described
  by the mask that allow to select them):
    0x01: Compression flag. If set, the frame body is compressed. The actual
          compression to use should have been set up beforehand through the
          Startup message (which thus cannot be compressed; Section 4.1.1).
    0x02: Tracing flag. For a request frame, this indicate the client requires
          tracing of the request. Note that not all requests support tracing.
          Currently, only QUERY, PREPARE and EXECUTE queries support tracing.
          Other requests will simply ignore the tracing flag if set. If a
          request support tracing and the tracing flag was set, the response to
          this request will have the tracing flag set and contain tracing
          information.
          If a response frame has the tracing flag set, its body contains
          a tracing ID. The tracing ID is a [uuid] and is the first thing in
          the frame body. The rest of the body will then be the usual body
          corresponding to the response opcode.

  The rest of the flags is currently unused and ignored.

2.3. stream

  A frame has a stream id (one signed byte). When sending request messages, this
  stream id must be set by the client to a positive byte (negative stream id
  are reserved for streams initiated by the server; currently all EVENT messages
  (section 4.2.6) have a streamId of -1). If a client sends a request message
  with the stream id X, it is guaranteed that the stream id of the response to
  that message will be X.

  This allow to deal with the asynchronous nature of the protocol. If a client
  sends multiple messages simultaneously (without waiting for responses), there
  is no guarantee on the order of the responses. For instance, if the client
  writes REQ_1, REQ_2, REQ_3 on the wire (in that order), the server might
  respond to REQ_3 (or REQ_2) first. Assigning different stream id to these 3
  requests allows the client to distinguish to which request an received answer
  respond to. As there can only be 128 different simultaneous stream, it is up
  to the client to reuse stream id.

  Note that clients are free to use the protocol synchronously (i.e. wait for
  the response to REQ_N before sending REQ_N+1). In that case, the stream id
  can be safely set to 0. Clients should also feel free to use only a subset of
  the 128 maximum possible stream ids if it is simpler for those
  implementation.

2.4. opcode

  An integer byte that distinguish the actual message:
    0x00    ERROR
    0x01    STARTUP
    0x02    READY
    0x03    AUTHENTICATE
    0x04    CREDENTIALS
    0x05    OPTIONS
    0x06    SUPPORTED
    0x07    QUERY
    0x08    RESULT
    0x09    PREPARE
    0x0A    EXECUTE
    0x0B    REGISTER
    0x0C    EVENT

  Messages are described in Section 4.


2.5. length

  A 4 byte integer representing the length of the body of the frame (note:
  currently a frame is limited to 256MB in length).


3. Notations

  To describe the layout of the frame body for the messages in Section 4, we
  define the following:

    [int]          A 4 byte integer
    [short]        A 2 byte unsigned integer
    [string]       A [short] n, followed by n bytes representing an UTF-8
                   string.
    [long string]  An [int] n, followed by n bytes representing an UTF-8 string.
    [uuid]         A 16 bytes long uuid.
    [string list]  A [short] n, followed by n [string].
    [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
                   no byte should follow and the value represented is `null`.
    [short bytes]  A [short] n, followed by n bytes if n >= 0.

    [option]       A pair of <id><value> where <id> is a [short] representing
                   the option id and <value> depends on that option (and can be
                   of size 0). The supported id (and the corresponding <value>)
                   will be described when this is used.
    [option list]  A [short] n, followed by n [option].
    [inet]         An address (ip and port) to a node. It consists of one
                   [byte] n, that represents the address size, followed by n
                   [byte] representing the IP address (in practice n can only be
                   either 4 (IPv4) or 16 (IPv6)), following by one [int]
                   representing the port.
    [consistency]  A consistency level specification. This is a [short]
                   representing a consistency level with the following
                   correspondance:
                     0x0000    ANY
                     0x0001    ONE
                     0x0002    TWO
                     0x0003    THREE
                     0x0004    QUORUM
                     0x0005    ALL
                     0x0006    LOCAL_QUORUM
                     0x0007    EACH_QUORUM
                     0x000A    LOCAL_ONE

    [string map]      A [short] n, followed by n pair <k><v> where <k> and <v>
                      are [string].
    [string multimap] A [short] n, followed by n pair <k><v> where <k> is a
                      [string] and <v> is a [string list].


4. Messages

4.1. Requests

  Note that outside of their normal responses (described below), all requests
  can get an ERROR message (Section 4.2.1) as response.

4.1.1. STARTUP

  Initialize the connection. The server will respond by either a READY message
  (in which case the connection is ready for queries) or an AUTHENTICATE message
  (in which case credentials will need to be provided using CREDENTIALS).

  This must be the first message of the connection, except for OPTIONS that can
  be sent before to find out the options supported by the server. Once the
  connection has been initialized, a client should not send any more STARTUP
  message.

  The body is a [string map] of options. Possible options are:
    - "CQL_VERSION": the version of CQL to use. This option is mandatory and
      currenty, the only version supported is "3.0.0". Note that this is
      different from the protocol version.
    - "COMPRESSION": the compression algorithm to use for frames (See section 5).
      This is optional, if not specified no compression will be used.


4.1.2. CREDENTIALS

  Provides credentials information for the purpose of identification. This
  message comes as a response to an AUTHENTICATE message from the server, but
  can be use later in the communication to change the authentication
  information.

  The body is a list of key/value informations. It is a [short] n, followed by n
  pair of [string]. These key/value pairs are passed as is to the Cassandra
  IAuthenticator and thus the detail of which informations is needed depends on
  that authenticator.

  The response to a CREDENTIALS is a READY message (or an ERROR message).


4.1.3. OPTIONS

  Asks the server to return what STARTUP options are supported. The body of an
  OPTIONS message should be empty and the server will respond with a SUPPORTED
  message.


4.1.4. QUERY

  Performs a CQL query. The body of the message consists of a CQL query as a [long
  string] followed by the [consistency] for the operation.

  Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
  TRUNCATE, ...).

  The server will respond to a QUERY message with a RESULT message, the content
  of which depends on the query.


4.1.5. PREPARE

  Prepare a query for later execution (through EXECUTE). The body consists of
  the CQL query to prepare as a [long string].

  The server will respond with a RESULT message with a `prepared` kind (0x0004,
  see Section 4.2.5).


4.1.6. EXECUTE

  Executes a prepared query. The body of the message must be:
    <id><n><value_1>....<value_n><consistency>
  where:
    - <id> is the prepared query ID. It's the [short bytes] returned as a
      response to a PREPARE message.
    - <n> is a [short] indicating the number of following values.
    - <value_1>...<value_n> are the [bytes] to use for bound variables in the
      prepared query.
    - <consistency> is the [consistency] level for the operation.

  Note that the consistency is ignored by some (prepared) queries (USE, CREATE,
  ALTER, TRUNCATE, ...).

  The response from the server will be a RESULT message.


4.1.7. REGISTER

  Register this connection to receive some type of events. The body of the
  message is a [string list] representing the event types to register to. See
  section 4.2.6 for the list of valid event types.

  The response to a REGISTER message will be a READY message.

  Please note that if a client driver maintains multiple connections to a
  Cassandra node and/or connections to multiple nodes, it is advised to
  dedicate a handful of connections to receive events, but to *not* register
  for events on all connections, as this would only result in receiving
  multiple times the same event messages, wasting bandwidth.


4.2. Responses

  This section describes the content of the frame body for the different
  responses. Please note that to make room for future evolution, clients should
  support extra informations (that they should simply discard) to the one
  described in this document at the end of the frame body.

4.2.1. ERROR

  Indicates an error processing a request. The body of the message will be an
  error code ([int]) followed by a [string] error message. Then, depending on
  the exception, more content may follow. The error codes are defined in
  Section 7, along with their additional content if any.


4.2.2. READY

  Indicates that the server is ready to process queries. This message will be
  sent by the server either after a STARTUP message if no authentication is
  required, or after a successful CREDENTIALS message.

  The body of a READY message is empty.


4.2.3. AUTHENTICATE

  Indicates that the server require authentication. This will be sent following
  a STARTUP message and must be answered by a CREDENTIALS message from the
  client to provide authentication informations.

  The body consists of a single [string] indicating the full class name of the
  IAuthenticator in use.


4.2.4. SUPPORTED

  Indicates which startup options are supported by the server. This message
  comes as a response to an OPTIONS message.

  The body of a SUPPORTED message is a [string multimap]. This multimap gives
  for each of the supported STARTUP options, the list of supported values.


4.2.5. RESULT

  The result to a query (QUERY, PREPARE or EXECUTE messages).

  The first element of the body of a RESULT message is an [int] representing the
  `kind` of result. The rest of the body depends on the kind. The kind can be
  one of:
    0x0001    Void: for results carrying no information.
    0x0002    Rows: for results to select queries, returning a set of rows.
    0x0003    Set_keyspace: the result to a `use` query.
    0x0004    Prepared: result to a PREPARE message.
    0x0005    Schema_change: the result to a schema altering query.

  The body for each kind (after the [int] kind) is defined below.


4.2.5.1. Void

  The rest of the body for a Void result is empty. It indicates that a query was
  successful without providing more information.


4.2.5.2. Rows

  Indicates a set of rows. The rest of body of a Rows result is:
    <metadata><rows_count><rows_content>
  where:
    - <metadata> is composed of:
        <flags><columns_count><global_table_spec>?<col_spec_1>...<col_spec_n>
      where:
        - <flags> is an [int]. The bits of <flags> provides information on the
          formatting of the remaining informations. A flag is set if the bit
          corresponding to its `mask` is set. Supported flags are, given there
          mask:
            0x0001    Global_tables_spec: if set, only one table spec (keyspace
                      and table name) is provided as <global_table_spec>. If not
                      set, <global_table_spec> is not present.
        - <columns_count> is an [int] representing the number of columns selected
          by the query this result is of. It defines the number of <col_spec_i>
          elements in and the number of element for each row in <rows_content>.
        - <global_table_spec> is present if the Global_tables_spec is set in
          <flags>. If present, it is composed of two [string] representing the
          (unique) keyspace name and table name the columns return are of.
        - <col_spec_i> specifies the columns returned in the query. There is
          <column_count> such column specification that are composed of:
            (<ksname><tablename>)?<column_name><type>
          The initial <ksname> and <tablename> are two [string] are only present
          if the Global_tables_spec flag is not set. The <column_name> is a
          [string] and <type> is an [option] that correspond to the column name
          and type. The option for <type> is either a native type (see below),
          in which case the option has no value, or a 'custom' type, in which
          case the value is a [string] representing the full qualified class
          name of the type represented. Valid option ids are:
            0x0000    Custom: the value is a [string], see above.
            0x0001    Ascii
            0x0002    Bigint
            0x0003    Blob
            0x0004    Boolean
            0x0005    Counter
            0x0006    Decimal
            0x0007    Double
            0x0008    Float
            0x0009    Int
            0x000A    Text
            0x000B    Timestamp
            0x000C    Uuid
            0x000D    Varchar
            0x000E    Varint
            0x000F    Timeuuid
            0x0010    Inet
            0x0020    List: the value is an [option], representing the type
                            of the elements of the list.
            0x0021    Map: the value is two [option], representing the types of the
                           keys and values of the map
            0x0022    Set: the value is an [option], representing the type
                            of the elements of the set
    - <rows_count> is an [int] representing the number of rows present in this
      result. Those rows are serialized in the <rows_content> part.
    - <rows_content> is composed of <row_1>...<row_m> where m is <rows_count>.
      Each <row_i> is composed of <value_1>...<value_n> where n is
      <columns_count> and where <value_j> is a [bytes] representing the value
      returned for the jth column of the ith row. In other words, <rows_content>
      is composed of (<rows_count> * <columns_count>) [bytes].


4.2.5.3. Set_keyspace

  The result to a `use` query. The body (after the kind [int]) is a single
  [string] indicating the name of the keyspace that has been set.


4.2.5.4. Prepared

  The result to a PREPARE message. The rest of the body of a Prepared result is:
    <id><metadata>
  where:
    - <id> is [short bytes] representing the prepared query ID.
    - <metadata> is defined exactly as for a Rows RESULT (See section 4.2.5.2).

  Note that prepared query ID return is global to the node on which the query
  has been prepared. It can be used on any connection to that node and this
  until the node is restarted (after which the query must be reprepared).

4.2.5.5. Schema_change

  The result to a schema altering query (creation/update/drop of a
  keyspace/table/index). The body (after the kind [int]) is composed of 3
  [string]:
    <change><keyspace><table>
  where:
    - <change> describe the type of change that has occured. It can be one of
      "CREATED", "UPDATED" or "DROPPED".
    - <keyspace> is the name of the affected keyspace or the keyspace of the
      affected table.
    - <table> is the name of the affected table. <table> will be empty (i.e.
      the empty string "") if the change was affecting a keyspace and not a
      table.

  Note that queries to create and drop an index are considered changes
  updating the table the index is on.  Queries that create, alter, or drop
  user-defined types (availble in Cassandra 2.1+) are considered changes
  updating the keyspace the type is defined in.


4.2.6. EVENT

  And event pushed by the server. A client will only receive events for the
  type it has REGISTER to. The body of an EVENT message will start by a
  [string] representing the event type. The rest of the message depends on the
  event type. The valid event types are:
    - "TOPOLOGY_CHANGE": events related to change in the cluster topology.
      Currently, events are sent when new nodes are added to the cluster, and
      when nodes are removed. The body of the message (after the event type)
      consists of a [string] and an [inet], corresponding respectively to the
      type of change ("NEW_NODE", "REMOVED_NODE", or "MOVED_NODE") followed
      by the address of the new/removed/moved node.
    - "STATUS_CHANGE": events related to change of node status. Currently,
      up/down events are sent. The body of the message (after the event type)
      consists of a [string] and an [inet], corresponding respectively to the
      type of status change ("UP" or "DOWN") followed by the address of the
      concerned node.
    - "SCHEMA_CHANGE": events related to schema change. The body of the message
      (after the event type) consists of 3 [string] corresponding respectively
      to the type of schema change ("CREATED", "UPDATED" or "DROPPED"),
      followed by the name of the affected keyspace and the name of the
      affected table within that keyspace. For changes that affect a keyspace
      directly, the table name will be empty (i.e. the empty string "").
      Changes to user-defined types (available in Cassandra 2.1+) will result
      in an "UPDATED" change for the keyspace containing the type, and the
      table name will be empty.

  All EVENT message have a streamId of -1 (Section 2.3).

  Please note that "NEW_NODE" and "UP" events are sent based on internal Gossip
  communication and as such may be sent a short delay before the binary
  protocol server on the newly up node is fully started. Clients are thus
  advise to wait a short time before trying to connect to the node (1 seconds
  should be enough), otherwise they may experience a connection refusal at
  first.

  It is possible for the same event to be sent multiple times. Therefore,
  a client library should ignore the same event if it has already been notified
  of a change.

5. Compression

  Frame compression is supported by the protocol, but then only the frame body
  is compressed (the frame header should never be compressed).

  Before being used, client and server must agree on a compression algorithm to
  use, which is done in the STARTUP message. As a consequence, a STARTUP message
  must never be compressed.  However, once the STARTUP frame has been received
  by the server can be compressed (including the response to the STARTUP
  request). Frame do not have to be compressed however, even if compression has
  been agreed upon (a server may only compress frame above a certain size at its
  discretion). A frame body should be compressed if and only if the compressed
  flag (see Section 2.2) is set.


6. Data Type Serialization Formats

  This sections describes the serialization formats for all CQL data types
  supported by Cassandra through the native protocol.  These serialization
  formats should be used by client drivers to encode values for EXECUTE
  messages.  Cassandra will use these formats when returning values in
  RESULT messages.

  All values are represented as [bytes] in EXECUTE and RESULT messages.
  The [bytes] format includes an int prefix denoting the length of the value.
  For that reason, the serialization formats described here will not include
  a length component.

  For legacy compatibility reasons, note that most non-string types support
  "empty" values (i.e. a value with zero length).  An empty value is distinct
  from NULL, which is encoded with a negative length.

  As with the rest of the native protocol, all encodings are big-endian.

6.1. ascii

  A sequence of bytes in the ASCII range [0, 127].  Bytes with values outside of
  this range will result in a validation error.

6.2 bigint

  An eight-byte two's complement integer.

6.3 blob

  Any sequence of bytes.

6.4 boolean

  A single byte.  A value of 0 denotes "false"; any other value denotes "true".
  (However, it is recommended that a value of 1 be used to represent "true".)

6.5 decimal

  The decimal format represents an arbitrary-precision number.  It contains an
  [int] "scale" component followed by a varint encoding (see section 6.17)
  of the unscaled value.  The encoded value represents "<unscaled>E<-scale>".
  In other words, "<unscaled> * 10 ^ (-1 * <scale>)".

6.6 double

  An eight-byte floating point number in the IEEE 754 binary64 format.

6.7 float

  An four-byte floating point number in the IEEE 754 binary32 format.

6.8 inet

  A 4 byte or 16 byte sequence denoting an IPv4 or IPv6 address, respectively.

6.9 int

  A four-byte two's complement integer.

6.10 list

  A [short] n indicating the number of elements in the list, followed by n
  elements.  Each element is [short bytes] representing the serialized value.

6.11 map

  A [short] n indicating the number of key/value pairs in the map, followed by
  n entries.  Each entry is composed of two [short bytes] representing the key
  and value.

6.12 set

  A [short] n indicating the number of elements in the set, followed by n
  elements.  Each element is [short bytes] representing the serialized value.

6.13 text

  A sequence of bytes conforming to the UTF-8 specifications.

6.14 timestamp

  An eight-byte two's complement integer representing a millisecond-precision
  offset from the unix epoch (00:00:00, January 1st, 1970).  Negative values
  represent a negative offset from the epoch.

6.15 uuid

  A 16 byte sequence representing any valid UUID as defined by RFC 4122.

6.16 varchar

  An alias of the "text" type.

6.17 varint

  A variable-length two's complement encoding of a signed integer.

  The following examples may help implementors of this spec:

  Value | Encoding
  ------|---------
      0 |     0x00
      1 |     0x01
    127 |     0x7F
    128 |   0x0080
     -1 |     0xFF
   -128 |     0x80
   -129 |   0xFF7F

  Note that positive numbers must use a most-significant byte with a value
  less than 0x80, because a most-significant bit of 1 indicates a negative
  value.  Implementors should pad positive values that have a MSB >= 0x80
  with a leading 0x00 byte.

6.18 timeuuid

  A 16 byte sequence representing a version 1 UUID as defined by RFC 4122.


7. Error codes

  The supported error codes are described below:
    0x0000    Server error: something unexpected happened. This indicates a
              server-side bug.
    0x000A    Protocol error: some client message triggered a protocol
              violation (for instance a QUERY message is sent before a STARTUP
              one has been sent)
    0x0100    Bad credentials: CREDENTIALS request failed because Cassandra
              did not accept the provided credentials.

    0x1000    Unavailable exception. The rest of the ERROR message body will be
                <cl><required><alive>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <required> is an [int] representing the number of node that
                           should be alive to respect <cl>
                <alive> is an [int] representing the number of replica that
                        were known to be alive when the request has been
                        processed (since an unavailable exception has been
                        triggered, there will be <alive> < <required>)
    0x1001    Overloaded: the request cannot be processed because the
              coordinator node is overloaded
    0x1002    Is_bootstrapping: the request was a read request but the
              coordinator node is bootstrapping
    0x1003    Truncate_error: error during a truncation error.
    0x1100    Write_timeout: Timeout exception during a write request. The rest
              of the ERROR message body will be
                <cl><received><blockfor><writeType>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <received> is an [int] representing the number of nodes having
                           acknowledged the request.
                <blockfor> is an [int] representing the number of replica whose
                           acknowledgement is required to achieve <cl>.
                <writeType> is a [string] that describe the type of the write
                            that timeouted. The value of that string can be one
                            of:
                             - "SIMPLE": the write was a non-batched
                               non-counter write.
                             - "BATCH": the write was a (logged) batch write.
                               If this type is received, it means the batch log
                               has been successfully written (otherwise a
                               "BATCH_LOG" type would have been send instead).
                             - "UNLOGGED_BATCH": the write was an unlogged
                               batch. Not batch log write has been attempted.
                             - "COUNTER": the write was a counter write
                               (batched or not).
                             - "BATCH_LOG": the timeout occured during the
                               write to the batch log when a (logged) batch
                               write was requested.
    0x1200    Read_timeout: Timeout exception during a read request. The rest
              of the ERROR message body will be
                <cl><received><blockfor><data_present>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <received> is an [int] representing the number of nodes having
                           answered the request.
                <blockfor> is an [int] representing the number of replicas whose
                           response is required to achieve <cl>. Please note that it
                           is possible to have <received> >= <blockfor> if
                           <data_present> is false. And also in the (unlikely)
                           case were <cl> is achieved but the coordinator node
                           timeout while waiting for read-repair
                           acknowledgement.
                <data_present> is a single byte. If its value is 0, it means
                               the replica that was asked for data has not
                               responded. Otherwise, the value is != 0.

    0x2000    Syntax_error: The submitted query has a syntax error.
    0x2100    Unauthorized: The logged user doesn't have the right to perform
              the query.
    0x2200    Invalid: The query is syntactically correct but invalid.
    0x2300    Config_error: The query is invalid because of some configuration issue
    0x2400    Already_exists: The query attempted to create a keyspace or a
              table that was already existing. The rest of the ERROR message
              body will be <ks><table> where:
                <ks> is a [string] representing either the keyspace that
                     already exists, or the keyspace in which the table that
                     already exists is.
                <table> is a [string] representing the name of the table that
                        already exists. If the query was attempting to create a
                        keyspace, <table> will be present but will be the empty
                        string.
    0x2500    Unprepared: Can be thrown while a prepared statement tries to be
              executed if the provide prepared statement ID is not known by
              this host. The rest of the ERROR message body will be [short
              bytes] representing the unknown ID.
