
                             CQL BINARY PROTOCOL v4


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
      4.1.2. AUTH_RESPONSE
      4.1.3. OPTIONS
      4.1.4. QUERY
      4.1.5. PREPARE
      4.1.6. EXECUTE
      4.1.7. BATCH
      4.1.8. REGISTER
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
      4.2.7. AUTH_CHALLENGE
      4.2.8. AUTH_SUCCESS
  5. Compression
  6. Data Type Serialization Formats
  7. User Defined Type Serialization
  8. Result paging
  9. Error codes
  10. Changes from v3


1. Overview

  The CQL binary protocol is a frame based protocol. Frames are defined as:

      0         8        16        24        32         40
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
      |                                       |
      .            ...  body ...              .
      .                                       .
      .                                       .
      +----------------------------------------

  The protocol is big-endian (network byte order).

  Each frame contains a fixed size header (9 bytes) followed by a variable size
  body. The header is described in Section 2. The content of the body depends
  on the header opcode value (the body can in particular be empty for some
  opcode values). The list of allowed opcode is defined Section 2.3 and the
  details of each corresponding message is described Section 4.

  The protocol distinguishes 2 types of frames: requests and responses. Requests
  are those frame sent by the clients to the server, response are the ones sent
  by the server. Note however that the protocol supports server pushes (events)
  so responses does not necessarily come right after a client request.

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
  The rest of that byte is the protocol version (4 for the protocol defined in
  this document). In other words, for this version of the protocol, version will
  have one of:
    0x04    Request frame for this protocol version
    0x84    Response frame for this protocol version

  Please note that the while every message ship with the version, only one version
  of messages is accepted on a given connection. In other words, the first message
  exchanged (STARTUP) sets the version for the connection for the lifetime of this
  connection.

  This document describe the version 3 of the protocol. For the changes made since
  version 3, see Section 10.


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

  A frame has a stream id (a [short] value). When sending request messages, this
  stream id must be set by the client to a non-negative value (negative stream id
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
  respond to. As there can only be 32768 different simultaneous streams, it is up
  to the client to reuse stream id.

  Note that clients are free to use the protocol synchronously (i.e. wait for
  the response to REQ_N before sending REQ_N+1). In that case, the stream id
  can be safely set to 0. Clients should also feel free to use only a subset of
  the 32768 maximum possible stream ids if it is simpler for those
  implementation.

2.4. opcode

  An integer byte that distinguish the actual message:
    0x00    ERROR
    0x01    STARTUP
    0x02    READY
    0x03    AUTHENTICATE
    0x05    OPTIONS
    0x06    SUPPORTED
    0x07    QUERY
    0x08    RESULT
    0x09    PREPARE
    0x0A    EXECUTE
    0x0B    REGISTER
    0x0C    EVENT
    0x0D    BATCH
    0x0E    AUTH_CHALLENGE
    0x0F    AUTH_RESPONSE
    0x10    AUTH_SUCCESS

  Messages are described in Section 4.

  (Note that there is no 0x04 message in this version of the protocol)


2.5. length

  A 4 byte integer representing the length of the body of the frame (note:
  currently a frame is limited to 256MB in length).


3. Notations

  To describe the layout of the frame body for the messages in Section 4, we
  define the following:

    [int]          A 4 bytes integer
    [long]         A 8 bytes integer
    [short]        A 2 bytes unsigned integer
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
                     0x0008    SERIAL
                     0x0009    LOCAL_SERIAL
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
  (in which case credentials will need to be provided using AUTH_RESPONSE).

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


4.1.2. AUTH_RESPONSE

  Answers a server authentication challenge.

  Authentication in the protocol is SASL based. The server sends authentication
  challenges (a bytes token) to which the client answer with this message. Those
  exchanges continue until the server accepts the authentication by sending a
  AUTH_SUCCESS message after a client AUTH_RESPONSE. It is however that client that
  initiate the exchange by sending an initial AUTH_RESPONSE in response to a
  server AUTHENTICATE request.

  The body of this message is a single [bytes] token. The details of what this
  token contains (and when it can be null/empty, if ever) depends on the actual
  authenticator used.

  The response to a AUTH_RESPONSE is either a follow-up AUTH_CHALLENGE message,
  an AUTH_SUCCESS message or an ERROR message.


4.1.3. OPTIONS

  Asks the server to return what STARTUP options are supported. The body of an
  OPTIONS message should be empty and the server will respond with a SUPPORTED
  message.


4.1.4. QUERY

  Performs a CQL query. The body of the message must be:
    <query><query_parameters>
  where <query> is a [long string] representing the query and
  <query_parameters> must be
    <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
  where:
    - <consistency> is the [consistency] level for the operation.
    - <flags> is a [byte] whose bits define the options for this query and
      in particular influence what the remainder of the message contains.
      A flag is set if the bit corresponding to its `mask` is set. Supported
      flags are, given there mask:
        0x01: Values. In that case, a [short] <n> followed by <n> [bytes]
              values are provided. Those value are used for bound variables in
              the query. Optionally, if the 0x40 flag is present, each value
              will be preceded by a [string] name, representing the name of
              the marker the value must be binded to. This is optional, and
              if not present, values will be binded by position.
        0x02: Skip_metadata. If present, the Result Set returned as a response
              to that query (if any) will have the NO_METADATA flag (see
              Section 4.2.5.2).
        0x04: Page_size. In that case, <result_page_size> is an [int]
              controlling the desired page size of the result (in CQL3 rows).
              See the section on paging (Section 8) for more details.
        0x08: With_paging_state. If present, <paging_state> should be present.
              <paging_state> is a [bytes] value that should have been returned
              in a result set (Section 4.2.5.2). If provided, the query will be
              executed but starting from a given paging state. This also to
              continue paging on a different node from the one it has been
              started (See Section 8 for more details).
        0x10: With serial consistency. If present, <serial_consistency> should be
              present. <serial_consistency> is the [consistency] level for the
              serial phase of conditional updates. That consitency can only be
              either SERIAL or LOCAL_SERIAL and if not present, it defaults to
              SERIAL. This option will be ignored for anything else that a
              conditional update/insert.
        0x20: With default timestamp. If present, <timestamp> should be present.
              <timestamp> is a [long] representing the default timestamp for the query
              in microseconds (negative values are forbidden). If provided, this will
              replace the server side assigned timestamp as default timestamp.
              Note that a timestamp in the query itself will still override
              this timestamp. This is entirely optional.
        0x40: With names for values. This only makes sense if the 0x01 flag is set and
              is ignored otherwise. If present, the values from the 0x01 flag will
              be preceded by a name (see above). Note that this is only useful for
              QUERY requests where named bind markers are used; for EXECUTE statements,
              since the names for the expected values was returned during preparation,
              a client can always provide values in the right order without any names
              and using this flag, while supported, is almost surely inefficient.

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
    <id><query_parameters>
  where <id> is the prepared query ID. It's the [short bytes] returned as a
  response to a PREPARE message. As for <query_parameters>, it has the exact
  same definition than in QUERY (see Section 4.1.4).

  The response from the server will be a RESULT message.


4.1.7. BATCH

  Allows executing a list of queries (prepared or not) as a batch (note that
  only DML statements are accepted in a batch). The body of the message must
  be:
    <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
  where:
    - <type> is a [byte] indicating the type of batch to use:
        - If <type> == 0, the batch will be "logged". This is equivalent to a
          normal CQL3 batch statement.
        - If <type> == 1, the batch will be "unlogged".
        - If <type> == 2, the batch will be a "counter" batch (and non-counter
          statements will be rejected).
    - <flags> is a [byte] whose bits define the options for this query and
      in particular influence the remainder of the message contains. It is similar
      to the <flags> from QUERY and EXECUTE methods, except that the 4 rightmost
      bits must always be 0 as their corresponding option do not make sense for
      Batch. A flag is set if the bit corresponding to its `mask` is set. Supported
      flags are, given there mask:
        0x10: With serial consistency. If present, <serial_consistency> should be
              present. <serial_consistency> is the [consistency] level for the
              serial phase of conditional updates. That consitency can only be
              either SERIAL or LOCAL_SERIAL and if not present, it defaults to
              SERIAL. This option will be ignored for anything else that a
              conditional update/insert.
        0x20: With default timestamp. If present, <timestamp> should be present.
              <timestamp> is a [long] representing the default timestamp for the query
              in microseconds. If provided, this will replace the server side assigned
              timestamp as default timestamp. Note that a timestamp in the query itself
              will still override this timestamp. This is entirely optional.
        0x40: With names for values. If set, then all values for all <query_i> must be
              preceded by a [string] <name_i> that have the same meaning as in QUERY
              requests.
    - <n> is a [short] indicating the number of following queries.
    - <query_1>...<query_n> are the queries to execute. A <query_i> must be of the
      form:
        <kind><string_or_id><n>[<name_1>]<value_1>...[<name_n>]<value_n>
      where:
       - <kind> is a [byte] indicating whether the following query is a prepared
         one or not. <kind> value must be either 0 or 1.
       - <string_or_id> depends on the value of <kind>. If <kind> == 0, it should be
         a [long string] query string (as in QUERY, the query string might contain
         bind markers). Otherwise (that is, if <kind> == 1), it should be a
         [short bytes] representing a prepared query ID.
       - <n> is a [short] indicating the number (possibly 0) of following values.
       - <name_i> is the optional name of the following <value_i>. It must be present
         if and only if the 0x40 flag is provided for the batch.
       - <value_i> is the [bytes] to use for bound variable i (of bound variable <name_i>
         if the 0x40 flag is used).
    - <consistency> is the [consistency] level for the operation.
    - <serial_consistency> is only present if the 0x10 flag is set. In that case,
      <serial_consistency> is the [consistency] level for the serial phase of
      conditional updates. That consitency can only be either SERIAL or
      LOCAL_SERIAL and if not present will defaults to SERIAL. This option will
      be ignored for anything else that a conditional update/insert.

  The server will respond with a RESULT message.


4.1.8. REGISTER

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
  Section 9, along with their additional content if any.


4.2.2. READY

  Indicates that the server is ready to process queries. This message will be
  sent by the server either after a STARTUP message if no authentication is
  required, or after a successful CREDENTIALS message.

  The body of a READY message is empty.


4.2.3. AUTHENTICATE

  Indicates that the server require authentication, and which authentication
  mechanism to use.

  The authentication is SASL based and thus consists on a number of server
  challenges (AUTH_CHALLENGE, Section 4.2.7) followed by client responses
  (AUTH_RESPONSE, Section 4.1.2). The Initial exchange is however boostrapped
  by an initial client response. The details of that exchange (including how
  much challenge-response pair are required) are specific to the authenticator
  in use. The exchange ends when the server sends an AUTH_SUCCESS message or
  an ERROR message.

  This message will be sent following a STARTUP message if authentication is
  required and must be answered by a AUTH_RESPONSE message from the client.

  The body consists of a single [string] indicating the full class name of the
  IAuthenticator in use.


4.2.4. SUPPORTED

  Indicates which startup options are supported by the server. This message
  comes as a response to an OPTIONS message.

  The body of a SUPPORTED message is a [string multimap]. This multimap gives
  for each of the supported STARTUP options, the list of supported values.


4.2.5. RESULT

  The result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).

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
        <flags><columns_count>[<paging_state>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
      where:
        - <flags> is an [int]. The bits of <flags> provides information on the
          formatting of the remaining informations. A flag is set if the bit
          corresponding to its `mask` is set. Supported flags are, given there
          mask:
            0x0001    Global_tables_spec: if set, only one table spec (keyspace
                      and table name) is provided as <global_table_spec>. If not
                      set, <global_table_spec> is not present.
            0x0002    Has_more_pages: indicates whether this is not the last
                      page of results and more should be retrieve. If set, the
                      <paging_state> will be present. The <paging_state> is a
                      [bytes] value that should be used in QUERY/EXECUTE to
                      continue paging and retrieve the remained of the result for
                      this query (See Section 8 for more details).
            0x0004    No_metadata: if set, the <metadata> is only composed of
                      these <flags>, the <column_count> and optionally the
                      <paging_state> (depending on the Has_more_pages flage) but
                      no other information (so no <global_table_spec> nor <col_spec_i>).
                      This will only ever be the case if this was requested
                      during the query (see QUERY and RESULT messages).
        - <columns_count> is an [int] representing the number of columns selected
          by the query this result is of. It defines the number of <col_spec_i>
          elements in and the number of element for each row in <rows_content>.
        - <global_table_spec> is present if the Global_tables_spec is set in
          <flags>. If present, it is composed of two [string] representing the
          (unique) keyspace name and table name the columns return are of.
        - <col_spec_i> specifies the columns returned in the query. There is
          <column_count> such column specifications that are composed of:
            (<ksname><tablename>)?<name><type>
          The initial <ksname> and <tablename> are two [string] are only present
          if the Global_tables_spec flag is not set. The <column_name> is a
          [string] and <type> is an [option] that correspond to the description
          (what this description is depends a bit on the context: in results to
          selects, this will be either the user chosen alias or the selection used
          (often a colum name, but it can be a function call too). In results to
          a PREPARE, this will be either the name of the bind variable corresponding
          or the column name for the variable if it is "anonymous") and type of
          the corresponding result. The option for <type> is either a native
          type (see below), in which case the option has no value, or a
          'custom' type, in which case the value is a [string] representing
          the full qualified class name of the type represented. Valid option
          ids are:
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
            0x000B    Timestamp
            0x000C    Uuid
            0x000D    Varchar
            0x000E    Varint
            0x000F    Timeuuid
            0x0010    Inet
            0x0011    Date
            0x0012    Time
            0x0020    List: the value is an [option], representing the type
                            of the elements of the list.
            0x0021    Map: the value is two [option], representing the types of the
                           keys and values of the map
            0x0022    Set: the value is an [option], representing the type
                            of the elements of the set
            0x0030    UDT: the value is <ks><udt_name><n><name_1><type_1>...<name_n><type_n>
                           where:
                              - <ks> is a [string] representing the keyspace name this
                                UDT is part of.
                              - <udt_name> is a [string] representing the UDT name.
                              - <n> is a [short] reprensenting the number of fields of
                                the UDT, and thus the number of <name_i><type_i> pair
                                following
                              - <name_i> is a [string] representing the name of the
                                i_th field of the UDT.
                              - <type_i> is an [option] representing the type of the
                                i_th field of the UDT.
            0x0031    Tuple: the value is <n><type_1>...<type_n> where <n> is a [short]
                             representing the number of value in the type, and <type_i>
                             are [option] representing the type of the i_th component
                             of the tuple

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

  The result to a PREPARE message. The body of a Prepared result is:
    <id><metadata><result_metadata>
  where:
    - <id> is [short bytes] representing the prepared query ID.
    - <metadata> is composed of:
        <flags><columns_count><pk_count>[<pk_index_1>...<pk_index_n>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
      where:
        - <flags> is an [int]. The bits of <flags> provides information on the
          formatting of the remaining informations. A flag is set if the bit
          corresponding to its `mask` is set. Supported masks and their flags
          are:
            0x0001    Global_tables_spec: if set, only one table spec (keyspace
                      and table name) is provided as <global_table_spec>. If not
                      set, <global_table_spec> is not present.
        - <columns_count> is an [int] representing the number of bind markers
          in the prepared statement.  It defines the number of <col_spec_i>
          elements.
        - <pk_count> is an [int] representing the number of <pk_index_i>
          elements to follow. If this value is zero, at least one of the
          partition key columns in the table that the statement acts on
          did not have a corresponding bind marker (or the bind marker
          was wrapped in a function call).
        - <pk_index_i> is a short that represents the index of the bind marker
          that corresponds to the partition key column in position i.
          For example, a <pk_index> sequence of [2, 0, 1] indicates that the
          table has three partition key columns; the full partition key
          can be constructed by creating a composite of the values for
          the bind markers at index 2, at index 0, and at index 1.
          This allows implementations with token-aware routing to correctly
          construct the partition key without needing to inspect table
          metadata.
        - <global_table_spec> is present if the Global_tables_spec is set in
          <flags>. If present, it is composed of two [string]s. The first
          [string] is the name of the keyspace that the statement acts on.
          The second [string] is the name of the table that the columns
          represented by the bind markers belong to.
        - <col_spec_i> specifies the bind markers in the prepared statement.
          There are <column_count> such column specifications, each with the
          following format:
            (<ksname><tablename>)?<name><type>
          The initial <ksname> and <tablename> are two [string] that are only
          present if the Global_tables_spec flag is not set. The <name> field
          is a [string] that holds the name of the bind marker (if named),
          or the name of the column, field, or expression that the bind marker
          corresponds to (if the bind marker is "anonymous").  The <type>
          field is an [option] that represents the expected type of values for
          the bind marker.  See the Rows documentation (section 4.2.5.2) for
          full details on the <type> field.

    - <result_metadata> is defined exactly the same as <metadata> in the Rows
      documentation (section 4.2.5.2).  This describes the metadata for the
      result set that will be returned when this prepared statement is executed.
      Note that <result_metadata> may be empty (have the No_metadata flag and
      0 columns, See section 4.2.5.2) and will be for any query that is not a
      Select. In fact, there is never a guarantee that this will non-empty, so
      implementations should protect themselves accordingly. This result metadata
      is an optimization that allows implementations to later execute the
      prepared statement without requesting the metadata (see the Skip_metadata
      flag in EXECUTE).  Clients can safely discard this metadata if they do not
      want to take advantage of that optimization.

  Note that prepared query ID return is global to the node on which the query
  has been prepared. It can be used on any connection to that node and this
  until the node is restarted (after which the query must be reprepared).

4.2.5.5. Schema_change

  The result to a schema altering query (creation/update/drop of a
  keyspace/table/index). The body (after the kind [int]) is the same
  as the body for a "SCHEMA_CHANGE" event, so 3 strings:
    <change_type><target><options>
  Please refer to the section 4.2.6 below for the meaning of those fields.

  Note that queries to create and drop an index are considered as change
  updating the table the index is on.


4.2.6. EVENT

  And event pushed by the server. A client will only receive events for the
  type it has REGISTER to. The body of an EVENT message will start by a
  [string] representing the event type. The rest of the message depends on the
  event type. The valid event types are:
    - "TOPOLOGY_CHANGE": events related to change in the cluster topology.
      Currently, events are sent when new nodes are added to the cluster, and
      when nodes are removed. The body of the message (after the event type)
      consists of a [string] and an [inet], corresponding respectively to the
      type of change ("NEW_NODE" or "REMOVED_NODE") followed by the address of
      the new/removed node.
    - "STATUS_CHANGE": events related to change of node status. Currently,
      up/down events are sent. The body of the message (after the event type)
      consists of a [string] and an [inet], corresponding respectively to the
      type of status change ("UP" or "DOWN") followed by the address of the
      concerned node.
    - "SCHEMA_CHANGE": events related to schema change. After the event type,
      the rest of the message will be <change_type><target><options> where:
        - <change_type> is a [string] representing the type of changed involved.
          It will be one of "CREATED", "UPDATED" or "DROPPED".
        - <target> is a [string] that can be one of "KEYSPACE", "TABLE", "TYPE",
          "FUNCTION" or "AGGREGATE" and describes what has been modified
          ("TYPE" stands for modifications related to user types, "FUNCTION"
          for modifications related to user defined functions, "AGGREGATE"
          for modifications related to user defined aggregates).
        - <options> depends on the preceding <target>:
          - If <target> is "KEYSPACE", then <options> will be a single [string]
            representing the keyspace changed.
          - If <target> is "TABLE" or "TYPE", then
            <options> will be 2 [string]: the first one will be the keyspace
            containing the affected object, and the second one will be the name
            of said affected object (either the table, user type, function, or
            aggregate name).
          - If <target> is "FUNCTION" or "AGGREGATE", multiple arguments follow:
            - [string] keyspace containing the user defined function / aggregate
            - [string] the function/aggregate name
            - [string list] one string for each argument type (as CQL type)

  All EVENT messages have a streamId of -1 (Section 2.3).

  Please note that "NEW_NODE" and "UP" events are sent based on internal Gossip
  communication and as such may be sent a short delay before the binary
  protocol server on the newly up node is fully started. Clients are thus
  advise to wait a short time before trying to connect to the node (1 seconds
  should be enough), otherwise they may experience a connection refusal at
  first.

4.2.7. AUTH_CHALLENGE

  A server authentication challenge (see AUTH_RESPONSE (Section 4.1.2) for more
  details).

  The body of this message is a single [bytes] token. The details of what this
  token contains (and when it can be null/empty, if ever) depends on the actual
  authenticator used.

  Clients are expected to answer the server challenge by an AUTH_RESPONSE
  message.

4.2.7. AUTH_SUCCESS

  Indicate the success of the authentication phase. See Section 4.2.3 for more
  details.

  The body of this message is a single [bytes] token holding final information
  from the server that the client may require to finish the authentication
  process. What that token contains and whether it can be null depends on the
  actual authenticator used.


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

  As of this version 2 of the protocol, the following compressions are available:
    - lz4 (https://code.google.com/p/lz4/). In that, note that the 4 first bytes
      of the body will be the uncompressed length (followed by the compressed
      bytes).
    - snappy (https://code.google.com/p/snappy/). This compression might not be
      available as it depends on a native lib (server-side) that might not be
      avaivable on some installation.


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

  A [int] n indicating the number of elements in the list, followed by n
  elements.  Each element is [bytes] representing the serialized value.

6.11 map

  A [int] n indicating the number of key/value pairs in the map, followed by
  n entries.  Each entry is composed of two [bytes] representing the key
  and value.

6.12 set

  A [int] n indicating the number of elements in the set, followed by n
  elements.  Each element is [bytes] representing the serialized value.

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
    129 |   0x0081
     -1 |     0xFF
   -128 |     0x80
   -129 |   0xFF7F

  Note that positive numbers must use a most-significant byte with a value
  less than 0x80, because a most-significant bit of 1 indicates a negative
  value.  Implementors should pad positive values that have a MSB >= 0x80
  with a leading 0x00 byte.

6.18 timeuuid

  A 16 byte sequence representing a version 1 UUID as defined by RFC 4122.

6.19 tuple

  A sequence of [bytes] values representing the items in a tuple.  The encoding
  of each element depends on the data type for that position in the tuple.
  Null values may be represented by using length -1 for the [bytes]
  representation of an element.

  Within a tuple, all data types should use the v3 protocol serialization format.


7. User Defined Types

  This section describes the serialization format for User defined types (UDT),
  as described in section 4.2.5.2.

  A UDT value is composed of successive [bytes] values, one for each field of the UDT
  value (in the order defined by the type). A UDT value will generally have one value
  for each field of the type it represents, but it is allowed to have less values than
  the type has fields.

  Within a user-defined type value, all data types should use the v3 protocol
  serialization format.


8. Result paging

  The protocol allows for paging the result of queries. For that, the QUERY and
  EXECUTE messages have a <result_page_size> value that indicate the desired
  page size in CQL3 rows.

  If a positive value is provided for <result_page_size>, the result set of the
  RESULT message returned for the query will contain at most the
  <result_page_size> first rows of the query result. If that first page of result
  contains the full result set for the query, the RESULT message (of kind `Rows`)
  will have the Has_more_pages flag *not* set. However, if some results are not
  part of the first response, the Has_more_pages flag will be set and the result
  will contain a <paging_state> value. In that case, the <paging_state> value
  should be used in a QUERY or EXECUTE message (that has the *same* query than
  the original one or the behavior is undefined) to retrieve the next page of
  results.

  Only CQL3 queries that return a result set (RESULT message with a Rows `kind`)
  support paging. For other type of queries, the <result_page_size> value is
  ignored.

  Note to client implementors:
  - While <result_page_size> can be as low as 1, it will likely be detrimental
    to performance to pick a value too low. A value below 100 is probably too
    low for most use cases.
  - Clients should not rely on the actual size of the result set returned to
    decide if there is more result to fetch or not. Instead, they should always
    check the Has_more_pages flag (unless they did not enabled paging for the query
    obviously). Clients should also not assert that no result will have more than
    <result_page_size> results. While the current implementation always respect
    the exact value of <result_page_size>, we reserve ourselves the right to return
    slightly smaller or bigger pages in the future for performance reasons.


9. Error codes

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
                <blockfor> is the number of replica whose acknowledgement is
                           required to achieve <cl>.
                <writeType> is a [string] that describe the type of the write
                            that timeouted. The value of that string can be one
                            of:
                             - "SIMPLE": the write was a non-batched
                               non-counter write.
                             - "BATCH": the write was a (logged) batch write.
                               If this type is received, it means the batch log
                               has been successfully written (otherwise a
                               "BATCH_LOG" type would have been sent instead).
                             - "UNLOGGED_BATCH": the write was an unlogged
                               batch. No batch log write has been attempted.
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
                <blockfor> is the number of replica whose response is
                           required to achieve <cl>. Please note that it is
                           possible to have <received> >= <blockfor> if
                           <data_present> is false. And also in the (unlikely)
                           case were <cl> is achieved but the coordinator node
                           timeout while waiting for read-repair
                           acknowledgement.
                <data_present> is a single byte. If its value is 0, it means
                               the replica that was asked for data has not
                               responded. Otherwise, the value is != 0.
    0x1300    Read_failure: A non-timeout exception during a read request. The rest
              of the ERROR message body will be
                <cl><received><blockfor><numfailures><data_present>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <received> is an [int] representing the number of nodes having
                           answered the request.
                <blockfor> is the number of replicas whose response is
                           required to achieve <cl>.
                <numfailures> is an [int] representing the number of nodes that
                              experience a failure while executing the request.
                <data_present> is a single byte. If its value is 0, it means
                               the replica that was asked for data had not
                               responded. Otherwise, the value is != 0.
    0x1400    Function_failure: A (user defined) function failed during execution.
              The rest of the ERROR message body will be
                <keyspace><function><arg_types>
              where:
                <keyspace> is the keyspace [string] of the failed function
                <function> is the name [string] of the failed function
                <arg_types> [string list] one string for each argument type (as CQL type) of the failed function
    0x1500    Write_failure: A non-timeout exception during a write request. The rest
              of the ERROR message body will be
                <cl><received><blockfor><numfailures><write_type>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <received> is an [int] representing the number of nodes having
                           answered the request.
                <blockfor> is the number of replicas whose response is
                           required to achieve <cl>.
                <numfailures> is an [int] representing the number of nodes that
                              experience a failure while executing the request.
                <writeType> is a [string] that describe the type of the write
                            that failed. The value of that string can be one
                            of:
                             - "SIMPLE": the write was a non-batched
                               non-counter write.
                             - "BATCH": the write was a (logged) batch write.
                               If this type is received, it means the batch log
                               has been successfully written (otherwise a
                               "BATCH_LOG" type would have been sent instead).
                             - "UNLOGGED_BATCH": the write was an unlogged
                               batch. No batch log write has been attempted.
                             - "COUNTER": the write was a counter write
                               (batched or not).
                             - "BATCH_LOG": the failure occured during the
                               write to the batch log when a (logged) batch
                               write was requested.

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

10. Changes from v3

  * The format of "SCHEMA_CHANGE" events (Section 4.2.6) (and implicitly "Schema_change" results (Section 4.2.5.5))
    has been modified, and now includes changes related to user defined functions and user defined aggregates.
  * Read_failure error code was added.
  * Function_failure error code was added.
