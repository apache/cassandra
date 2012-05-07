
                             CQL BINARY PROTOCOL v1
                                    (draft)

Table of Contents

  1. Overview
  2. Frame header
    2.1. version
    2.2. flags
    2.3. opcode
    2.4. length
  3. Notations
  4. Messages
    4.1. Requests
      4.1.1. STARTUP
      4.1.2. CREDENTIALS
      4.1.3. OPTIONS
      4.1.4. QUERY
      4.1.5. PREPARE
      4.1.6. EXECUTE
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
  5. Compression
  6. Error codes


1. Overview

  The CQL binary protocol is a frame based protocol. Frames are defined as:

      0         8        16        24        32
      +---------+---------+---------+---------+
      | version |  flags  |      opcode       |
      +---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
      |                                       |
      .            ...  body ...              .
      .                                       .
      .                                       .
      +----------------------------------------

  Each frame contains a fixed size header (8 bytes) followed by a variable size
  body.  The header is described in Section 2. The content of the body depends
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

  Flags applying to this frame. Currently only one bit (the lower-most one, the
  one masked by 0x01) has a meaning and indicates whether the frame body is
  compressed. The actual compression to use should have been set up beforehand
  through the Startup message (which thus cannot be compressed; Section 4.1.1).
  The rest of the flags is kept for future use.


2.3. opcode

  A 2 byte integer that distinguish the actual message:
    0x0000    ERROR
    0x0001    STARTUP
    0x0002    READY
    0x0003    AUTHENTICATE
    0x0004    CREDENTIALS
    0x0005    OPTIONS
    0x0006    SUPPORTED
    0x0007    QUERY
    0x0008    RESULT
    0x0009    PREPARE
    0x000A    EXECUTE

  Messages are described in Section 4.


2.4. length

  A 4 byte integer representing the length of the body of the frame (note:
  currently a frame is limited to 256MB in length).


3. Notations

  To describe the layout of the frame body for the messages in Section 4, we
  define the following:

    [int]          A 4 bytes integer
    [short]        A 2 bytes unsigned integer
    [string]       A [short] n, followed by n bytes representing an UTF-8
                   string.
    [long string]  An [int] n, followed by n bytes representing an UTF-8 string.
    [string list]  A [short] n, followed by n [string].
    [value]        An [int] n, followed by n bytes if n >= 0. If n < 0,
                   no byte should follow and the value represented is `null`.

    [option]       A pair of <id><value> where <id> is a [short] representing
                   the option id and <value> depends on that option (and can be
                   of size 0). The supported id (and the respecting value) will
                   be described when this is used.
    [option list]  A [short] n, followed by n [option].


4. Messages

4.1. Requests

  Note that outside of their normal responses (described below), all requests
  can get an ERROR message (Section 4.2.1) as response.

4.1.1. STARTUP

  Initialize the connection. The server will respond by either a READY message
  (in which case the connection is ready for queries) or an AUTHENTICATE message
  (in which case credentials will need to be provided using CREDENTIALS).

  This must be the first message of the connection, except for OPTIONS that can
  be sent before to find out the option supported by the server. Once the
  connection has been initialized, a client should not send any more STARTUP
  message.

  The body is defined as:
    <version><options>
  where:
    - <version> is a [string] representing the version of the CQL version to use.
      Currently the only version supported is 3.0.0. Note that this is different
      from the protocol version.
    - <options> is an [option list]. Valid option ids are:
        0x0001    Compression: the value is a [string] representing the
                  algorithm to use (See section 5).


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
  string].

  The server will respond to a QUERY message with a RESULT message, the content
  of which depends on the query.


4.1.5. PREPARE

  Prepare a query for later execution (through EXECUTE). The body consists of
  the CQL query to prepare as a [long string].

  The server will respond with a RESULT message with a `prepared` kind (0x00003,
  see Section 4.2.5).


4.1.6. EXECUTE

  Executes a prepared query. The body of the message must be:
    <id><n><value_1>....<value_n>
  where:
    - <id> is the prepared query ID. It's an [int] returned as a response to a
      PREPARE message.
    - <n> is a [short] indicating the number of following values.
    - <value_1>...<value_n> are the [value] to use for bound variables in the
      prepared query.

  The response from the server will be a RESULT message.


4.2. Responses

  This section describes the content of the frame body for the different
  responses. Please note that to make room for future evolution, clients should
  support extra informations (that they should simply discard) to the one
  described in this document at the end of the frame body.

4.2.1. ERROR

  Indicates an error processing a request. The body of the message will be an
  error code ([int]) followed by a [string] error message. The error codes are
  defined in Section 6.


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

  The body of a SUPPORTED message is a [string list] indicating which CQL
  version the server support, followed by a second [string list] indicating
  which compression algorithm is supported, if any (at the time of this writing,
  only snappy compression is available if the library is in the classpath).


4.2.5. RESULT

  The result to a query (QUERY, PREPARE or EXECUTE messages).

  The first element of the body of a RESULT message is an [int] representing the
  `kind` of result. The rest of the body depends on the kind. The kind can be
  one of:
    0x0001    Void: for results carrying no information.
    0x0002    Rows: for results to select queries, returning a set of rows.
    0x0003    Set_keyspace: the result to a `use` query.
    0x0004    Prepared: result to a PREPARE message

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
    - <rows_count> is an [int] representing the number of rows present in this
      result. Those rows are serialized in the <rows_content> part.
    - <rows_content> is composed of <row_1>...<row_m> where m is <rows_count>.
      Each <row_i> is composed of <value_1>...<value_n> where n is
      <columns_count> and where <value_j> is a [value] representing the value
      returned for the jth column of the ith row. In other words, <rows_content>
      is composed of (<rows_count> * <columns_count>) [value].


4.2.5.3. Set_keyspace

  The result to a `use` query. The body (after the kind [int]) is a single
  [string] indicating the name of the keyspace that has been set.


4.2.5.4. Prepared

  The result to a PREPARE message. The rest of the body of a Prepared result is:
    <id><metadata>
  where:
    - <id> is an [int] representing the prepared query ID.
    - <metadata> is defined exactly as for a Rows RESULT (See section 4.2.5.2).


5. Compression

  Frame compression is supported by the protocol, but then only the frame body
  is compressed (the frame header should never be compressed).

  Before being used, client and server must agree on a compression algorithm to
  use, which is done in the STARTUP message. As a consequence, a STARTUP message
  must never be compressed.  However, once the STARTUP frame has been received
  by the server can be compressed (including the response to the STARTUP
  request). Frame do not have to compressed however, even if compression has
  been agreed upon (a server may only compress frame above a certain size at its
  discretion). A frame body should be compressed if and only if the compressed
  flag (see Section 2.2) is set.


6. Error codes

  The currently supported errors are:
    0x0000    Server error
    0x0001    Protocol error
    0x0002    Authentication error
    0x0100    Unavailable exception
    0x0101    Timeout exception
    0x0102    Schema disagreement exception
    0x0200    Request exception
