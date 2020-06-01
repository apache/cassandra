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

Audit Logging
-------------

Audit Logging is a new feature in Apache Cassandra 4.0 (`CASSANDRA-12151
<https://issues.apache.org/jira/browse/CASSANDRA-12151>`_). All database activity is logged to a directory in the local filesystem and the audit log files are rolled periodically. All database operations are monitored and recorded.  Audit logs are stored in local directory files instead of the database itself as it provides several benefits, some of which are:

- No additional database capacity is needed to store audit logs
- No query tool is required while storing the audit logs in the database would require a query tool
- Latency of database operations is not affected; no performance impact
- It is easier to implement file based logging than database based logging

What does Audit Logging Log?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Audit logging logs:

1. All authentication which includes successful and failed login attempts
2. All database command requests to CQL. Both failed and successful CQL is logged

More specifically an audit log entry could be one of two types:

a) CQL Audit Log Entry Type or
b) Common Audit Log Entry Type

Each of these types comprises of several database operations. The CQL Audit Log Entry Type could be one of the following; the category of the CQL audit log entry type is listed in parentheses.

1. SELECT(QUERY),
2. UPDATE(DML),
3. DELETE(DML),
4. TRUNCATE(DDL),
5. CREATE_KEYSPACE(DDL),
6. ALTER_KEYSPACE(DDL),
7. DROP_KEYSPACE(DDL),
8. CREATE_TABLE(DDL),
9. DROP_TABLE(DDL),
10. PREPARE_STATEMENT(PREPARE),
11. DROP_TRIGGER(DDL),
12. LIST_USERS(DCL),
13. CREATE_INDEX(DDL),
14. DROP_INDEX(DDL),
15. GRANT(DCL),
16. REVOKE(DCL),
17. CREATE_TYPE(DDL),
18. DROP_AGGREGATE(DDL),
19. ALTER_VIEW(DDL),
20. CREATE_VIEW(DDL),
21. DROP_ROLE(DCL),
22. CREATE_FUNCTION(DDL),
23. ALTER_TABLE(DDL),
24. BATCH(DML),
25. CREATE_AGGREGATE(DDL),
26. DROP_VIEW(DDL),
27. DROP_TYPE(DDL),
28. DROP_FUNCTION(DDL),
29. ALTER_ROLE(DCL),
30. CREATE_TRIGGER(DDL),
31. LIST_ROLES(DCL),
32. LIST_PERMISSIONS(DCL),
33. ALTER_TYPE(DDL),
34. CREATE_ROLE(DCL),
35. USE_KEYSPACE (OTHER).

The Common Audit Log Entry Type could be one of the following; the category of the Common audit log entry type is listed in parentheses.

1. REQUEST_FAILURE(ERROR),
2. LOGIN_ERROR(AUTH),
3. UNAUTHORIZED_ATTEMPT(AUTH),
4. LOGIN_SUCCESS (AUTH).

What Audit Logging does not Log?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Audit logging does not log:

1. Configuration changes made in ``cassandra.yaml``
2. Nodetool Commands

Audit Logging is Flexible and Configurable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Audit logging is flexible and configurable in ``cassandra.yaml`` as follows:

- Keyspaces and tables to be monitored and audited may be specified.
- Users to be included/excluded may be specified. By default all users are audit logged.
- Categories of operations to audit or exclude may be specified.
- The frequency at which to roll the log files may be specified. Default frequency is hourly.

Configuring Audit Logging
^^^^^^^^^^^^^^^^^^^^^^^^^
Audit Logging is configured on each node separately. Audit Logging is configured in ``cassandra.yaml`` in the ``audit_logging_options`` setting.
The settings may be same/different on each node.

Enabling Audit Logging
**********************
Audit logging is enabled by setting the ``enabled``  option to ``true`` in the ``audit_logging_options`` setting.

::

 audit_logging_options:
    enabled: true

Setting the Logger
******************
The audit logger is set with the ``logger`` option.

::

 logger:
 - class_name: BinAuditLogger

Two types of audit loggers are supported: ``FileAuditLogger`` and ``BinAuditLogger``.
``BinAuditLogger`` is the default setting.  The ``BinAuditLogger`` is an efficient way to log events to file in a binary format.

``FileAuditLogger`` is synchronous, file-based audit logger; just uses the standard logging mechanism. ``FileAuditLogger`` logs events to ``audit/audit.log`` file using ``slf4j`` logger.

The ``NoOpAuditLogger`` is a No-Op implementation of the audit logger to be used as a default audit logger when audit logging is disabled.

It is possible to configure your custom logger implementation by injecting a map of property keys and their respective values. Default `IAuditLogger`
implementations shipped with Cassandra do not react on these properties but your custom logger might. They would be present as
a parameter of logger constructor (as `Map<String, String>`). In ``cassandra.yaml`` file, you may configure it like this:

::

 logger:
 - class_name: MyCustomAuditLogger
   parameters:
   - key1: value1
     key2: value2

When it comes to configuring these parameters, you can use respective ``enableAuditLog`` method in ``StorageServiceMBean``.
There are two methods of same name with different signatures. The first one does not accept a map where your parameters would be. This method
is used primarily e.g. from JConsole or similar tooling. JConsole can not accept a map to be sent over JMX so in order to be able to enable it
from there, even without any parameters, use this method. ``BinAuditLogger`` does not need any parameters to run with so invoking this method is fine.
The second one does accept a map with your custom parameters so you can pass them programmatically. ``enableauditlog`` command of ``nodetool`` uses
the first ``enableAuditLog`` method mentioned. Hence, currently, there is not a way how to pass parameters to your custom audit logger from ``nodetool``.

Setting the Audit Logs Directory
********************************
The audit logs directory is set with the ``audit_logs_dir`` option. A new directory is not created automatically and an existing directory must be set. Audit Logs directory can be configured using ``cassandra.logdir.audit`` system property or default is set to ``cassandra.logdir + /audit/``. A user created directory may be set. As an example, create a directory for the audit logs and set its permissions.

::

 sudo mkdir –p  /cassandra/audit/logs/hourly
 sudo chmod -R 777 /cassandra/audit/logs/hourly

Set the directory for the audit logs directory using the ``audit_logs_dir`` option.

::

 audit_logs_dir: "/cassandra/audit/logs/hourly"


Setting Keyspaces to Audit
**************************
Set  the keyspaces to include with the ``included_keyspaces`` option and the keyspaces to exclude with the ``excluded_keyspaces`` option.  By default all keyspaces are included. By default, ``system``, ``system_schema`` and ``system_virtual_schema`` are excluded.

::

 # included_keyspaces:
 # excluded_keyspaces: system, system_schema, system_virtual_schema

Setting Categories to Audit
***************************

The categories of database operations to be included are specified with the ``included_categories``  option as a comma separated list.  By default all supported categories are included. The categories of database operations to be excluded are specified with ``excluded_categories``  option as a comma separated list.  By default no category is excluded.

::

 # included_categories:
 # excluded_categories:

The supported categories for audit log are:

1. QUERY
2. DML
3. DDL
4. DCL
5. OTHER
6. AUTH
7. ERROR
8. PREPARE

Setting Users to Audit
**********************

Users to audit log are set with the ``included_users`` and  ``excluded_users``  options.  The ``included_users`` option specifies a comma separated list of users to include explicitly and by default all users are included. The ``excluded_users`` option specifies a comma separated list of  users to exclude explicitly and by default no user is excluded.

::

    # included_users:
    # excluded_users:

Setting the Roll Frequency
***************************
The ``roll_cycle`` option sets the frequency at which the audit log file is rolled. Supported values are ``MINUTELY``, ``HOURLY``, and ``DAILY``. Default value is ``HOURLY``, which implies that after every hour a new audit log file is created.

::

 roll_cycle: HOURLY

An audit log file could get rolled for other reasons as well such as a log file reaches the configured size threshold.

Setting Archiving Options
*************************

The archiving options are for archiving the rolled audit logs. The ``archive`` command to use is set with the ``archive_command`` option and the ``max_archive_retries`` sets the maximum # of tries of failed archive commands.

::

  # archive_command:
  # max_archive_retries: 10

Default archive command is ``"/path/to/script.sh %path"`` where ``%path`` is replaced with the file being rolled:

Other Settings
***************

The other audit logs settings are as follows.

::

 # block: true
 # max_queue_weight: 268435456 # 256 MiB
 # max_log_size: 17179869184 # 16 GiB

The ``block`` option specifies whether the audit logging should block if the logging falls behind or should drop log records.

The ``max_queue_weight`` option sets the maximum weight of in memory queue for records waiting to be written to the file before blocking or dropping.

The  ``max_log_size`` option sets the maximum size of the rolled files to retain on disk before deleting the oldest.

Using Nodetool to Enable Audit Logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``nodetool  enableauditlog``  command may be used to enable audit logs and it overrides the settings in ``cassandra.yaml``.  The ``nodetool enableauditlog`` command syntax is as follows.

::

        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                [(-u <username> | --username <username>)] enableauditlog
                [--excluded-categories <excluded_categories>]
                [--excluded-keyspaces <excluded_keyspaces>]
                [--excluded-users <excluded_users>]
                [--included-categories <included_categories>]
                [--included-keyspaces <included_keyspaces>]
                [--included-users <included_users>] [--logger <logger>]

OPTIONS
        --excluded-categories <excluded_categories>
            Comma separated list of Audit Log Categories to be excluded for
            audit log. If not set the value from cassandra.yaml will be used

        --excluded-keyspaces <excluded_keyspaces>
            Comma separated list of keyspaces to be excluded for audit log. If
            not set the value from cassandra.yaml will be used

        --excluded-users <excluded_users>
            Comma separated list of users to be excluded for audit log. If not
            set the value from cassandra.yaml will be used

        -h <host>, --host <host>
            Node hostname or ip address

        --included-categories <included_categories>
            Comma separated list of Audit Log Categories to be included for
            audit log. If not set the value from cassandra.yaml will be used

        --included-keyspaces <included_keyspaces>
            Comma separated list of keyspaces to be included for audit log. If
            not set the value from cassandra.yaml will be used

        --included-users <included_users>
            Comma separated list of users to be included for audit log. If not
            set the value from cassandra.yaml will be used

        --logger <logger>
            Logger name to be used for AuditLogging. Default BinAuditLogger. If
            not set the value from cassandra.yaml will be used

        -p <port>, --port <port>
            Remote jmx agent port number

        -pp, --print-port
            Operate in 4.0 mode with hosts disambiguated by port number

        -pw <password>, --password <password>
            Remote jmx agent password

        -pwf <passwordFilePath>, --password-file <passwordFilePath>
            Path to the JMX password file

        -u <username>, --username <username>
            Remote jmx agent username


The ``nodetool disableauditlog`` command disables audit log. The command syntax is as follows.

::

        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                [(-u <username> | --username <username>)] disableauditlog

OPTIONS
        -h <host>, --host <host>
            Node hostname or ip address

        -p <port>, --port <port>
            Remote jmx agent port number

        -pp, --print-port
            Operate in 4.0 mode with hosts disambiguated by port number

        -pw <password>, --password <password>
            Remote jmx agent password

        -pwf <passwordFilePath>, --password-file <passwordFilePath>
            Path to the JMX password file

        -u <username>, --username <username>
            Remote jmx agent username

Viewing the Audit Logs
^^^^^^^^^^^^^^^^^^^^^^
An audit log event comprises of a keyspace that is being audited, the operation that is being logged, the scope and the user. An audit log entry comprises of the following attributes concatenated with a "|".

::

 type (AuditLogEntryType): Type of request
 source (InetAddressAndPort): Source IP Address from which request originated
 user (String): User name
 timestamp (long ): Timestamp of the request
 batch (UUID): Batch of request
 keyspace (String): Keyspace on which request is made
 scope (String): Scope of request such as Table/Function/Aggregate name
 operation (String): Database operation such as CQL command
 options (QueryOptions): CQL Query options
 state (QueryState): State related to a given query

Some of these attributes may not be applicable to a given request and not all of these options must be set.

An Audit Logging Demo
^^^^^^^^^^^^^^^^^^^^^^
To demonstrate audit logging enable and configure audit logs with following settings.

::

 audit_logging_options:
    enabled: true
    logger:
    - class_name: BinAuditLogger
    audit_logs_dir: "/cassandra/audit/logs/hourly"
    # included_keyspaces:
    # excluded_keyspaces: system, system_schema, system_virtual_schema
    # included_categories:
    # excluded_categories:
    # included_users:
    # excluded_users:
    roll_cycle: HOURLY
    # block: true
    # max_queue_weight: 268435456 # 256 MiB
    # max_log_size: 17179869184 # 16 GiB
    ## archive command is "/path/to/script.sh %path" where %path is replaced with the file being rolled:
    # archive_command:
    # max_archive_retries: 10

Create the audit log directory ``/cassandra/audit/logs/hourly`` and set its permissions as discussed earlier. Run some CQL commands such as create a keyspace, create a table and query a table. Any supported CQL commands may be run as discussed in section **What does Audit Logging Log?**.  Change directory (with ``cd`` command) to the audit logs directory.

::

 cd /cassandra/audit/logs/hourly

List the files/directories and some ``.cq4`` files should get listed. These are the audit logs files.

::

 [ec2-user@ip-10-0-2-238 hourly]$ ls -l
 total 28
 -rw-rw-r--. 1 ec2-user ec2-user 83886080 Aug  2 03:01 20190802-02.cq4
 -rw-rw-r--. 1 ec2-user ec2-user 83886080 Aug  2 03:01 20190802-03.cq4
 -rw-rw-r--. 1 ec2-user ec2-user    65536 Aug  2 03:01 directory-listing.cq4t

The ``auditlogviewer`` tool is used to dump audit logs. Run the ``auditlogviewer`` tool. Audit log files directory path is a required argument. The output should be similar to the following output.

::

 [ec2-user@ip-10-0-2-238 hourly]$ auditlogviewer /cassandra/audit/logs/hourly
 WARN  03:12:11,124 Using Pauser.sleepy() as not enough processors, have 2, needs 8+
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/127.0.0.1|port:46264|timestamp:1564711427328|type :USE_KEYSPACE|category:OTHER|ks:auditlogkeyspace|operation:USE AuditLogKeyspace;
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/127.0.0.1|port:46264|timestamp:1564711427329|type :USE_KEYSPACE|category:OTHER|ks:auditlogkeyspace|operation:USE "auditlogkeyspace"
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/127.0.0.1|port:46264|timestamp:1564711446279|type :SELECT|category:QUERY|ks:auditlogkeyspace|scope:t|operation:SELECT * FROM t;
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/127.0.0.1|port:46264|timestamp:1564713878834|type :DROP_TABLE|category:DDL|ks:auditlogkeyspace|scope:t|operation:DROP TABLE IF EXISTS
 AuditLogKeyspace.t;
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/3.91.56.164|port:42382|timestamp:1564714618360|ty
 pe:REQUEST_FAILURE|category:ERROR|operation:CREATE KEYSPACE AuditLogKeyspace
 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};; Cannot add
 existing keyspace "auditlogkeyspace"
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/127.0.0.1|port:46264|timestamp:1564714690968|type :DROP_KEYSPACE|category:DDL|ks:auditlogkeyspace|operation:DROP KEYSPACE AuditLogKeyspace;
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/3.91.56.164|port:42406|timestamp:1564714708329|ty pe:CREATE_KEYSPACE|category:DDL|ks:auditlogkeyspace|operation:CREATE KEYSPACE
 AuditLogKeyspace
 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 Type: AuditLog
 LogMessage:
 user:anonymous|host:10.0.2.238:7000|source:/127.0.0.1|port:46264|timestamp:1564714870678|type :USE_KEYSPACE|category:OTHER|ks:auditlogkeyspace|operation:USE auditlogkeyspace;
 [ec2-user@ip-10-0-2-238 hourly]$


The ``auditlogviewer`` tool usage syntax is as follows.

::

 ./auditlogviewer
 Audit log files directory path is a required argument.
 usage: auditlogviewer <path1> [<path2>...<pathN>] [options]
 --
 View the audit log contents in human readable format
 --
 Options are:
 -f,--follow       Upon reaching the end of the log continue indefinitely
                   waiting for more records
 -h,--help         display this help message
 -r,--roll_cycle   How often to roll the log file was rolled. May be
                   necessary for Chronicle to correctly parse file names. (MINUTELY, HOURLY,
                   DAILY). Default HOURLY.

Diagnostic events for user audit logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Any native transport enabled client is able to subscribe to diagnostic events that are raised around authentication and CQL operations. These events can then be consumed and used by external tools to implement a Cassandra user auditing solution.

