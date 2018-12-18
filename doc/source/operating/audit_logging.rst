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

.. highlight:: none



Audit Logging
------------------

Audit logging in Cassandra logs every incoming CQL command request, Authentication (successful as well as unsuccessful login)
to C* node. Currently, there are two implementations provided, the custom logger can be implemented and injected with the
class name as a parameter in cassandra.yaml.

- ``BinAuditLogger`` An efficient way to log events to file in a binary format.
- ``FileAuditLogger`` Logs events to  ``audit/audit.log`` file using slf4j logger.

*Recommendation* ``BinAuditLogger`` is a community recommended logger considering the performance

What does it capture
^^^^^^^^^^^^^^^^^^^^^^^

Audit logging captures following events

- Successful as well as unsuccessful login attempts.

- All database commands executed via Native protocol (CQL) attempted or successfully executed.

Limitations
^^^^^^^^^^^

Executing prepared statements will log the query as provided by the client in the prepare call, along with the execution time stamp and all other attributes (see below). Actual values bound for prepared statement execution will not show up in the audit log.

What does it log
^^^^^^^^^^^^^^^^^^^
Each audit log implementation has access to the following attributes, and for the default text based logger these fields are concatenated with `|` s to yield the final message.

 - ``user``: User name(if available)
 - ``host``: Host IP, where the command is being executed
 - ``source ip address``: Source IP address from where the request initiated
 - ``source port``: Source port number from where the request initiated
 - ``timestamp``: unix time stamp
 - ``type``: Type of the request (SELECT, INSERT, etc.,)
 - ``category`` - Category of the request (DDL, DML, etc.,)
 - ``keyspace`` - Keyspace(If applicable) on which request is targeted to be executed
 - ``scope`` - Table/Aggregate name/ function name/ trigger name etc., as applicable
 - ``operation`` - CQL command being executed

How to configure
^^^^^^^^^^^^^^^^^^
Auditlog can be configured using cassandra.yaml. If you want to try Auditlog on one node, it can also be enabled and configured using ``nodetool``.

cassandra.yaml configurations for AuditLog
"""""""""""""""""""""""""""""""""""""""""""""
	- ``enabled``: This option enables/ disables audit log
	- ``logger``: Class name of the logger/ custom logger.
	- ``audit_logs_dir``: Auditlogs directory location, if not set, default to `cassandra.logdir.audit` or `cassandra.logdir` + /audit/
	- ``included_keyspaces``: Comma separated list of keyspaces to be included in audit log, default - includes all keyspaces
	- ``excluded_keyspaces``: Comma separated list of keyspaces to be excluded from audit log, default - excludes no keyspace except `system`,  `system_schema` and `system_virtual_schema`
	- ``included_categories``: Comma separated list of Audit Log Categories to be included in audit log, default - includes all categories
	- ``excluded_categories``: Comma separated list of Audit Log Categories to be excluded from audit log, default - excludes no category
	- ``included_users``: Comma separated list of users to be included in audit log, default - includes all users
	- ``excluded_users``: Comma separated list of users to be excluded from audit log, default - excludes no user


List of available categories are: QUERY, DML, DDL, DCL, OTHER, AUTH, ERROR, PREPARE

NodeTool command to enable AuditLog
"""""""""""""""""""""""""""""""""""""
``enableauditlog``: Enables AuditLog with yaml defaults. yaml configurations can be overridden using options via nodetool command.

::

    nodetool enableauditlog

Options
**********


``--excluded-categories``
    Comma separated list of Audit Log Categories to be excluded for
    audit log. If not set the value from cassandra.yaml will be used

``--excluded-keyspaces``
    Comma separated list of keyspaces to be excluded for audit log. If
    not set the value from cassandra.yaml will be used.
    Please remeber that `system`, `system_schema` and `system_virtual_schema` are excluded by default,
    if you are overwriting this option via nodetool,
    remember to add these keyspaces back if you dont want them in audit logs

``--excluded-users``
    Comma separated list of users to be excluded for audit log. If not
    set the value from cassandra.yaml will be used

``--included-categories``
    Comma separated list of Audit Log Categories to be included for
    audit log. If not set the value from cassandra.yaml will be used

``--included-keyspaces``
    Comma separated list of keyspaces to be included for audit log. If
    not set the value from cassandra.yaml will be used

``--included-users``
    Comma separated list of users to be included for audit log. If not
    set the value from cassandra.yaml will be used

``--logger``
    Logger name to be used for AuditLogging. Default BinAuditLogger. If
    not set the value from cassandra.yaml will be used


NodeTool command to disable AuditLog
"""""""""""""""""""""""""""""""""""""""

``disableauditlog``: Disables AuditLog.

::

    nodetool disableuditlog







NodeTool command to reload AuditLog filters
"""""""""""""""""""""""""""""""""""""""""""""

``enableauditlog``: NodeTool enableauditlog command can be used to reload auditlog filters when called with default or previous ``loggername`` and updated filters

E.g.,

::

    nodetool enableauditlog --loggername <Default/ existing loggerName> --included-keyspaces <New Filter values>



View the contents of AuditLog Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``auditlogviewer`` is the new tool introduced to help view the contents of binlog file in human readable text format.

::

	auditlogviewer <path1> [<path2>...<pathN>] [options]

Options
""""""""

``-f,--follow`` 
	Upon reacahing the end of the log continue indefinitely
				waiting for more records
``-r,--roll_cycle``
   How often to roll the log file was rolled. May be
				necessary for Chronicle to correctly parse file names. (MINUTELY, HOURLY,
				DAILY). Default HOURLY.

``-h,--help``
         display this help message

For example, to dump the contents of audit log files on the console

::

	auditlogviewer /logs/cassandra/audit

Sample output
"""""""""""""

::

    LogMessage: user:anonymous|host:localhost/X.X.X.X|source:/X.X.X.X|port:60878|timestamp:1521158923615|type:USE_KS|category:DDL|ks:dev1|operation:USE "dev1"



Configuring BinAuditLogger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To use ``BinAuditLogger`` as a logger in AuditLogging, set the logger to ``BinAuditLogger`` in cassandra.yaml under ``audit_logging_options`` section. ``BinAuditLogger`` can be futher configued using its advanced options in cassandra.yaml.


Adcanced Options for BinAuditLogger
""""""""""""""""""""""""""""""""""""""

``block``
	Indicates if the AuditLog should block if the it falls behind or should drop audit log records. Default is set to ``true`` so that AuditLog records wont be lost

``max_queue_weight``
	Maximum weight of in memory queue for records waiting to be written to the audit log file before blocking or dropping the log records. Default is set to ``256 * 1024 * 1024``

``max_log_size``
	Maximum size of the rolled files to retain on disk before deleting the oldest file. Default is set to ``16L * 1024L * 1024L * 1024L``

``roll_cycle``
	How often to roll Audit log segments so they can potentially be reclaimed. Available options are: MINUTELY, HOURLY, DAILY, LARGE_DAILY, XLARGE_DAILY, HUGE_DAILY.For more options, refer: net.openhft.chronicle.queue.RollCycles. Default is set to ``"HOURLY"``

Configuring FileAuditLogger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To use ``FileAuditLogger`` as a logger in AuditLogging, apart from setting the class name in cassandra.yaml, following configuration is needed to have the audit log events to flow through separate log file instead of system.log


.. code-block:: xml

    	<!-- Audit Logging (FileAuditLogger) rolling file appender to audit.log -->
    	<appender name="AUDIT" class="ch.qos.logback.core.rolling.RollingFileAppender">
    	  <file>${cassandra.logdir}/audit/audit.log</file>
    	  <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
    	    <!-- rollover daily -->
    	    <fileNamePattern>${cassandra.logdir}/audit/audit.log.%d{yyyy-MM-dd}.%i.zip</fileNamePattern>
    	    <!-- each file should be at most 50MB, keep 30 days worth of history, but at most 5GB -->
    	    <maxFileSize>50MB</maxFileSize>
    	    <maxHistory>30</maxHistory>
    	    <totalSizeCap>5GB</totalSizeCap>
    	  </rollingPolicy>
    	  <encoder>
    	    <pattern>%-5level [%thread] %date{ISO8601} %F:%L - %msg%n</pattern>
    	  </encoder>
    	</appender>

      	<!-- Audit Logging additivity to redirect audt logging events to audit/audit.log -->
      	<logger name="org.apache.cassandra.audit" additivity="false" level="INFO">
        	<appender-ref ref="AUDIT"/>
      	</logger>
