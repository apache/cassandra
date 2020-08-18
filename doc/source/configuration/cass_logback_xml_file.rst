.. _cassandra-logback-xml:

logback.xml file 
================================

The ``logback.xml`` configuration file can optionally set logging levels for the logs written to ``system.log`` and ``debug.log``. The logging levels can also be set using ``nodetool setlogginglevels``.

===========================
Options
===========================

``appender name="<appender_choice>"...</appender>``
------

Specify log type and settings. Possible appender names are: ``SYSTEMLOG``, ``DEBUGLOG``, ``ASYNCDEBUGLOG``, and ``STDOUT``. ``SYSTEMLOG`` ensures that WARN and ERROR message are written synchronously to the specified file. ``DEBUGLOG`` and  ``ASYNCDEBUGLOG`` ensure that DEBUG messages are written either synchronously or asynchronously, respectively, to the specified file. ``STDOUT`` writes all messages to the console in a human-readable format.

**Example:** <appender name="SYSTEMLOG" class="ch.qos.logback.core.rolling.RollingFileAppender">

``<file> <filename> </file>``
------

Specify the filename for a log.

**Example:** <file>${cassandra.logdir}/system.log</file>

``<level> <log_level> </level>``
------

Specify the level for a log. Part of the filter. Levels are: ``ALL``, ``TRACE``, ``DEBUG``, ``INFO``, ``WARN``, ``ERROR``, ``OFF``. ``TRACE`` creates the most verbose log, ``ERROR`` the least.

.. note::
Note: Increasing logging levels can generate heavy logging output on a moderately trafficked cluster.
You can use the ``nodetool getlogginglevels`` command to see the current logging configuration.

**Default:** INFO

**Example:** <level>INFO</level>

``<rollingPolicy class="<rolling_policy_choice>" <fileNamePattern><pattern_info></fileNamePattern> ... </rollingPolicy>``
------

Specify the policy for rolling logs over to an archive.

**Example:** <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">

``<fileNamePattern> <pattern_info> </fileNamePattern>``
------

Specify the pattern information for rolling over the log to archive. Part of the rolling policy.

**Example:** <fileNamePattern>${cassandra.logdir}/system.log.%d{yyyy-MM-dd}.%i.zip</fileNamePattern>

``<maxFileSize> <size> </maxFileSize>``
------

Specify the maximum file size to trigger rolling a log. Part of the rolling policy.

**Example:** <maxFileSize>50MB</maxFileSize>

``<maxHistory> <number_of_days> </maxHistory>``
------

Specify the maximum history in days to trigger rolling a log. Part of the rolling policy.

**Example:** <maxHistory>7</maxHistory>

``<encoder> <pattern>...</pattern> </encoder>``
------

Specify the format of the message. Part of the rolling policy.

**Example:** <maxHistory>7</maxHistory>
**Example:** <encoder> <pattern>%-5level [%thread] %date{ISO8601} %F:%L - %msg%n</pattern> </encoder>

Contents of default ``logback.xml``
-----------------------

.. code-block:: XML

	<configuration scan="true" scanPeriod="60 seconds">
	  <jmxConfigurator />

	  <!-- No shutdown hook; we run it ourselves in StorageService after shutdown -->

	  <!-- SYSTEMLOG rolling file appender to system.log (INFO level) -->

	  <appender name="SYSTEMLOG" class="ch.qos.logback.core.rolling.RollingFileAppender">
	    <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
      <level>INFO</level>
	    </filter>
	    <file>${cassandra.logdir}/system.log</file>
	    <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
	      <!-- rollover daily -->
	      <fileNamePattern>${cassandra.logdir}/system.log.%d{yyyy-MM-dd}.%i.zip</fileNamePattern>
	      <!-- each file should be at most 50MB, keep 7 days worth of history, but at most 5GB -->
	      <maxFileSize>50MB</maxFileSize>
	      <maxHistory>7</maxHistory>
	      <totalSizeCap>5GB</totalSizeCap>
	    </rollingPolicy>
	    <encoder>
	      <pattern>%-5level [%thread] %date{ISO8601} %F:%L - %msg%n</pattern>
	    </encoder>
	  </appender>

	  <!-- DEBUGLOG rolling file appender to debug.log (all levels) -->

	  <appender name="DEBUGLOG" class="ch.qos.logback.core.rolling.RollingFileAppender">
	    <file>${cassandra.logdir}/debug.log</file>
	    <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
	      <!-- rollover daily -->
	      <fileNamePattern>${cassandra.logdir}/debug.log.%d{yyyy-MM-dd}.%i.zip</fileNamePattern>
	      <!-- each file should be at most 50MB, keep 7 days worth of history, but at most 5GB -->
	      <maxFileSize>50MB</maxFileSize>
	      <maxHistory>7</maxHistory>
	      <totalSizeCap>5GB</totalSizeCap>
	    </rollingPolicy>
	    <encoder>
	      <pattern>%-5level [%thread] %date{ISO8601} %F:%L - %msg%n</pattern>
	    </encoder>
	  </appender>

	  <!-- ASYNCLOG assynchronous appender to debug.log (all levels) -->

	  <appender name="ASYNCDEBUGLOG" class="ch.qos.logback.classic.AsyncAppender">
	    <queueSize>1024</queueSize>
	    <discardingThreshold>0</discardingThreshold>
	    <includeCallerData>true</includeCallerData>
	    <appender-ref ref="DEBUGLOG" />
	  </appender>

	  <!-- STDOUT console appender to stdout (INFO level) -->

	  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
	    <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
	      <level>INFO</level>
	    </filter>
	    <encoder>
	      <pattern>%-5level [%thread] %date{ISO8601} %F:%L - %msg%n</pattern>
	    </encoder>
	  </appender>

	  <!-- Uncomment bellow and corresponding appender-ref to activate logback metrics
	  <appender name="LogbackMetrics" class="com.codahale.metrics.logback.InstrumentedAppender" />
	   -->

	  <root level="INFO">
	    <appender-ref ref="SYSTEMLOG" />
	    <appender-ref ref="STDOUT" />
	    <appender-ref ref="ASYNCDEBUGLOG" /> <!-- Comment this line to disable debug.log -->
	    <!--
	    <appender-ref ref="LogbackMetrics" />
	    -->
	  </root>

	  <logger name="org.apache.cassandra" level="DEBUG"/>
	  <logger name="com.thinkaurelius.thrift" level="ERROR"/>
	</configuration>
