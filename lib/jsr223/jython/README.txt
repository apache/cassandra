Apache Cassandra User-Defined-Functions JSR 223 scripting
=========================================================

Using JSR-223 capable Jython

Tested with version 2.3.5

Installation
------------

1. Download Jython binary release
2. Unpack the downloaded archive into a temporary directory
3. Copy the jar jython.jar from the Jython directory to $CASSANDRA_HOME/lib/jsr223/jython
4. Restart your Cassandra daemon if it's already running

Cassandra log should contain a line like this:
  INFO  10:58:18 Found scripting engine jython 2.5.3 - python 2.5 - language names: [python, jython]
Such a line appears when you already have scripted UDFs in your system or add a scripted UDF for the first time (see below).

Smoke Test
----------

To test Jython functionality, open cqlsh and execute the following command:
  CREATE OR REPLACE FUNCTION foobar ( input text ) RETURNS text LANGUAGE python AS '''foo''' ;

If you get the error
  code=2200 [Invalid query] message="Invalid language python for 'foobar'"
Jython for Apache Cassandra has not been installed correctly.

Notes / Java7 invokedynamic
---------------------------

Jython currently targets Java6 only. They want to switch to Java7 + invokedynamic in Jython 3.
