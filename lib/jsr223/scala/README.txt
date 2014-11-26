Apache Cassandra User-Defined-Functions JSR 223 scripting
=========================================================

Using JSR-223 capable Scala

Tested with version 2.11.2

Installation
------------

1. Download Scala binary release
2. Unpack the downloaded archive into a temporary directory
3. Copy the following jars from the Scala lib directory to $CASSANDRA_HOME/lib/jsr223/scala
   scala-compiler.jar
   scala-library.jar
   scala-reflect.jar
4. Restart your Cassandra daemon if it's already running

Cassandra log should contain a line like this:
  INFO  11:42:35 Found scripting engine Scala Interpreter 1.0 - Scala version 2.11.2 - language names: [scala]
Such a line appears when you already have scripted UDFs in your system or add a scripted UDF for the first time (see below).

Smoke Test
----------

To test Scala functionality, open cqlsh and execute the following command:
  CREATE OR REPLACE FUNCTION foobar ( input text ) RETURNS text LANGUAGE scala AS 'return "foo";' ;

If you get the error
  code=2200 [Invalid query] message="Invalid language scala for 'foobar'"
Scala for Apache Cassandra has not been installed correctly.

Notes / Java7 invokedynamic
---------------------------

Scala 2.10 has Java6 support only. 2.11 has experimental invokedynamic support (use at your own risk!).
2.12 introduces an upgrade directly to Java8 - see https://stackoverflow.com/questions/14285894/advantages-of-scala-emitting-bytecode-for-the-jvm-1-7