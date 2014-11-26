Apache Cassandra User-Defined-Functions JSR 223 scripting
=========================================================

Using JSR-223 capable Groovy

Tested with version 2.3.6

Installation
------------

1. Download Groovy binary release
2. Unpack the downloaded archive into a temporary directory
3. Copy the jar groovy-all-2.3.6-indy.jar from the Groovy embeddable directory to $CASSANDRA_HOME/lib/jsr223/groovy
   "indy" means "invokedynamic" and is a JVM instruction for scripting languages new to Java 7.
4. Restart your Cassandra daemon if it's already running

Cassandra log should contain a line like this:
  INFO  10:49:45 Found scripting engine Groovy Scripting Engine 2.0 - Groovy 2.3.6 - language names: [groovy, Groovy]
Such a line appears when you already have scripted UDFs in your system or add a scripted UDF for the first time (see below).

Smoke Test
----------

To test Groovy functionality, open cqlsh and execute the following command:
  CREATE OR REPLACE FUNCTION foobar ( input text ) RETURNS text LANGUAGE groovy AS 'return "foo";' ;

If you get the error
  code=2200 [Invalid query] message="Invalid language groovy for 'foobar'"
Groovy for Apache Cassandra has not been installed correctly.

Notes / Java7 invokedynamic
---------------------------

Groovy provides jars that support invokedynamic bytecode instruction. These jars are whose ending with
"-indy.jar".
