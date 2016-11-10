Apache Cassandra User-Defined-Functions JSR 223 scripting
=========================================================

Using JSR-223 capable JRuby

Tested with version 1.7.15

Installation
------------

1. Download JRuby binary release
2. Unpack the downloaded archive into a temporary directory
3. Copy everything from the JRuby lib directory to $CASSANDRA_HOME/lib/jsr223/jruby
4. Restart your Cassandra daemon if it's already running

Cassandra log should contain a line like this:
  INFO  10:29:03 Found scripting engine JSR 223 JRuby Engine 1.7.15 - ruby jruby 1.7.15 - language names: [ruby, jruby]
Such a line appears when you already have scripted UDFs in your system or add a scripted UDF for the first time (see below).


Smoke Test
----------

To test JRuby functionality, open cqlsh and execute the following command:
  CREATE OR REPLACE FUNCTION foobar ( input text ) RETURNS text LANGUAGE ruby AS 'return "foo";' ;

If you get the error
  code=2200 [Invalid query] message="Invalid language ruby for 'foobar'"
JRuby for Apache Cassandra has not been installed correctly.


Ruby require/include
--------------------

You can use Ruby require and include in your scripts as in the following example:


CREATE OR REPLACE FUNCTION foobar ( input text ) RETURNS text LANGUAGE ruby AS '
require "bigdecimal"
require "bigdecimal/math"

include BigMath

a = BigDecimal((PI(100)/2).to_s)

return "foo " + a.to_s;
' ;


Notes / Java7 invokedynamic
---------------------------

See JRuby wiki pages https://github.com/jruby/jruby/wiki/ConfiguringJRuby and
https://github.com/jruby/jruby/wiki/PerformanceTuning for more information and optimization tips.
