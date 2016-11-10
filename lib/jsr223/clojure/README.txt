Apache Cassandra User-Defined-Functions JSR 223 scripting
=========================================================

Unfortunately the JSR-223 support provided by the project https://github.com/ato/clojure-jsr223
and the related ones do not provide compileable script support.

The JSR-223 javax.script.Compilable implementation takes source file names or readers but not script sources
as all other JSR-223 implementations do.
