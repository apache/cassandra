Hadoop Streaming WordCount example: runs a simple Python wordcount mapper script, and a slightly
more complex Cassandra specific reducer that outputs the results of the wordcount as Avro
records to Cassandra.

Dependencies for the example:
* Hadoop 0.20.x installed, such that the 'bin/hadoop' launcher is available on your path
* Avro's Python library installed

Unfortunately, due to the way Hadoop builds its CLASSPATH, it is also necessary to
upgrade a library that conflicts between Hadoop and Avro. Within your Hadoop distribution,
you'll need to ensure that the following jars are of a sufficiently high version, or else
you will see a classloader error at runtime:
* jackson-core-asl >= 1.4.0
* jackson-mapper-asl >= 1.4.0

To run the example, edit bin/streaming to point to a valid Cassandra cluster and
then execute it over a text input, located on the local filesystem or in Hadoop:
$ bin/streaming -input <mytxtfile.txt>

Hadoop streaming will execute a simple wordcount over your input files, and write the generated
counts to Keyspace1,Standard1. bin/reducer.py gives an example of how to format the output of
a script as Avro records which can be consumed by Cassandra's AvroOutputReader, and bin/streaming
shows the necessary incantations to execute a Hadoop Streaming job with Cassandra as output.

