Introduction
============

WordCount hadoop example: Inserts a bunch of words across multiple rows,
and counts them, with RandomPartitioner. The word_count_counters example sums
the value of counter columns for a key.

The scripts in bin/ assume you are running with cwd of contrib/word_count.


Running
=======

First build and start a Cassandra server with the default configuration*, 
then run

contrib/word_count$ ant
contrib/word_count$ bin/word_count_setup
contrib/word_count$ bin/word_count
contrib/word_count$ bin/word_count_counters

In order to view the results in Cassandra, one can use bin/cqlsh and
perform the following operations:
$ bin/cqlsh localhost
> use wordcount;
> select * from output_words;

The output of the word count can now be configured. In the bin/word_count
file, you can specify the OUTPUT_REDUCER. The two options are 'filesystem'
and 'cassandra'. The filesystem option outputs to the /tmp/word_count*
directories. The cassandra option outputs to the 'output_words' column family
in the 'wordcount' keyspace.  'cassandra' is the default.

Read the code in src/ for more details.

The word_count_counters example sums the counter columns for a row. The output
is written to a text file in /tmp/word_count_counters.

*It is recommended to turn off vnodes when running Cassandra with hadoop.
This is done by setting "num_tokens: 1" in cassandra.yaml. If you want to
point wordcount at a real cluster, modify the seed and listenaddress
settings accordingly.


Troubleshooting
===============

word_count uses conf/log4j.properties to log to wc.out.
