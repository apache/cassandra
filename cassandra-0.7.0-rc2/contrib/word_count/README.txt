WordCount hadoop example: Inserts a bunch of words across multiple rows,
and counts them, with RandomPartitioner.

The scripts in bin/ assume you are running with cwd of contrib/word_count.

First build and start a Cassandra server with the default configuration*, 
then run

contrib/word_count$ ant
contrib/word_count$ bin/word_count_setup
contrib/word_count$ bin/word_count

The output of the word count can now be configured. In the bin/word_count
file, you can specify the OUTPUT_REDUCER. The two options are 'filesystem'
and 'cassandra'. The filesystem option outputs to the /tmp/word_count*
directories. The cassandra option outputs to the 'Standard2' column family.

In order to view the results in Cassandra, one can use python/pycassa and
perform the following operations:
$ python
>>> import pycassa
>>> con = pycassa.connect('Keyspace1')
>>> cf = pycassa.ColumnFamily(con, 'Standard2')
>>> list(cf.get_range())

Read the code in src/ for more details.

*If you want to point wordcount at a real cluster, modify the seed
and listenaddress settings accordingly.
