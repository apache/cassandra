WordCount hadoop example: Inserts a bunch of words across multiple rows,
and counts them, with RandomPartitioner.

The scripts in bin/ assume you are running with cwd of contrib/word_count.

First build and start a Cassandra server with the default configuration*, 
then run

contrib/word_count$ ant
contrib/word_count$ bin/word_count_setup
contrib/word_count$ bin/word_count

Output will be in /tmp/word_count*.

Read the code in src/ for more details.

*If you want to point wordcount at a real cluster, modify the seed
and listenaddress settings in storage-conf.xml accordingly.

*For Mac users, the storage-conf.xml uses 127.0.0.2 for the 
word_count_setup. Mac OS X doesn't have that address available.
To add it, run this before running bin/word_count_setup:
sudo ifconfig lo0 alias 127.0.0.2 up
