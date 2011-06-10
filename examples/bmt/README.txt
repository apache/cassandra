This is an example for the deprecated BinaryMemtable bulk-load interface.

Inserting data through the binary memtable, allows you to skip the
commit log overhead, and an ack from Thrift on every insert. The
example below utilizes Hadoop to generate all the data necessary to
send to Cassandra, and sends it using the Binary Memtable
interface. What Hadoop ends up doing is creating the actual data that
gets put into an SSTable as if you were using Thrift. With enough
Hadoop nodes inserting the data, the bottleneck at this point should
become the network.

We recommend adjusting the compaction threshold to 0 while the import
is running. After the import, you need to run `nodeprobe -host <IP>
flush_binary <Keyspace>` on every node, as this will flush the
remaining data still left in memory to disk. Then it's recommended to
adjust the compaction threshold to it's original value.

The example in CassandraBulkLoader.java is a sample Hadoop job that
inserts SuperColumns. It can be tweaked to work with normal Columns.

You should construct your data you want to import as rows delimited by
a new line. You end up grouping by <Key> in the mapper, so that
the end result generates the data set into a column oriented
subset. Once you get to the reduce aspect, you can generate the
ColumnFamilies you want inserted, and send it to your nodes.

For Cassandra 0.6.4, we modified this example to wait for acks from
all Cassandra nodes for each row before proceeding to the next.  This
means to keep Cassandra similarly busy you can either
1) add more reducer tasks,
2) remove the "wait for acks" block of code,
3) parallelize the writing of rows to Cassandra, e.g. with an Executor.

THIS CANNOT RUN ON THE SAME IP ADDRESS AS A CASSANDRA INSTANCE.
