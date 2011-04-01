/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
 /**
  * Cassandra has a back door called the Binary Memtable. The purpose of this backdoor is to
  * mass import large amounts of data, without using the Thrift interface.
  *
  * Inserting data through the binary memtable, allows you to skip the commit log overhead, and an ack
  * from Thrift on every insert. The example below utilizes Hadoop to generate all the data necessary 
  * to send to Cassandra, and sends it using the Binary Memtable interface. What Hadoop ends up doing is
  * creating the actual data that gets put into an SSTable as if you were using Thrift. With enough Hadoop nodes
  * inserting the data, the bottleneck at this point should become the network.
  * 
  * We recommend adjusting the compaction threshold to 0, while the import is running. After the import, you need
  * to run `nodeprobe -host <IP> flush_binary <Keyspace>` on every node, as this will flush the remaining data still left 
  * in memory to disk. Then it's recommended to adjust the compaction threshold to it's original value.
  * 
  * The example below is a sample Hadoop job, and it inserts SuperColumns. It can be tweaked to work with normal Columns.
  *
  * You should construct your data you want to import as rows delimited by a new line. You end up grouping by <Key>
  * in the mapper, so that the end result generates the data set into a column oriented subset. Once you get to the
  * reduce aspect, you can generate the ColumnFamilies you want inserted, and send it to your nodes.
  *
  * For Cassandra 0.6.4, we modified this example to wait for acks from all Cassandra nodes for each row
  * before proceeding to the next.  This means to keep Cassandra similarly busy you can either
  * 1) add more reducer tasks,
  * 2) remove the "wait for acks" block of code,
  * 3) parallelize the writing of rows to Cassandra, e.g. with an Executor.
  *
  * THIS CANNOT RUN ON THE SAME IP ADDRESS AS A CASSANDRA INSTANCE.
  */
  
package org.apache.cassandra.bulkloader;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Charsets;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CassandraBulkLoader {
    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // This is a simple key/value mapper.
            output.collect(key, value);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Path[] localFiles;
        private JobConf jobconf;

        public void configure(JobConf job) {
            this.jobconf = job;
            String cassConfig;

            // Get the cached files
            try
            {
                localFiles = DistributedCache.getLocalCacheFiles(job);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            cassConfig = localFiles[0].getParent().toString();

            System.setProperty("storage-config",cassConfig);

            try
            {
                StorageService.instance.initClient();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            try
            {
                Thread.sleep(10*1000);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void close()
        {
            try
            {
                // release the cache
                DistributedCache.releaseCache(new URI("/cassandra/storage-conf.xml#storage-conf.xml"), this.jobconf);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            catch (URISyntaxException e)
            {
                throw new RuntimeException(e);
            }
            try
            {
                // Sleep just in case the number of keys we send over is small
                Thread.sleep(3*1000);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            StorageService.instance.stopClient();
        }

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        {
            ColumnFamily columnFamily;
            String keyspace = "Keyspace1";
            String cfName = "Super1";
            Message message;
            List<ColumnFamily> columnFamilies;
            columnFamilies = new LinkedList<ColumnFamily>();
            String line;

            /* Create a column family */
            columnFamily = ColumnFamily.create(keyspace, cfName);
            while (values.hasNext()) {
                // Split the value (line based on your own delimiter)
                line = values.next().toString();
                String[] fields = line.split("\1");
                String SuperColumnName = fields[1];
                String ColumnName = fields[2];
                String ColumnValue = fields[3];
                int timestamp = 0;
                columnFamily.addColumn(new QueryPath(cfName,
                                                     ByteBufferUtil.bytes(SuperColumnName),
                                                     ByteBufferUtil.bytes(ColumnName)),
                                       ByteBufferUtil.bytes(ColumnValue),
                                       timestamp);
            }

            columnFamilies.add(columnFamily);

            /* Get serialized message to send to cluster */
            message = createMessage(keyspace, key.getBytes(), cfName, columnFamilies);
            List<IAsyncResult> results = new ArrayList<IAsyncResult>();
            for (InetAddress endpoint: StorageService.instance.getNaturalEndpoints(keyspace, ByteBufferUtil.bytes(key)))
            {
                /* Send message to end point */
                results.add(MessagingService.instance().sendRR(message, endpoint));
            }
            /* wait for acks */
            for (IAsyncResult result : results)
            {
                try
                {
                    result.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e)
                {
                    // you should probably add retry logic here
                    throw new RuntimeException(e);
                }
            }
            
            output.collect(key, new Text(" inserted into Cassandra node(s)"));
        }
    }

    public static void runJob(String[] args)
    {
        JobConf conf = new JobConf(CassandraBulkLoader.class);

        if(args.length >= 4)
        {
          conf.setNumReduceTasks(new Integer(args[3]));
        }

        try
        {
            // We store the cassandra storage-conf.xml on the HDFS cluster
            DistributedCache.addCacheFile(new URI("/cassandra/storage-conf.xml#storage-conf.xml"), conf);
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setJobName("CassandraBulkLoader_v2");
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        try
        {
            JobClient.runJob(conf);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Message createMessage(String keyspace, byte[] key, String columnFamily, List<ColumnFamily> columnFamilies)
    {
        ColumnFamily baseColumnFamily;
        DataOutputBuffer bufOut = new DataOutputBuffer();
        RowMutation rm;
        Message message;
        Column column;

        /* Get the first column family from list, this is just to get past validation */
        baseColumnFamily = new ColumnFamily(ColumnFamilyType.Standard,
                                            DatabaseDescriptor.getComparator(keyspace, columnFamily),
                                            DatabaseDescriptor.getSubComparator(keyspace, columnFamily),
                                            CFMetaData.getId(keyspace, columnFamily));
        
        for(ColumnFamily cf : columnFamilies) {
            bufOut.reset();
            ColumnFamily.serializer().serializeWithIndexes(cf, bufOut);
            byte[] data = new byte[bufOut.getLength()];
            System.arraycopy(bufOut.getData(), 0, data, 0, bufOut.getLength());

            column = new Column(FBUtilities.toByteBuffer(cf.id()), ByteBuffer.wrap(data), 0);
            baseColumnFamily.addColumn(column);
        }
        rm = new RowMutation(keyspace, ByteBuffer.wrap(key));
        rm.add(baseColumnFamily);

        try
        {
            /* Make message */
            message = rm.makeRowMutationMessage(StorageService.Verb.BINARY, MessagingService.version_);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return message;
    }
    public static void main(String[] args) throws Exception
    {
        runJob(args);
    }
}
