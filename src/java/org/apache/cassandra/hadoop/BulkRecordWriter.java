package org.apache.cassandra.hadoop;

/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


final class BulkRecordWriter extends RecordWriter<ByteBuffer,List<Mutation>>
implements org.apache.hadoop.mapred.RecordWriter<ByteBuffer,List<Mutation>>
{
    private final static String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
    private final static String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
    private final static String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
    private final static String IS_SUPERCF = "mapreduce.output.bulkoutputformat.issuper";
    private final Configuration conf;
    private boolean isSuper = false;
    private SSTableSimpleUnsortedWriter writer;
    private SSTableLoader loader;

    static {
        DatabaseDescriptor.initDefaultsOnly(); // make sure DD doesn't load yaml
    }

    BulkRecordWriter(TaskAttemptContext context) throws IOException
    {
        this(context.getConfiguration());
    }
    
    BulkRecordWriter(Configuration conf) throws IOException
    {
        this.conf = conf;
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(Integer.valueOf(conf.get(STREAM_THROTTLE_MBITS, "0")));
        String keyspace = ConfigHelper.getOutputKeyspace(conf);
        File outputdir = new File(getOutputLocation() + File.separator + keyspace); //dir must be named by ks for the loader
        outputdir.mkdirs();
        this.isSuper = Boolean.valueOf(conf.get(IS_SUPERCF));
        AbstractType subcomparator = null;
        if (isSuper)
            subcomparator = BytesType.instance;
        this.writer = new SSTableSimpleUnsortedWriter(
                outputdir,
                keyspace,
                ConfigHelper.getOutputColumnFamily(conf),
                BytesType.instance,
                subcomparator,
                Integer.valueOf(conf.get(BUFFER_SIZE_IN_MB, "64")));

        this.loader = new SSTableLoader(outputdir, new ExternalClient(ConfigHelper.getOutputInitialAddress(conf), ConfigHelper.getOutputRpcPort(conf)), new NullOutputHandler());
    }

    private String getOutputLocation() throws IOException
    {
        String dir = conf.get(OUTPUT_LOCATION, conf.get("mapred.local.dir"));
        if (dir == null)
            throw new IOException("Output directory not defined, if hadoop is not setting mapred.local.dir then define " + OUTPUT_LOCATION);
        return dir;
    }


    @Override
    public void write(ByteBuffer keybuff, List<Mutation> value) throws IOException
    {
        writer.newRow(keybuff);
        for (Mutation mut : value)
        {
            if (isSuper)
            {
                writer.newSuperColumn(mut.getColumn_or_supercolumn().getSuper_column().name);
                for (Column column : mut.getColumn_or_supercolumn().getSuper_column().columns)
                   writer.addColumn(column.name, column.value, column.timestamp);
            }
            else
                writer.addColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException
    {
        close();
    }

    /** Fills the deprecated RecordWriter interface for streaming. */
    @Deprecated
    public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException
    {
        close();
    }

    private void close() throws IOException
    {
        writer.close();
        try
        {
            loader.stream().get();
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }

    static class ExternalClient extends SSTableLoader.Client
    {
        private final Map<String, Set<String>> knownCfs = new HashMap<String, Set<String>>();
        private String hostlist;
        private int rpcPort;

        public ExternalClient(String hostlist, int port)
        {
            super();
            this.hostlist = hostlist;
            this.rpcPort = port;
        }

        public void init(String keyspace)
        {
            Set<InetAddress> hosts = new HashSet<InetAddress>();
            String[] nodes = hostlist.split(",");
            for (String node : nodes)
            {
                try
                {
                    hosts.add(InetAddress.getByName(node));
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }
            Iterator<InetAddress> hostiter = hosts.iterator();
            while (hostiter.hasNext())
            {
                try
                {
                    InetAddress host = hostiter.next();
                    Cassandra.Client client = createThriftClient(host.getHostAddress(), rpcPort);
                    List<TokenRange> tokenRanges = client.describe_ring(keyspace);
                    List<KsDef> ksDefs = client.describe_keyspaces();

                    setPartitioner(client.describe_partitioner());
                    Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                    for (TokenRange tr : tokenRanges)
                    {
                        Range range = new Range(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
                        for (String ep : tr.endpoints)
                        {
                            addRangeForEndpoint(range, InetAddress.getByName(ep));
                        }
                    }

                    for (KsDef ksDef : ksDefs)
                    {
                        Set<String> cfs = new HashSet<String>();
                        for (CfDef cfDef : ksDef.cf_defs)
                            cfs.add(cfDef.name);
                        knownCfs.put(ksDef.name, cfs);
                    }
                    break;
                }
                catch (Exception e)
                {
                    if (!hostiter.hasNext())
                        throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
                }
            }
        }

        public boolean validateColumnFamily(String keyspace, String cfName)
        {
            Set<String> cfs = knownCfs.get(keyspace);
            return cfs != null && cfs.contains(cfName);
        }

        private static Cassandra.Client createThriftClient(String host, int port) throws TTransportException
        {
            TSocket socket = new TSocket(host, port);
            TTransport trans = new TFramedTransport(socket);
            trans.open();
            TProtocol protocol = new org.apache.thrift.protocol.TBinaryProtocol(trans);
            return new Cassandra.Client(protocol);
        }
    }

    static class NullOutputHandler implements SSTableLoader.OutputHandler
    {
        public void output(String msg) {}

        public void debug(String msg) {}
    }
}
