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
package org.apache.cassandra.hadoop;


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class BulkRecordWriter extends RecordWriter<ByteBuffer,List<Mutation>>
implements org.apache.hadoop.mapred.RecordWriter<ByteBuffer,List<Mutation>>
{
    private final static String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
    private final static String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
    private final static String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
    private final static String MAX_FAILED_HOSTS = "mapreduce.output.bulkoutputformat.maxfailedhosts";
    private final Configuration conf;
    private final Logger logger = LoggerFactory.getLogger(BulkRecordWriter.class);
    private SSTableSimpleUnsortedWriter writer;
    private SSTableLoader loader;
    private final File outputdir;
    private Progressable progress;
    private int maxFailures;

    private enum CFType
    {
        NORMAL,
        SUPER,
    }

    private enum ColType
    {
        NORMAL,
        COUNTER
    }

    private CFType cfType;
    private ColType colType;

    BulkRecordWriter(TaskAttemptContext context) throws IOException
    {
        this(context.getConfiguration());
        this.progress = new Progressable(context);
    }


    BulkRecordWriter(Configuration conf, Progressable progress) throws IOException
    {
        this(conf);
        this.progress = progress;
    }

    BulkRecordWriter(Configuration conf) throws IOException
    {
        Config.setLoadYaml(false);
        Config.setOutboundBindAny(true);
        this.conf = conf;
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(Integer.valueOf(conf.get(STREAM_THROTTLE_MBITS, "0")));
        maxFailures = Integer.valueOf(conf.get(MAX_FAILED_HOSTS, "0"));
        String keyspace = ConfigHelper.getOutputKeyspace(conf);
        outputdir = new File(getOutputLocation() + File.separator + keyspace + File.separator + ConfigHelper.getOutputColumnFamily(conf)); //dir must be named by ks/cf for the loader
        outputdir.mkdirs();
    }

    private String getOutputLocation() throws IOException
    {
        String dir = conf.get(OUTPUT_LOCATION, System.getProperty("java.io.tmpdir"));
        if (dir == null)
            throw new IOException("Output directory not defined, if hadoop is not setting java.io.tmpdir then define " + OUTPUT_LOCATION);
        return dir;
    }

    private void setTypes(Mutation mutation)
    {
       if (cfType == null)
       {
           if (mutation.getColumn_or_supercolumn().isSetSuper_column() || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
               cfType = CFType.SUPER;
           else
               cfType = CFType.NORMAL;
           if (mutation.getColumn_or_supercolumn().isSetCounter_column() || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
               colType = ColType.COUNTER;
           else
               colType = ColType.NORMAL;
       }
    }

    private void prepareWriter() throws IOException
    {
        if (writer == null)
        {
            AbstractType<?> subcomparator = null;
            ExternalClient externalClient = null;
            String username = ConfigHelper.getOutputKeyspaceUserName(conf);
            String password = ConfigHelper.getOutputKeyspacePassword(conf);

            if (cfType == CFType.SUPER)
                subcomparator = BytesType.instance;
            
            this.writer = new SSTableSimpleUnsortedWriter(
                    outputdir,
                    ConfigHelper.getOutputPartitioner(conf),
                    ConfigHelper.getOutputKeyspace(conf),
                    ConfigHelper.getOutputColumnFamily(conf),
                    BytesType.instance,
                    subcomparator,
                    Integer.valueOf(conf.get(BUFFER_SIZE_IN_MB, "64")),
                    ConfigHelper.getOutputCompressionParamaters(conf));

            externalClient = new ExternalClient(ConfigHelper.getOutputInitialAddress(conf), 
                                                ConfigHelper.getOutputRpcPort(conf),
                                                username,
                                                password);

            this.loader = new SSTableLoader(outputdir, externalClient, new NullOutputHandler());
        }
    }

    @Override
    public void write(ByteBuffer keybuff, List<Mutation> value) throws IOException
    {
        setTypes(value.get(0));
        prepareWriter();
        writer.newRow(keybuff);
        for (Mutation mut : value)
        {
            if (cfType == CFType.SUPER)
            {
                writer.newSuperColumn(mut.getColumn_or_supercolumn().getSuper_column().name);
                if (colType == ColType.COUNTER)
                    for (CounterColumn column : mut.getColumn_or_supercolumn().getCounter_super_column().columns)
                        writer.addCounterColumn(column.name, column.value);
                else
                {
                    for (Column column : mut.getColumn_or_supercolumn().getSuper_column().columns)
                    {
                        if(column.ttl == 0)
                            writer.addColumn(column.name, column.value, column.timestamp);
                        else
                            writer.addExpiringColumn(column.name, column.value, column.timestamp, column.ttl, System.currentTimeMillis() + ((long)column.ttl * 1000));
                    }
                }
            }
            else
            {
                if (colType == ColType.COUNTER)
                    writer.addCounterColumn(mut.getColumn_or_supercolumn().counter_column.name, mut.getColumn_or_supercolumn().counter_column.value);
                else
                {
                    if(mut.getColumn_or_supercolumn().column.ttl == 0)
                        writer.addColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp);
                    else
                        writer.addExpiringColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp, mut.getColumn_or_supercolumn().column.ttl, System.currentTimeMillis() + ((long)(mut.getColumn_or_supercolumn().column.ttl) * 1000));
                }
            }
            progress.progress();
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
        if (writer != null)
        {
            writer.close();
            SSTableLoader.LoaderFuture future = loader.stream();
            while (true)
            {
                try
                {
                    future.get(1000, TimeUnit.MILLISECONDS);
                    break;
                }
                catch (TimeoutException te)
                {
                    progress.progress();
                }
                catch (InterruptedException e)
                {
                    throw new IOException(e);
                }
            }
            if (future.hadFailures())
            {
                if (future.getFailedHosts().size() > maxFailures)
                    throw new IOException("Too many hosts failed: " + future.getFailedHosts());
                else
                    logger.warn("Some hosts failed: " + future.getFailedHosts());
            }
        }
    }

    static class ExternalClient extends SSTableLoader.Client
    {
        private final Map<String, Set<String>> knownCfs = new HashMap<String, Set<String>>();
        private final String hostlist;
        private final int rpcPort;
        private final String username;
        private final String password;

        public ExternalClient(String hostlist, int port, String username, String password)
        {
            super();
            this.hostlist = hostlist;
            this.rpcPort = port;
            this.username = username;
            this.password = password;
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

                    // log in
                    client.set_keyspace(keyspace);
                    if (username != null)
                    {
                        Map<String, String> creds = new HashMap<String, String>();
                        creds.put(IAuthenticator.USERNAME_KEY, username);
                        creds.put(IAuthenticator.PASSWORD_KEY, password);
                        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
                        client.login(authRequest);
                    }

                    List<TokenRange> tokenRanges = client.describe_ring(keyspace);
                    List<KsDef> ksDefs = client.describe_keyspaces();

                    setPartitioner(client.describe_partitioner());
                    Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                    for (TokenRange tr : tokenRanges)
                    {
                        Range<Token> range = new Range<Token>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
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

    static class NullOutputHandler implements OutputHandler
    {
        public void output(String msg) {}
        public void debug(String msg) {}
        public void warn(String msg) {}
        public void warn(String msg, Throwable th) {}
    }
}
