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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.NativeSSTableLoaderClient;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

@Deprecated
public final class BulkRecordWriter extends RecordWriter<ByteBuffer, List<Mutation>>
        implements org.apache.hadoop.mapred.RecordWriter<ByteBuffer, List<Mutation>>
{
    public final static String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
    public final static String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
    public final static String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
    public final static String MAX_FAILED_HOSTS = "mapreduce.output.bulkoutputformat.maxfailedhosts";

    private final Logger logger = LoggerFactory.getLogger(BulkRecordWriter.class);

    protected final Configuration conf;
    protected final int maxFailures;
    protected final int bufferSize;
    protected Closeable writer;
    protected SSTableLoader loader;
    protected Progressable progress;
    protected TaskAttemptContext context;
    private File outputDir;
    
    
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

    BulkRecordWriter(TaskAttemptContext context)
    {

        this(HadoopCompat.getConfiguration(context));
        this.context = context;
    }

    BulkRecordWriter(Configuration conf, Progressable progress)
    {
        this(conf);
        this.progress = progress;
    }

    BulkRecordWriter(Configuration conf)
    {
        Config.setOutboundBindAny(true);
        this.conf = conf;
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(Integer.parseInt(conf.get(STREAM_THROTTLE_MBITS, "0")));
        maxFailures = Integer.parseInt(conf.get(MAX_FAILED_HOSTS, "0"));
        bufferSize = Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64"));
    }

    protected String getOutputLocation() throws IOException
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
        if (outputDir == null)
        {
            String keyspace = ConfigHelper.getOutputKeyspace(conf);
            //dir must be named by ks/cf for the loader
            outputDir = new File(getOutputLocation() + File.separator + keyspace + File.separator + ConfigHelper.getOutputColumnFamily(conf));
            outputDir.mkdirs();
        }
        
        if (writer == null)
        {
            AbstractType<?> subcomparator = null;

            if (cfType == CFType.SUPER)
                subcomparator = BytesType.instance;

            writer = new SSTableSimpleUnsortedWriter(
                    outputDir,
                    ConfigHelper.getOutputPartitioner(conf),
                    ConfigHelper.getOutputKeyspace(conf),
                    ConfigHelper.getOutputColumnFamily(conf),
                    BytesType.instance,
                    subcomparator,
                    Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")),
                    ConfigHelper.getOutputCompressionParamaters(conf));

            this.loader = new SSTableLoader(outputDir, new ExternalClient(conf), new NullOutputHandler());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException
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
            Future<StreamState> future = loader.stream();
            while (true)
            {
                try
                {
                    future.get(1000, TimeUnit.MILLISECONDS);
                    break;
                }
                catch (ExecutionException | TimeoutException te)
                {
                    if (null != progress)
                        progress.progress();
                    if (null != context)
                        HadoopCompat.progress(context);
                }
                catch (InterruptedException e)
                {
                    throw new IOException(e);
                }
            }
            if (loader.getFailedHosts().size() > 0)
            {
                if (loader.getFailedHosts().size() > maxFailures)
                    throw new IOException("Too many hosts failed: " + loader.getFailedHosts());
                else
                    logger.warn("Some hosts failed: {}", loader.getFailedHosts());
            }
        }
    }

    @Override
    public void write(ByteBuffer keybuff, List<Mutation> value) throws IOException
    {
        setTypes(value.get(0));
        prepareWriter();
        SSTableSimpleUnsortedWriter ssWriter = (SSTableSimpleUnsortedWriter) writer;
        ssWriter.newRow(keybuff);
        for (Mutation mut : value)
        {
            if (cfType == CFType.SUPER)
            {
                ssWriter.newSuperColumn(mut.getColumn_or_supercolumn().getSuper_column().name);
                if (colType == ColType.COUNTER)
                    for (CounterColumn column : mut.getColumn_or_supercolumn().getCounter_super_column().columns)
                        ssWriter.addCounterColumn(column.name, column.value);
                else
                {
                    for (Column column : mut.getColumn_or_supercolumn().getSuper_column().columns)
                    {
                        if(column.ttl == 0)
                            ssWriter.addColumn(column.name, column.value, column.timestamp);
                        else
                            ssWriter.addExpiringColumn(column.name, column.value, column.timestamp, column.ttl, System.currentTimeMillis() + ((long)column.ttl * 1000));
                    }
                }
            }
            else
            {
                if (colType == ColType.COUNTER)
                    ssWriter.addCounterColumn(mut.getColumn_or_supercolumn().counter_column.name, mut.getColumn_or_supercolumn().counter_column.value);
                else
                {
                    if(mut.getColumn_or_supercolumn().column.ttl == 0)
                        ssWriter.addColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp);
                    else
                        ssWriter.addExpiringColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp, mut.getColumn_or_supercolumn().column.ttl, System.currentTimeMillis() + ((long)(mut.getColumn_or_supercolumn().column.ttl) * 1000));
                }
            }
            if (null != progress)
                progress.progress();
            if (null != context)
                HadoopCompat.progress(context);
        }
    }

    public static class ExternalClient extends NativeSSTableLoaderClient
    {
        public ExternalClient(Configuration conf)
        {
            super(resolveHostAddresses(conf),
                  CqlConfigHelper.getOutputNativePort(conf),
                  ConfigHelper.getOutputKeyspaceUserName(conf),
                  ConfigHelper.getOutputKeyspacePassword(conf),
                  CqlConfigHelper.getSSLOptions(conf).orNull());
        }

        private static Collection<InetAddress> resolveHostAddresses(Configuration conf)
        {
            Set<InetAddress> addresses = new HashSet<>();

            for (String host : ConfigHelper.getOutputInitialAddress(conf).split(","))
            {
                try
                {
                    addresses.add(InetAddress.getByName(host));
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }

            return addresses;
        }
    }

    public static class NullOutputHandler implements OutputHandler
    {
        public void output(String msg) {}
        public void debug(String msg) {}
        public void warn(String msg) {}
        public void warn(String msg, Throwable th) {}
    }
}
