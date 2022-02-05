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
package org.apache.cassandra.hadoop.cql3;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.net.HostAndPort;
import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.NativeSSTableLoaderClient;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;

/**
 * The <code>CqlBulkRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra column family. In particular, it applies the binded variables
 * in the value to the prepared statement, which it associates with the key, and in 
 * turn the responsible endpoint.
 *
 * <p>
 * Furthermore, this writer groups the cql queries by the endpoint responsible for
 * the rows being affected. This allows the cql queries to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 *
 * @see CqlBulkOutputFormat
 */
public class CqlBulkRecordWriter extends RecordWriter<Object, List<ByteBuffer>>
        implements org.apache.hadoop.mapred.RecordWriter<Object, List<ByteBuffer>>
{
    public final static String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
    public final static String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
    public final static String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
    public final static String MAX_FAILED_HOSTS = "mapreduce.output.bulkoutputformat.maxfailedhosts";
    public final static String IGNORE_HOSTS = "mapreduce.output.bulkoutputformat.ignorehosts";

    private final Logger logger = LoggerFactory.getLogger(CqlBulkRecordWriter.class);

    protected final Configuration conf;
    protected final int maxFailures;
    protected final int bufferSize;
    protected Closeable writer;
    protected SSTableLoader loader;
    protected Progressable progress;
    protected TaskAttemptContext context;
    protected final Set<InetAddressAndPort> ignores = new HashSet<>();

    private String keyspace;
    private String table;
    private String schema;
    private String insertStatement;
    private File outputDir;
    private boolean deleteSrc;
    private IPartitioner partitioner;

    CqlBulkRecordWriter(TaskAttemptContext context) throws IOException
    {
        this(HadoopCompat.getConfiguration(context));
        this.context = context;
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf, Progressable progress) throws IOException
    {
        this(conf);
        this.progress = progress;
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf) throws IOException
    {
        this.conf = conf;
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(Integer.parseInt(conf.get(STREAM_THROTTLE_MBITS, "0")));
        maxFailures = Integer.parseInt(conf.get(MAX_FAILED_HOSTS, "0"));
        bufferSize = Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64"));
        setConfigs();
    }
    
    private void setConfigs() throws IOException
    {
        // if anything is missing, exceptions will be thrown here, instead of on write()
        keyspace = ConfigHelper.getOutputKeyspace(conf);
        table = ConfigHelper.getOutputColumnFamily(conf);
        
        // check if table is aliased
        String aliasedCf = CqlBulkOutputFormat.getTableForAlias(conf, table);
        if (aliasedCf != null)
            table = aliasedCf;
        
        schema = CqlBulkOutputFormat.getTableSchema(conf, table);
        insertStatement = CqlBulkOutputFormat.getTableInsertStatement(conf, table);
        outputDir = getTableDirectory();
        deleteSrc = CqlBulkOutputFormat.getDeleteSourceOnSuccess(conf);
        try
        {
            partitioner = ConfigHelper.getInputPartitioner(conf);
        }
        catch (Exception e)
        {
            partitioner = Murmur3Partitioner.instance;
        }
        try
        {
            for (String hostToIgnore : CqlBulkOutputFormat.getIgnoreHosts(conf))
                ignores.add(InetAddressAndPort.getByName(hostToIgnore));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(("Unknown host: " + e.getMessage()));
        }
    }

    protected String getOutputLocation() throws IOException
    {
        String dir = conf.get(OUTPUT_LOCATION, JAVA_IO_TMPDIR.getString());
        if (dir == null)
            throw new IOException("Output directory not defined, if hadoop is not setting java.io.tmpdir then define " + OUTPUT_LOCATION);
        return dir;
    }

    private void prepareWriter() throws IOException
    {
        if (writer == null)
        {
            writer = CQLSSTableWriter.builder()
                                     .forTable(schema)
                                     .using(insertStatement)
                                     .withPartitioner(ConfigHelper.getOutputPartitioner(conf))
                                     .inDirectory(outputDir)
                                     .withBufferSizeInMiB(Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")))
                                     .withPartitioner(partitioner)
                                     .build();
        }

        if (loader == null)
        {
            ExternalClient externalClient = new ExternalClient(conf);
            externalClient.setTableMetadata(TableMetadataRef.forOfflineTools(CreateTableStatement.parse(schema, keyspace).build()));

            loader = new SSTableLoader(outputDir, externalClient, new NullOutputHandler())
            {
                @Override
                public void onSuccess(StreamState finalState)
                {
                    if (deleteSrc)
                        FileUtils.deleteRecursive(outputDir);
                }
            };
        }
    }
    
    /**
     * <p>
     * The column values must correspond to the order in which
     * they appear in the insert stored procedure. 
     * 
     * Key is not used, so it can be null or any object.
     * </p>
     *
     * @param key
     *            any object or null.
     * @param values
     *            the values to write.
     * @throws IOException
     */
    @Override
    public void write(Object key, List<ByteBuffer> values) throws IOException
    {
        prepareWriter();
        try
        {
            ((CQLSSTableWriter) writer).rawAddRow(values);
            
            if (null != progress)
                progress.progress();
            if (null != context)
                HadoopCompat.progress(context);
        } 
        catch (InvalidRequestException e)
        {
            throw new IOException("Error adding row with key: " + key, e);
        }
    }
    
    private File getTableDirectory() throws IOException
    {
        File dir = new File(String.format("%s%s%s%s%s-%s", getOutputLocation(), File.pathSeparator(), keyspace, File.pathSeparator(), table, UUID.randomUUID().toString()));
        
        if (!dir.exists() && !dir.tryCreateDirectories())
        {
            throw new IOException("Failed to created output directory: " + dir);
        }
        
        return dir;
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
            Future<StreamState> future = loader.stream(ignores);
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
    
    public static class ExternalClient extends NativeSSTableLoaderClient
    {
        public ExternalClient(Configuration conf)
        {
            super(resolveHostAddresses(conf),
                  ConfigHelper.getOutputInitialPort(conf),
                  ConfigHelper.getOutputKeyspaceUserName(conf),
                  ConfigHelper.getOutputKeyspacePassword(conf),
                  CqlConfigHelper.getSSLOptions(conf).orNull());
        }

        private static Collection<InetSocketAddress> resolveHostAddresses(Configuration conf)
        {
            Set<InetSocketAddress> addresses = new HashSet<>();
            int port = CqlConfigHelper.getOutputNativePort(conf);
            for (String host : ConfigHelper.getOutputInitialAddress(conf).split(","))
            {
                try
                {
                    HostAndPort hap = HostAndPort.fromString(host);
                    addresses.add(new InetSocketAddress(InetAddress.getByName(hap.getHost()), hap.getPortOrDefault(port)));
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
