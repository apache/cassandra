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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.hadoop.AbstractBulkRecordWriter;
import org.apache.cassandra.hadoop.BulkRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.thrift.ITransportFactory;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

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
public class CqlBulkRecordWriter extends AbstractBulkRecordWriter<Object, List<ByteBuffer>>
{
    private String keyspace;
    private String columnFamily;
    private String schema;
    private String insertStatement;
    private File outputDir;
    private boolean deleteSrc;

    CqlBulkRecordWriter(TaskAttemptContext context) throws IOException
    {
        super(context);
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf, Progressable progress) throws IOException
    {
        super(conf, progress);
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf) throws IOException
    {
        super(conf);
        setConfigs();
    }
    
    private void setConfigs() throws IOException
    {
        // if anything is missing, exceptions will be thrown here, instead of on write()
        keyspace = ConfigHelper.getOutputKeyspace(conf);
        columnFamily = ConfigHelper.getOutputColumnFamily(conf);
        schema = CqlBulkOutputFormat.getColumnFamilySchema(conf, columnFamily);
        insertStatement = CqlBulkOutputFormat.getColumnFamilyInsertStatement(conf, columnFamily);
        outputDir = getColumnFamilyDirectory();
        deleteSrc = CqlBulkOutputFormat.getDeleteSourceOnSuccess(conf);
    }

    
    private void prepareWriter() throws IOException
    {
        try
        {
            if (writer == null)
            {
                writer = CQLSSTableWriter.builder()
                    .forTable(schema)
                    .using(insertStatement)
                    .withPartitioner(ConfigHelper.getOutputPartitioner(conf))
                    .inDirectory(outputDir)
                    .withBufferSizeInMB(Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")))
                    .build();
            }
            if (loader == null)
            {
                BulkLoader.ExternalClient externalClient = getExternalClient(conf);
                this.loader = new SSTableLoader(outputDir, externalClient, new BulkRecordWriter.NullOutputHandler()) {
                    @Override
                    public void onSuccess(StreamState finalState)
                    {
                        if (deleteSrc)
                            FileUtils.deleteRecursive(outputDir);
                    }
                };
            }
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }      
    }
    
    /**
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
    
    private File getColumnFamilyDirectory() throws IOException
    {
        File dir = new File(String.format("%s%s%s%s%s-%s", getOutputLocation(), File.separator, keyspace, File.separator, columnFamily, UUID.randomUUID().toString()));
        
        if (!dir.exists() && !dir.mkdirs())
        {
            throw new IOException("Failed to created output directory: " + dir);
        }
        
        return dir;
    }

    private BulkLoader.ExternalClient getExternalClient(Configuration conf)
    {
        Set<InetAddress> hosts = new HashSet<InetAddress>();
        String outputAddress = ConfigHelper.getOutputInitialAddress(conf);
        if (outputAddress == null) outputAddress = "localhost";
        String[] nodes = outputAddress.split(",");
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
        int rpcPort = ConfigHelper.getOutputRpcPort(conf);
        String username = ConfigHelper.getOutputKeyspaceUserName(conf);
        String password = ConfigHelper.getOutputKeyspacePassword(conf);
        ITransportFactory transportFactory = ConfigHelper.getClientTransportFactory(conf);
        return new BulkLoader.ExternalClient(hosts,
                rpcPort,
                username,
                password,
                transportFactory,
                CqlBulkOutputFormat.getStoragePort(conf),
                CqlBulkOutputFormat.getSSLStoragePort(conf),
                getServerEncryptOpt(conf));
    }

    private ServerEncryptionOptions getServerEncryptOpt(Configuration conf)
    {
        ServerEncryptionOptions encryptOpt = new ServerEncryptionOptions();
        String internodeEncrypt = CqlBulkOutputFormat.getInternodeEncryption(conf);
        if (StringUtils.isEmpty(internodeEncrypt))
            return encryptOpt;

        encryptOpt.internode_encryption = EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.valueOf(internodeEncrypt);
        encryptOpt.keystore = CqlBulkOutputFormat.getServerKeystore(conf);
        encryptOpt.truststore = CqlBulkOutputFormat.getServerTruststore(conf);
        encryptOpt.keystore_password = CqlBulkOutputFormat.getServerKeystorePassword(conf);
        encryptOpt.truststore_password = CqlBulkOutputFormat.getServerTruststorePassword(conf);
        String cipherSuites = CqlBulkOutputFormat.getServerCipherSuites(conf);
        if (!StringUtils.isEmpty(cipherSuites))
            encryptOpt.cipher_suites = cipherSuites.replace(" ", "").split(",");
        return encryptOpt;
    }
}
