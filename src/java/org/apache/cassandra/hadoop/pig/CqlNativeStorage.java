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
package org.apache.cassandra.hadoop.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.datastax.driver.core.Row;

public class CqlNativeStorage extends CqlStorage
{
    private RecordReader<Long, Row> reader;
    private String nativePort;
    private String nativeCoreConnections;
    private String nativeMaxConnections;
    private String nativeMinSimultReqs;
    private String nativeMaxSimultReqs;
    private String nativeConnectionTimeout;
    private String nativeReadConnectionTimeout;
    private String nativeReceiveBufferSize;
    private String nativeSendBufferSize;
    private String nativeSolinger;
    private String nativeTcpNodelay;
    private String nativeReuseAddress;
    private String nativeKeepAlive;
    private String nativeAuthProvider;
    private String nativeSSLTruststorePath;
    private String nativeSSLKeystorePath;
    private String nativeSSLTruststorePassword;
    private String nativeSSLKeystorePassword;
    private String nativeSSLCipherSuites;
    private String inputCql;

    public CqlNativeStorage()
    {
        this(1000);
    }

    /** @param pageSize limit number of CQL rows to fetch in a thrift request */
    public CqlNativeStorage(int pageSize)
    {
        super(pageSize);
        DEFAULT_INPUT_FORMAT = "org.apache.cassandra.hadoop.cql3.CqlInputFormat";
    }

    public void prepareToRead(RecordReader reader, PigSplit split)
    {
        this.reader = reader;
    }

    /** get next row */
    public Tuple getNext() throws IOException
    {
        try
        {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;

            CfInfo cfInfo = getCfInfo(loadSignature);
            CfDef cfDef = cfInfo.cfDef;
            Row row = reader.getCurrentValue();
            Tuple tuple = TupleFactory.getInstance().newTuple(cfDef.column_metadata.size());
            Iterator<ColumnDef> itera = cfDef.column_metadata.iterator();
            int i = 0;
            while (itera.hasNext())
            {
                ColumnDef cdef = itera.next();
                ByteBuffer columnValue = row.getBytesUnsafe(ByteBufferUtil.string(cdef.name.duplicate()));
                if (columnValue != null)
                {
                    Cell cell = new BufferCell(CellNames.simpleDense(cdef.name), columnValue);
                    AbstractType<?> validator = getValidatorMap(cfDef).get(cdef.name);
                    setTupleValue(tuple, i, cqlColumnToObj(cell, cfDef), validator);
                }
                else
                    tuple.set(i, null);
                i++;
            }
            return tuple;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    /** set read configuration settings */
    public void setLocation(String location, Job job) throws IOException
    {
        conf = job.getConfiguration();
        setLocationFromUri(location);

        if (username != null && password != null)
        {
            ConfigHelper.setInputKeyspaceUserNameAndPassword(conf, username, password);
            CqlConfigHelper.setUserNameAndPassword(conf, username, password);
        }
        if (splitSize > 0)
            ConfigHelper.setInputSplitSize(conf, splitSize);
        if (partitionerClass!= null)
            ConfigHelper.setInputPartitioner(conf, partitionerClass);
        if (initHostAddress != null)
            ConfigHelper.setInputInitialAddress(conf, initHostAddress);
        if (rpcPort != null)
            ConfigHelper.setInputRpcPort(conf, rpcPort);
        if (nativePort != null)
            CqlConfigHelper.setInputNativePort(conf, nativePort);
        if (nativeCoreConnections != null)
            CqlConfigHelper.setInputCoreConnections(conf, nativeCoreConnections);
        if (nativeMaxConnections != null)
            CqlConfigHelper.setInputMaxConnections(conf, nativeMaxConnections);
        if (nativeMinSimultReqs != null)
            CqlConfigHelper.setInputMinSimultReqPerConnections(conf, nativeMinSimultReqs);
        if (nativeMaxSimultReqs != null)
            CqlConfigHelper.setInputMaxSimultReqPerConnections(conf, nativeMaxSimultReqs);
        if (nativeConnectionTimeout != null)
            CqlConfigHelper.setInputNativeConnectionTimeout(conf, nativeConnectionTimeout);
        if (nativeReadConnectionTimeout != null)
            CqlConfigHelper.setInputNativeReadConnectionTimeout(conf, nativeReadConnectionTimeout);
        if (nativeReceiveBufferSize != null)
            CqlConfigHelper.setInputNativeReceiveBufferSize(conf, nativeReceiveBufferSize);
        if (nativeSendBufferSize != null)
            CqlConfigHelper.setInputNativeSendBufferSize(conf, nativeSendBufferSize);
        if (nativeSolinger != null)
            CqlConfigHelper.setInputNativeSolinger(conf, nativeSolinger);
        if (nativeTcpNodelay != null)
            CqlConfigHelper.setInputNativeTcpNodelay(conf, nativeTcpNodelay);
        if (nativeReuseAddress != null)
            CqlConfigHelper.setInputNativeReuseAddress(conf, nativeReuseAddress);
        if (nativeKeepAlive != null)
            CqlConfigHelper.setInputNativeKeepAlive(conf, nativeKeepAlive);
        if (nativeAuthProvider != null)
            CqlConfigHelper.setInputNativeAuthProvider(conf, nativeAuthProvider);
        if (nativeSSLTruststorePath != null)
            CqlConfigHelper.setInputNativeSSLTruststorePath(conf, nativeSSLTruststorePath);
        if (nativeSSLKeystorePath != null)
            CqlConfigHelper.setInputNativeSSLKeystorePath(conf, nativeSSLKeystorePath);
        if (nativeSSLTruststorePassword != null)
            CqlConfigHelper.setInputNativeSSLTruststorePassword(conf, nativeSSLTruststorePassword);
        if (nativeSSLKeystorePassword != null)
            CqlConfigHelper.setInputNativeSSLKeystorePassword(conf, nativeSSLKeystorePassword);
        if (nativeSSLCipherSuites != null)
            CqlConfigHelper.setInputNativeSSLCipherSuites(conf, nativeSSLCipherSuites);

        ConfigHelper.setInputColumnFamily(conf, keyspace, column_family);
        setConnectionInformation();

        CqlConfigHelper.setInputCQLPageRowSize(conf, String.valueOf(pageSize));
        if (inputCql != null)
            CqlConfigHelper.setInputCql(conf, inputCql);
        if (columns != null)
            CqlConfigHelper.setInputColumns(conf, columns);
        if (whereClause != null)
            CqlConfigHelper.setInputWhereClauses(conf, whereClause);

        if (System.getenv(PIG_INPUT_SPLIT_SIZE) != null)
        {
            try
            {
                ConfigHelper.setInputSplitSize(conf, Integer.parseInt(System.getenv(PIG_INPUT_SPLIT_SIZE)));
            }
            catch (NumberFormatException e)
            {
                throw new IOException("PIG_INPUT_SPLIT_SIZE is not a number", e);
            }           
        }

        if (ConfigHelper.getInputInitialAddress(conf) == null)
            throw new IOException("PIG_INPUT_INITIAL_ADDRESS or PIG_INITIAL_ADDRESS environment variable not set");
        if (ConfigHelper.getInputPartitioner(conf) == null)
            throw new IOException("PIG_INPUT_PARTITIONER or PIG_PARTITIONER environment variable not set");
        if (loadSignature == null)
            loadSignature = location;

        initSchema(loadSignature);
    }

    private void setLocationFromUri(String location) throws IOException
    {
        try
        {
            if (!location.startsWith("cql://"))
                throw new Exception("Bad scheme: " + location);

            String[] urlParts = location.split("\\?");
            if (urlParts.length > 1)
            {
                Map<String, String> urlQuery = getQueryMap(urlParts[1]);

                // each page row size
                if (urlQuery.containsKey("page_size"))
                    pageSize = Integer.parseInt(urlQuery.get("page_size"));

                // output prepared statement
                if (urlQuery.containsKey("output_query"))
                    outputQuery = urlQuery.get("output_query");

                //split size
                if (urlQuery.containsKey("split_size"))
                    splitSize = Integer.parseInt(urlQuery.get("split_size"));
                if (urlQuery.containsKey("partitioner"))
                    partitionerClass = urlQuery.get("partitioner");
                if (urlQuery.containsKey("use_secondary"))
                    usePartitionFilter = Boolean.parseBoolean(urlQuery.get("use_secondary"));
                if (urlQuery.containsKey("init_address"))
                    initHostAddress = urlQuery.get("init_address");

                if (urlQuery.containsKey("native_port"))
                    nativePort = urlQuery.get("native_port");
                if (urlQuery.containsKey("core_conns"))
                    nativeCoreConnections = urlQuery.get("core_conns");
                if (urlQuery.containsKey("max_conns"))
                    nativeMaxConnections = urlQuery.get("max_conns");
                if (urlQuery.containsKey("min_simult_reqs"))
                    nativeMinSimultReqs = urlQuery.get("min_simult_reqs");
                if (urlQuery.containsKey("max_simult_reqs"))
                    nativeMaxSimultReqs = urlQuery.get("max_simult_reqs");
                if (urlQuery.containsKey("native_timeout"))
                    nativeConnectionTimeout = urlQuery.get("native_timeout");
                if (urlQuery.containsKey("native_read_timeout"))
                    nativeReadConnectionTimeout = urlQuery.get("native_read_timeout");
                if (urlQuery.containsKey("rec_buff_size"))
                    nativeReceiveBufferSize = urlQuery.get("rec_buff_size");
                if (urlQuery.containsKey("send_buff_size"))
                    nativeSendBufferSize = urlQuery.get("send_buff_size");
                if (urlQuery.containsKey("solinger"))
                    nativeSolinger = urlQuery.get("solinger");
                if (urlQuery.containsKey("tcp_nodelay"))
                    nativeTcpNodelay = urlQuery.get("tcp_nodelay");
                if (urlQuery.containsKey("reuse_address"))
                    nativeReuseAddress = urlQuery.get("reuse_address");
                if (urlQuery.containsKey("keep_alive"))
                    nativeKeepAlive = urlQuery.get("keep_alive");
                if (urlQuery.containsKey("auth_provider"))
                    nativeAuthProvider = urlQuery.get("auth_provider");
                if (urlQuery.containsKey("trust_store_path"))
                    nativeSSLTruststorePath = urlQuery.get("trust_store_path");
                if (urlQuery.containsKey("key_store_path"))
                    nativeSSLKeystorePath = urlQuery.get("key_store_path");
                if (urlQuery.containsKey("trust_store_password"))
                    nativeSSLTruststorePassword = urlQuery.get("trust_store_password");
                if (urlQuery.containsKey("key_store_password"))
                    nativeSSLKeystorePassword = urlQuery.get("key_store_password");
                if (urlQuery.containsKey("cipher_suites"))
                    nativeSSLCipherSuites = urlQuery.get("cipher_suites");
                if (urlQuery.containsKey("input_cql"))
                    inputCql = urlQuery.get("input_cql");
                if (urlQuery.containsKey("columns"))
                    columns = urlQuery.get("columns");
                if (urlQuery.containsKey("where_clause"))
                    whereClause = urlQuery.get("where_clause");
                if (urlQuery.containsKey("rpc_port"))
                    rpcPort = urlQuery.get("rpc_port");
            }
            String[] parts = urlParts[0].split("/+");
            String[] credentialsAndKeyspace = parts[1].split("@");
            if (credentialsAndKeyspace.length > 1)
            {
                String[] credentials = credentialsAndKeyspace[0].split(":");
                username = credentials[0];
                password = credentials[1];
                keyspace = credentialsAndKeyspace[1];
            }
            else
            {
                keyspace = parts[1];
            }
            column_family = parts[2];
        }
        catch (Exception e)
        {
            throw new IOException("Expected 'cql://[username:password@]<keyspace>/<columnfamily>" +
                    "[?[page_size=<size>][&columns=<col1,col2>][&output_query=<prepared_statement>]" +
                    "[&where_clause=<clause>][&split_size=<size>][&partitioner=<partitioner>][&use_secondary=true|false]" +
                    "[&init_address=<host>][&native_port=<native_port>][&core_conns=<core_conns>]" +
                    "[&max_conns=<max_conns>][&min_simult_reqs=<min_simult_reqs>][&max_simult_reqs=<max_simult_reqs>]" +
                    "[&native_timeout=<native_timeout>][&native_read_timeout=<native_read_timeout>][&rec_buff_size=<rec_buff_size>]" +
                    "[&send_buff_size=<send_buff_size>][&solinger=<solinger>][&tcp_nodelay=<tcp_nodelay>][&reuse_address=<reuse_address>]" +
                    "[&keep_alive=<keep_alive>][&auth_provider=<auth_provider>][&trust_store_path=<trust_store_path>]" +
                    "[&key_store_path=<key_store_path>][&trust_store_password=<trust_store_password>]" +
                    "[&key_store_password=<key_store_password>][&cipher_suites=<cipher_suites>][&input_cql=<input_cql>]" +
                    "[columns=<columns>][where_clause=<where_clause>]]': " + e.getMessage());
        }
    }

}
