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

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

public class ConfigHelper
{
    private static final String INPUT_PARTITIONER_CONFIG = "cassandra.input.partitioner.class";
    private static final String OUTPUT_PARTITIONER_CONFIG = "cassandra.output.partitioner.class";
    private static final String INPUT_KEYSPACE_CONFIG = "cassandra.input.keyspace";
    private static final String OUTPUT_KEYSPACE_CONFIG = "cassandra.output.keyspace";
    private static final String INPUT_KEYSPACE_USERNAME_CONFIG = "cassandra.input.keyspace.username";
    private static final String INPUT_KEYSPACE_PASSWD_CONFIG = "cassandra.input.keyspace.passwd";
    private static final String OUTPUT_KEYSPACE_USERNAME_CONFIG = "cassandra.output.keyspace.username";
    private static final String OUTPUT_KEYSPACE_PASSWD_CONFIG = "cassandra.output.keyspace.passwd";
    private static final String INPUT_COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";
    private static final String OUTPUT_COLUMNFAMILY_CONFIG = "mapreduce.output.basename"; //this must == OutputFormat.BASE_OUTPUT_NAME
    private static final String INPUT_PREDICATE_CONFIG = "cassandra.input.predicate";
    private static final String INPUT_KEYRANGE_CONFIG = "cassandra.input.keyRange";
    private static final String INPUT_SPLIT_SIZE_CONFIG = "cassandra.input.split.size";
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final int DEFAULT_SPLIT_SIZE = 64 * 1024;
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";
    private static final int DEFAULT_RANGE_BATCH_SIZE = 4096;
    private static final String INPUT_THRIFT_PORT = "cassandra.input.thrift.port";
    private static final String OUTPUT_THRIFT_PORT = "cassandra.output.thrift.port";
    private static final String INPUT_INITIAL_THRIFT_ADDRESS = "cassandra.input.thrift.address";
    private static final String OUTPUT_INITIAL_THRIFT_ADDRESS = "cassandra.output.thrift.address";
    private static final String READ_CONSISTENCY_LEVEL = "cassandra.consistencylevel.read";
    private static final String WRITE_CONSISTENCY_LEVEL = "cassandra.consistencylevel.write";
    private static final String OUTPUT_COMPRESSION_CLASS = "cassandra.output.compression.class";
    private static final String OUTPUT_COMPRESSION_CHUNK_LENGTH = "cassandra.output.compression.length";
    private static final String OUTPUT_LOCAL_DC_ONLY = "cassandra.output.local.dc.only";
    private static final String THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB = "cassandra.thrift.framed.size_mb";

    private static final Logger logger = LoggerFactory.getLogger(ConfigHelper.class);

    /**
     * Set the keyspace and column family for the input of this job.
     *
     * @param conf         Job configuration you are about to run
     * @param keyspace
     * @param columnFamily
     * @param widerows
     */
    public static void setInputColumnFamily(Configuration conf, String keyspace, String columnFamily, boolean widerows)
    {
        if (keyspace == null)
            throw new UnsupportedOperationException("keyspace may not be null");

        if (columnFamily == null)
            throw new UnsupportedOperationException("table may not be null");

        conf.set(INPUT_KEYSPACE_CONFIG, keyspace);
        conf.set(INPUT_COLUMNFAMILY_CONFIG, columnFamily);
        conf.set(INPUT_WIDEROWS_CONFIG, String.valueOf(widerows));
    }

    /**
     * Set the keyspace and column family for the input of this job.
     *
     * @param conf         Job configuration you are about to run
     * @param keyspace
     * @param columnFamily
     */
    public static void setInputColumnFamily(Configuration conf, String keyspace, String columnFamily)
    {
        setInputColumnFamily(conf, keyspace, columnFamily, false);
    }

    /**
     * Set the keyspace for the output of this job.
     *
     * @param conf Job configuration you are about to run
     * @param keyspace
     */
    public static void setOutputKeyspace(Configuration conf, String keyspace)
    {
        if (keyspace == null)
            throw new UnsupportedOperationException("keyspace may not be null");

        conf.set(OUTPUT_KEYSPACE_CONFIG, keyspace);
    }

    /**
     * Set the column family for the output of this job.
     *
     * @param conf         Job configuration you are about to run
     * @param columnFamily
     */
    public static void setOutputColumnFamily(Configuration conf, String columnFamily)
    {
    	conf.set(OUTPUT_COLUMNFAMILY_CONFIG, columnFamily);
    }

    /**
     * Set the column family for the output of this job.
     *
     * @param conf         Job configuration you are about to run
     * @param keyspace
     * @param columnFamily
     */
    public static void setOutputColumnFamily(Configuration conf, String keyspace, String columnFamily)
    {
    	setOutputKeyspace(conf, keyspace);
    	setOutputColumnFamily(conf, columnFamily);
    }

    /**
     * The number of rows to request with each get range slices request.
     * Too big and you can either get timeouts when it takes Cassandra too
     * long to fetch all the data. Too small and the performance
     * will be eaten up by the overhead of each request.
     *
     * @param conf      Job configuration you are about to run
     * @param batchsize Number of rows to request each time
     */
    public static void setRangeBatchSize(Configuration conf, int batchsize)
    {
        conf.setInt(RANGE_BATCH_SIZE_CONFIG, batchsize);
    }

    /**
     * The number of rows to request with each get range slices request.
     * Too big and you can either get timeouts when it takes Cassandra too
     * long to fetch all the data. Too small and the performance
     * will be eaten up by the overhead of each request.
     *
     * @param conf Job configuration you are about to run
     * @return Number of rows to request each time
     */
    public static int getRangeBatchSize(Configuration conf)
    {
        return conf.getInt(RANGE_BATCH_SIZE_CONFIG, DEFAULT_RANGE_BATCH_SIZE);
    }

    /**
     * Set the size of the input split.
     * This affects the number of maps created, if the number is too small
     * the overhead of each map will take up the bulk of the job time.
     *
     * @param conf      Job configuration you are about to run
     * @param splitsize Size of the input split
     */
    public static void setInputSplitSize(Configuration conf, int splitsize)
    {
        conf.setInt(INPUT_SPLIT_SIZE_CONFIG, splitsize);
    }

    public static int getInputSplitSize(Configuration conf)
    {
        return conf.getInt(INPUT_SPLIT_SIZE_CONFIG, DEFAULT_SPLIT_SIZE);
    }

    /**
     * Set the predicate that determines what columns will be selected from each row.
     *
     * @param conf      Job configuration you are about to run
     * @param predicate
     */
    public static void setInputSlicePredicate(Configuration conf, SlicePredicate predicate)
    {
        conf.set(INPUT_PREDICATE_CONFIG, thriftToString(predicate));
    }

    public static SlicePredicate getInputSlicePredicate(Configuration conf)
    {
        String s = conf.get(INPUT_PREDICATE_CONFIG);
        return s == null ? null : predicateFromString(s);
    }

    private static String thriftToString(TBase object)
    {
        assert object != null;
        // this is so awful it's kind of cool!
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            return Hex.bytesToHex(serializer.serialize(object));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static SlicePredicate predicateFromString(String st)
    {
        assert st != null;
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        SlicePredicate predicate = new SlicePredicate();
        try
        {
            deserializer.deserialize(predicate, Hex.hexToBytes(st));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return predicate;
    }

    /**
     * Set the KeyRange to limit the rows.
     * @param conf Job configuration you are about to run
     */
    public static void setInputRange(Configuration conf, String startToken, String endToken)
    {
        KeyRange range = new KeyRange().setStart_token(startToken).setEnd_token(endToken);
        conf.set(INPUT_KEYRANGE_CONFIG, thriftToString(range));
    }

    /**
     * Set the KeyRange to limit the rows.
     * @param conf Job configuration you are about to run
     */
    public static void setInputRange(Configuration conf, String startToken, String endToken, List<IndexExpression> filter)
    {
        KeyRange range = new KeyRange().setStart_token(startToken).setEnd_token(endToken).setRow_filter(filter);
        conf.set(INPUT_KEYRANGE_CONFIG, thriftToString(range));
    }

    /**
     * Set the KeyRange to limit the rows.
     * @param conf Job configuration you are about to run
     */
    public static void setInputRange(Configuration conf, List<IndexExpression> filter)
    {
        KeyRange range = new KeyRange().setRow_filter(filter);
        conf.set(INPUT_KEYRANGE_CONFIG, thriftToString(range));
    }

    /** may be null if unset */
    public static KeyRange getInputKeyRange(Configuration conf)
    {
        String str = conf.get(INPUT_KEYRANGE_CONFIG);
        return str == null ? null : keyRangeFromString(str);
    }

    private static KeyRange keyRangeFromString(String st)
    {
        assert st != null;
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        KeyRange keyRange = new KeyRange();
        try
        {
            deserializer.deserialize(keyRange, Hex.hexToBytes(st));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return keyRange;
    }

    public static String getInputKeyspace(Configuration conf)
    {
        return conf.get(INPUT_KEYSPACE_CONFIG);
    }

    public static String getOutputKeyspace(Configuration conf)
    {
        return conf.get(OUTPUT_KEYSPACE_CONFIG);
    }

    public static void setInputKeyspaceUserNameAndPassword(Configuration conf, String username, String password)
    {
        setInputKeyspaceUserName(conf, username);
        setInputKeyspacePassword(conf, password);
    }

    public static void setInputKeyspaceUserName(Configuration conf, String username)
    {
        conf.set(INPUT_KEYSPACE_USERNAME_CONFIG, username);
    }

    public static String getInputKeyspaceUserName(Configuration conf)
    {
    	return conf.get(INPUT_KEYSPACE_USERNAME_CONFIG);
    }

    public static void setInputKeyspacePassword(Configuration conf, String password)
    {
        conf.set(INPUT_KEYSPACE_PASSWD_CONFIG, password);
    }

    public static String getInputKeyspacePassword(Configuration conf)
    {
    	return conf.get(INPUT_KEYSPACE_PASSWD_CONFIG);
    }

    public static void setOutputKeyspaceUserNameAndPassword(Configuration conf, String username, String password)
    {
        setOutputKeyspaceUserName(conf, username);
        setOutputKeyspacePassword(conf, password);
    }

    public static void setOutputKeyspaceUserName(Configuration conf, String username)
    {
        conf.set(OUTPUT_KEYSPACE_USERNAME_CONFIG, username);
    }

    public static String getOutputKeyspaceUserName(Configuration conf)
    {
    	return conf.get(OUTPUT_KEYSPACE_USERNAME_CONFIG);
    }

    public static void setOutputKeyspacePassword(Configuration conf, String password)
    {
        conf.set(OUTPUT_KEYSPACE_PASSWD_CONFIG, password);
    }

    public static String getOutputKeyspacePassword(Configuration conf)
    {
    	return conf.get(OUTPUT_KEYSPACE_PASSWD_CONFIG);
    }

    public static String getInputColumnFamily(Configuration conf)
    {
        return conf.get(INPUT_COLUMNFAMILY_CONFIG);
    }

    public static String getOutputColumnFamily(Configuration conf)
    {
    	if (conf.get(OUTPUT_COLUMNFAMILY_CONFIG) != null)
    		return conf.get(OUTPUT_COLUMNFAMILY_CONFIG);
    	else
    		throw new UnsupportedOperationException("You must set the output column family using either setOutputColumnFamily or by adding a named output with MultipleOutputs");
    }

    public static boolean getInputIsWide(Configuration conf)
    {
        return Boolean.parseBoolean(conf.get(INPUT_WIDEROWS_CONFIG));
    }

    public static String getReadConsistencyLevel(Configuration conf)
    {
        return conf.get(READ_CONSISTENCY_LEVEL, "LOCAL_ONE");
    }

    public static void setReadConsistencyLevel(Configuration conf, String consistencyLevel)
    {
        conf.set(READ_CONSISTENCY_LEVEL, consistencyLevel);
    }

    public static String getWriteConsistencyLevel(Configuration conf)
    {
        return conf.get(WRITE_CONSISTENCY_LEVEL, "LOCAL_ONE");
    }

    public static void setWriteConsistencyLevel(Configuration conf, String consistencyLevel)
    {
        conf.set(WRITE_CONSISTENCY_LEVEL, consistencyLevel);
    }

    public static int getInputRpcPort(Configuration conf)
    {
        return Integer.parseInt(conf.get(INPUT_THRIFT_PORT, "9160"));
    }

    public static void setInputRpcPort(Configuration conf, String port)
    {
        conf.set(INPUT_THRIFT_PORT, port);
    }

    public static String getInputInitialAddress(Configuration conf)
    {
        return conf.get(INPUT_INITIAL_THRIFT_ADDRESS);
    }

    public static void setInputInitialAddress(Configuration conf, String address)
    {
        conf.set(INPUT_INITIAL_THRIFT_ADDRESS, address);
    }

    public static void setInputPartitioner(Configuration conf, String classname)
    {
        conf.set(INPUT_PARTITIONER_CONFIG, classname);
    }

    public static IPartitioner getInputPartitioner(Configuration conf)
    {
        return FBUtilities.newPartitioner(conf.get(INPUT_PARTITIONER_CONFIG));
    }

    public static int getOutputRpcPort(Configuration conf)
    {
        return Integer.parseInt(conf.get(OUTPUT_THRIFT_PORT, "9160"));
    }

    public static void setOutputRpcPort(Configuration conf, String port)
    {
        conf.set(OUTPUT_THRIFT_PORT, port);
    }

    public static String getOutputInitialAddress(Configuration conf)
    {
        return conf.get(OUTPUT_INITIAL_THRIFT_ADDRESS);
    }

    public static void setOutputInitialAddress(Configuration conf, String address)
    {
        conf.set(OUTPUT_INITIAL_THRIFT_ADDRESS, address);
    }

    public static void setOutputPartitioner(Configuration conf, String classname)
    {
        conf.set(OUTPUT_PARTITIONER_CONFIG, classname);
    }

    public static IPartitioner getOutputPartitioner(Configuration conf)
    {
        return FBUtilities.newPartitioner(conf.get(OUTPUT_PARTITIONER_CONFIG));
    }

    public static String getOutputCompressionClass(Configuration conf)
    {
        return conf.get(OUTPUT_COMPRESSION_CLASS);
    }

    public static String getOutputCompressionChunkLength(Configuration conf)
    {
        return conf.get(OUTPUT_COMPRESSION_CHUNK_LENGTH, String.valueOf(CompressionParameters.DEFAULT_CHUNK_LENGTH));
    }

    public static void setOutputCompressionClass(Configuration conf, String classname)
    {
        conf.set(OUTPUT_COMPRESSION_CLASS, classname);
    }

    public static void setOutputCompressionChunkLength(Configuration conf, String length)
    {
        conf.set(OUTPUT_COMPRESSION_CHUNK_LENGTH, length);
    }

    public static void setThriftFramedTransportSizeInMb(Configuration conf, int frameSizeInMB)
    {
        conf.setInt(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB, frameSizeInMB);
    }

    /**
     * @param conf The configuration to use.
     * @return Value (converts MBs to Bytes) set by {@link #setThriftFramedTransportSizeInMb(Configuration, int)} or default of 15MB
     */
    public static int getThriftFramedTransportSize(Configuration conf)
    {
        return conf.getInt(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB, 15) * 1024 * 1024; // 15MB is default in Cassandra
    }

    public static CompressionParameters getOutputCompressionParamaters(Configuration conf)
    {
        if (getOutputCompressionClass(conf) == null)
            return new CompressionParameters(null);

        Map<String, String> options = new HashMap<String, String>(2);
        options.put(CompressionParameters.SSTABLE_COMPRESSION, getOutputCompressionClass(conf));
        options.put(CompressionParameters.CHUNK_LENGTH_KB, getOutputCompressionChunkLength(conf));

        return CompressionParameters.create(options);
    }

    public static boolean getOutputLocalDCOnly(Configuration conf)
    {
        return Boolean.parseBoolean(conf.get(OUTPUT_LOCAL_DC_ONLY, "false"));
    }

    public static void setOutputLocalDCOnly(Configuration conf, boolean localDCOnly)
    {
        conf.set(OUTPUT_LOCAL_DC_ONLY, Boolean.toString(localDCOnly));
    }

    public static Cassandra.Client getClientFromInputAddressList(Configuration conf) throws IOException
    {
        return getClientFromAddressList(conf, ConfigHelper.getInputInitialAddress(conf).split(","), ConfigHelper.getInputRpcPort(conf));
    }

    public static Cassandra.Client getClientFromOutputAddressList(Configuration conf) throws IOException
    {
        return getClientFromAddressList(conf, ConfigHelper.getOutputInitialAddress(conf).split(","), ConfigHelper.getOutputRpcPort(conf));
    }

    private static Cassandra.Client getClientFromAddressList(Configuration conf, String[] addresses, int port) throws IOException
    {
        Cassandra.Client client = null;
        List<IOException> exceptions = new ArrayList<IOException>();
        for (String address : addresses)
        {
            try
            {
                client = createConnection(conf, address, port);
                break;
            }
            catch (IOException ioe)
            {
                exceptions.add(ioe);
            }
        }
        if (client == null)
        {
            logger.error("failed to connect to any initial addresses");
            for (IOException ioe : exceptions)
            {
                logger.error("", ioe);
            }
            throw exceptions.get(exceptions.size() - 1);
        }
        return client;
    }

    public static Cassandra.Client createConnection(Configuration conf, String host, Integer port) throws IOException
    {
        try
        {
            TTransport transport = getClientTransportFactory(conf).openTransport(host, port);
            return new Cassandra.Client(new TBinaryProtocol(transport, true, true));
        }
        catch (Exception e)
        {
            throw new IOException("Unable to connect to server " + host + ":" + port, e);
        }
    }

    public static ITransportFactory getClientTransportFactory(Configuration conf)
    {
        String factoryClassName = conf.get(ITransportFactory.PROPERTY_KEY, TFramedTransportFactory.class.getName());
        ITransportFactory factory = getClientTransportFactory(factoryClassName);
        Map<String, String> options = getOptions(conf, factory.supportedOptions());
        factory.setOptions(options);
        return factory;
    }

    private static ITransportFactory getClientTransportFactory(String factoryClassName)
    {
        try
        {
            return (ITransportFactory) Class.forName(factoryClassName).newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to instantiate transport factory:" + factoryClassName, e);
        }
    }

    private static Map<String, String> getOptions(Configuration conf, Set<String> supportedOptions)
    {
        Map<String, String> options = new HashMap<>();
        for (String optionKey : supportedOptions)
        {
            String optionValue = conf.get(optionKey);
            if (optionValue != null)
                options.put(optionKey, optionValue);
        }
        return options;
    }
}
