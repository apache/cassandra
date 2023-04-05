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
package org.apache.cassandra.hadoop;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;

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
    private static final String INPUT_SPLIT_SIZE_IN_MIB_CONFIG = "cassandra.input.split.size_mb";
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final int DEFAULT_SPLIT_SIZE = 64 * 1024;
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";
    private static final int DEFAULT_RANGE_BATCH_SIZE = 4096;
    private static final String INPUT_INITIAL_ADDRESS = "cassandra.input.address";
    private static final String OUTPUT_INITIAL_ADDRESS = "cassandra.output.address";
    private static final String OUTPUT_INITIAL_PORT = "cassandra.output.port";
    private static final String READ_CONSISTENCY_LEVEL = "cassandra.consistencylevel.read";
    private static final String WRITE_CONSISTENCY_LEVEL = "cassandra.consistencylevel.write";
    private static final String OUTPUT_COMPRESSION_CLASS = "cassandra.output.compression.class";
    private static final String OUTPUT_COMPRESSION_CHUNK_LENGTH = "cassandra.output.compression.length";
    private static final String OUTPUT_LOCAL_DC_ONLY = "cassandra.output.local.dc.only";
    private static final String DEFAULT_CASSANDRA_NATIVE_PORT = "7000";

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
     * @param splitsize Number of partitions in the input split
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
     * Set the size of the input split. setInputSplitSize value is used if this is not set.
     * This affects the number of maps created, if the number is too small
     * the overhead of each map will take up the bulk of the job time.
     *
     * @param conf          Job configuration you are about to run
     * @param splitSizeMb   Input split size in MiB
     */
    public static void setInputSplitSizeInMb(Configuration conf, int splitSizeMb)
    {
        conf.setInt(INPUT_SPLIT_SIZE_IN_MIB_CONFIG, splitSizeMb);
    }

    /**
     * cassandra.input.split.size will be used if the value is undefined or negative.
     * @param conf  Job configuration you are about to run
     * @return      split size in MiB or -1 if it is undefined.
     */
    public static int getInputSplitSizeInMb(Configuration conf)
    {
        return conf.getInt(INPUT_SPLIT_SIZE_IN_MIB_CONFIG, -1);
    }

    /**
     * Set the KeyRange to limit the rows.
     * @param conf Job configuration you are about to run
     */
    public static void setInputRange(Configuration conf, String startToken, String endToken)
    {
        conf.set(INPUT_KEYRANGE_CONFIG, startToken + "," + endToken);
    }

    /**
     * The start and end token of the input key range as a pair.
     *
     * may be null if unset.
     */
    public static Pair<String, String> getInputKeyRange(Configuration conf)
    {
        String str = conf.get(INPUT_KEYRANGE_CONFIG);
        if (str == null)
            return null;

        String[] parts = str.split(",");
        assert parts.length == 2;
        return Pair.create(parts[0], parts[1]);
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

    public static String getInputInitialAddress(Configuration conf)
    {
        return conf.get(INPUT_INITIAL_ADDRESS);
    }

    public static void setInputInitialAddress(Configuration conf, String address)
    {
        conf.set(INPUT_INITIAL_ADDRESS, address);
    }
    public static void setInputPartitioner(Configuration conf, String classname)
    {
        conf.set(INPUT_PARTITIONER_CONFIG, classname);
    }

    public static IPartitioner getInputPartitioner(Configuration conf)
    {
        return FBUtilities.newPartitioner(conf.get(INPUT_PARTITIONER_CONFIG));
    }

    public static String getOutputInitialAddress(Configuration conf)
    {
        return conf.get(OUTPUT_INITIAL_ADDRESS);
    }

    public static void setOutputInitialPort(Configuration conf, Integer port)
    {
        conf.set(OUTPUT_INITIAL_PORT, port.toString());
    }

    public static Integer getOutputInitialPort(Configuration conf)
    {
        return Integer.valueOf(conf.get(OUTPUT_INITIAL_PORT, DEFAULT_CASSANDRA_NATIVE_PORT));
    }

    public static void setOutputInitialAddress(Configuration conf, String address)
    {
        conf.set(OUTPUT_INITIAL_ADDRESS, address);
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
        return conf.get(OUTPUT_COMPRESSION_CHUNK_LENGTH, String.valueOf(CompressionParams.DEFAULT_CHUNK_LENGTH));
    }

    public static void setOutputCompressionClass(Configuration conf, String classname)
    {
        conf.set(OUTPUT_COMPRESSION_CLASS, classname);
    }

    public static void setOutputCompressionChunkLength(Configuration conf, String length)
    {
        conf.set(OUTPUT_COMPRESSION_CHUNK_LENGTH, length);
    }

    public static boolean getOutputLocalDCOnly(Configuration conf)
    {
        return Boolean.parseBoolean(conf.get(OUTPUT_LOCAL_DC_ONLY, "false"));
    }

    public static void setOutputLocalDCOnly(Configuration conf, boolean localDCOnly)
    {
        conf.set(OUTPUT_LOCAL_DC_ONLY, Boolean.toString(localDCOnly));
    }
}
