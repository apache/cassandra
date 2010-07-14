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


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public class ConfigHelper
{
    private static final String KEYSPACE_CONFIG = "cassandra.input.keyspace";
    private static final String COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";
    private static final String PREDICATE_CONFIG = "cassandra.input.predicate";
    private static final String INPUT_SPLIT_SIZE_CONFIG = "cassandra.input.split.size";
    private static final int DEFAULT_SPLIT_SIZE = 64 * 1024;
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";
    private static final int DEFAULT_RANGE_BATCH_SIZE = 4096;
    private static final String THRIFT_PORT = "cassandra.thrift.port";
    private static final String INITIAL_THRIFT_ADDRESS = "cassandra.thrift.address";
    private static final String COMPARATOR = "cassandra.input.comparator";
    private static final String SUB_COMPARATOR = "cassandra.input.subcomparator";
    private static final String PARTITIONER = "cassandra.partitioner";

    /**
     * Set the keyspace, column family, column comparator, and row partitioner for this job.
     *
     * @param conf         Job configuration you are about to run
     * @param keyspace
     * @param columnFamily
     * @param comparator
     * @param partitioner
     */
    public static void setColumnFamily(Configuration conf, String keyspace, String columnFamily, String comparator, String partitioner)
    {
        setColumnFamily(conf, keyspace, columnFamily);
        conf.set(COMPARATOR, comparator);
        conf.set(PARTITIONER, partitioner);
    }

    /**
     * Set the keyspace and column family for this job.
     * Comparator and Partitioner types will be read from storage-conf.xml.
     *
     * @param conf         Job configuration you are about to run
     * @param keyspace
     * @param columnFamily
     */
    public static void setColumnFamily(Configuration conf, String keyspace, String columnFamily)
    {
        if (keyspace == null)
        {
            throw new UnsupportedOperationException("keyspace may not be null");
        }
        if (columnFamily == null)
        {
            throw new UnsupportedOperationException("columnfamily may not be null");
        }
        try
        {
            ThriftValidation.validateColumnFamily(keyspace, columnFamily);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        conf.set(KEYSPACE_CONFIG, keyspace);
        conf.set(COLUMNFAMILY_CONFIG, columnFamily);
    }

    /**
     * Set the subcomparator to use in the configured ColumnFamily [of SuperColumns].
     * Optional when storage-conf.xml is provided.
     *
     * @param conf
     * @param subComparator
     */
    public static void setSubComparator(Configuration conf, String subComparator)
    {
        conf.set(SUB_COMPARATOR, subComparator);
    }

    /**
     * The address and port of a Cassandra node that Hadoop can contact over Thrift
     * to learn more about the Cassandra cluster.  Optional when storage-conf.xml
     * is provided.
     *
     * @param conf
     * @param address
     * @param port
     */
    public static void setThriftContact(Configuration conf, String address, int port)
    {
        conf.set(THRIFT_PORT, String.valueOf(port));
        conf.set(INITIAL_THRIFT_ADDRESS, address);
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
    public static void setSlicePredicate(Configuration conf, SlicePredicate predicate)
    {
        conf.set(PREDICATE_CONFIG, predicateToString(predicate));
    }

    public static SlicePredicate getSlicePredicate(Configuration conf)
    {
        return predicateFromString(conf.get(PREDICATE_CONFIG));
    }

    private static String predicateToString(SlicePredicate predicate)
    {
        assert predicate != null;
        // this is so awful it's kind of cool!
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            return FBUtilities.bytesToHex(serializer.serialize(predicate));
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
            deserializer.deserialize(predicate, FBUtilities.hexToBytes(st));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return predicate;
    }

    public static String getKeyspace(Configuration conf)
    {
        return conf.get(KEYSPACE_CONFIG);
    }

    public static String getColumnFamily(Configuration conf)
    {
        return conf.get(COLUMNFAMILY_CONFIG);
    }

    public static int getThriftPort(Configuration conf)
    {
        String v = conf.get(THRIFT_PORT);
        return v == null ? DatabaseDescriptor.getThriftPort() : Integer.valueOf(v);
    }

    public static String getInitialAddress(Configuration conf)
    {
        String v = conf.get(INITIAL_THRIFT_ADDRESS);
        return v == null ? DatabaseDescriptor.getSeeds().iterator().next().getHostAddress() : v;
    }

    public static AbstractType getComparator(Configuration conf)
    {
        String v = conf.get(COMPARATOR);
        return v == null
               ? DatabaseDescriptor.getComparator(getKeyspace(conf), getColumnFamily(conf))
               : DatabaseDescriptor.getComparator(v);
    }

    public static AbstractType getSubComparator(Configuration conf)
    {
        String v = conf.get(SUB_COMPARATOR);
        return v == null
               ? DatabaseDescriptor.getSubComparator(getKeyspace(conf), getColumnFamily(conf))
               : DatabaseDescriptor.getComparator(v);
    }

    public static IPartitioner getPartitioner(Configuration conf)
    {
        String v = conf.get(PARTITIONER);
        return v == null ? DatabaseDescriptor.getPartitioner() : DatabaseDescriptor.newPartitioner(v);
    }
}
