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
import java.net.InetAddress;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class ColumnFamilyInputFormat extends InputFormat<String, SortedMap<byte[], IColumn>>
{
    private static final String KEYSPACE_CONFIG = "cassandra.input.keyspace";
    private static final String COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";
    private static final String PREDICATE_CONFIG = "cassandra.input.predicate";
    private static final String INPUT_SPLIT_SIZE_CONFIG = "cassandra.input.split.size";

    private static final Logger logger = Logger.getLogger(StorageService.class);

    private String keyspace;
    private String columnFamily;
    private SlicePredicate predicate;

    public static void setColumnFamily(Job job, String keyspace, String columnFamily)
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
        Configuration conf = job.getConfiguration();
        conf.set(KEYSPACE_CONFIG, keyspace);
        conf.set(COLUMNFAMILY_CONFIG, columnFamily);
    }

    /**
     * Set the size of the input split.
     * This affects the number of maps created, if the number is too small
     * the overhead of each map will take up the bulk of the job time.
     *  
     * @param job Job you are about to run.
     * @param splitsize Size of the input split
     */
    public static void setInputSplitSize(Job job, int splitsize)
    {
        job.getConfiguration().setInt(INPUT_SPLIT_SIZE_CONFIG, splitsize);
    }
    
    public static void setSlicePredicate(Job job, SlicePredicate predicate)
    {
        Configuration conf = job.getConfiguration();
        conf.set(PREDICATE_CONFIG, predicateToString(predicate));
    }

    private static String predicateToString(SlicePredicate predicate)
    {
        assert predicate != null;
        // this is so awful it's kind of cool!
        TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
        try
        {
            return serializer.toString(predicate, "UTF-8");
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }

    private SlicePredicate predicateFromString(String st)
    {
        assert st != null;
        TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
        SlicePredicate predicate = new SlicePredicate();
        try
        {
            deserializer.deserialize(predicate, st, "UTF-8");
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return predicate;
    }

    private void validateConfiguration()
    {
        if (keyspace == null || columnFamily == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setColumnFamily()");
        }
        if (predicate == null)
        {
            throw new UnsupportedOperationException("you must set the predicate with setPredicate");
        }
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = context.getConfiguration();
        keyspace = conf.get(KEYSPACE_CONFIG);
        columnFamily = conf.get(COLUMNFAMILY_CONFIG);
        predicate = predicateFromString(conf.get(PREDICATE_CONFIG));
        validateConfiguration();

        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap();

        int splitsize = context.getConfiguration().getInt(INPUT_SPLIT_SIZE_CONFIG, 16384);
        
        // cannonical ranges, split into pieces:
        // for each range, pick a live owner and ask it to compute bite-sized splits
        // TODO parallelize this thread-per-range
        Map<TokenRange, List<String>> splitRanges = new HashMap<TokenRange, List<String>>();
        for (TokenRange range : masterRangeNodes)
        {
            splitRanges.put(range, getSubSplits(range, splitsize));
        }

        // turn the sub-ranges into InputSplits
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for (Map.Entry<TokenRange, List<String>> entry : splitRanges.entrySet())
        {
            TokenRange range = entry.getKey();
            List<String> tokens = entry.getValue();
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);

            int i = 1;
            for ( ; i < tokens.size(); i++)
            {
                ColumnFamilySplit split = new ColumnFamilySplit(keyspace, columnFamily, predicate, tokens.get(i - 1), tokens.get(i), endpoints);
                logger.debug("adding " + split);
                splits.add(split);
            }
        }
        assert splits.size() > 0;
        
        return splits;
    }

    private List<String> getSubSplits(TokenRange range, int splitsize) throws IOException
    {
        // TODO handle failure of range replicas & retry
        TSocket socket = new TSocket(range.endpoints.get(0),
                                     DatabaseDescriptor.getThriftPort());
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(socket, false, false);
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);
        try
        {
            socket.open();
        }
        catch (TTransportException e)
        {
            throw new IOException(e);
        }
        List<String> splits;
        try
        {
            splits = client.describe_splits(range.start_token, range.end_token, splitsize);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return splits;
    }

    private List<TokenRange> getRangeMap() throws IOException
    {
        TSocket socket = new TSocket(DatabaseDescriptor.getSeeds().iterator().next().getHostAddress(),
                                     DatabaseDescriptor.getThriftPort());
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(socket, false, false);
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);
        try
        {
            socket.open();
        }
        catch (TTransportException e)
        {
            throw new IOException(e);
        }
        List<TokenRange> map;
        try
        {
            map = client.describe_ring(keyspace);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return map;
    }

    @Override
    public RecordReader<String, SortedMap<byte[], IColumn>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new ColumnFamilyRecordReader();
    }
}
