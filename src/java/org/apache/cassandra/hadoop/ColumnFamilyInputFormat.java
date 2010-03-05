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

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class ColumnFamilyInputFormat extends InputFormat<String, SortedMap<byte[], IColumn>>
{

    private static final Logger logger = Logger.getLogger(StorageService.class);

    private void validateConfiguration(Configuration conf)
    {
        if (ConfigHelper.getKeyspace(conf) == null || ConfigHelper.getColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setColumnFamily()");
        }
        if (ConfigHelper.getSlicePredicate(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the predicate with setPredicate");
        }
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = context.getConfiguration();

        validateConfiguration(conf);

        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap(ConfigHelper.getKeyspace(conf));

        int splitsize = ConfigHelper.getInputSplitSize(context.getConfiguration());
        
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
                ColumnFamilySplit split = new ColumnFamilySplit(tokens.get(i - 1), tokens.get(i), endpoints);
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

    private List<TokenRange> getRangeMap(String keyspace) throws IOException
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
