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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 *
 * At minimum, you need to set the CF and predicate (description of columns to extract from each row)
 * in your Hadoop job Configuration.  The ConfigHelper class is provided to make this
 * simple:
 *   ConfigHelper.setColumnFamily
 *   ConfigHelper.setSlicePredicate
 *
 * You can also configure the number of rows per InputSplit with
 *   ConfigHelper.setInputSplitSize
 * This should be "as big as possible, but no bigger."  Each InputSplit is read from Cassandra
 * with multiple get_slice_range queries, and the per-call overhead of get_slice_range is high,
 * so larger split sizes are better -- but if it is too large, you will run out of memory.
 *
 * The default split size is 64k rows.
 */
public class ColumnFamilyInputFormat extends InputFormat<ByteBuffer, SortedMap<ByteBuffer, IColumn>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyInputFormat.class);

    private String keyspace;
    private String cfName;

    private static void validateConfiguration(Configuration conf)
    {
        if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setColumnFamily()");
        }
        if (ConfigHelper.getInputSlicePredicate(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the predicate with setPredicate");
        }
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = context.getConfiguration();

        validateConfiguration(conf);

        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap(conf);

        keyspace = ConfigHelper.getInputKeyspace(context.getConfiguration());
        cfName = ConfigHelper.getInputColumnFamily(context.getConfiguration());

        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = Executors.newCachedThreadPool();
        List<InputSplit> splits = new ArrayList<InputSplit>();

        try
        {
            List<Future<List<InputSplit>>> splitfutures = new ArrayList<Future<List<InputSplit>>>();
            KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            IPartitioner partitioner = null;
            Range jobRange = null;
            if (jobKeyRange != null)
            {
                partitioner = ConfigHelper.getPartitioner(context.getConfiguration());
                assert partitioner.preservesOrder() : "ConfigHelper.setInputKeyRange(..) can only be used with a order preserving paritioner";
                assert jobKeyRange.start_key == null : "only start_token supported";
                assert jobKeyRange.end_key == null : "only end_token supported";
                jobRange = new Range(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
                                     partitioner.getTokenFactory().fromString(jobKeyRange.end_token),
                                     partitioner);
            }

            for (TokenRange range : masterRangeNodes)
            {
                if (jobRange == null)
                {
                    // for each range, pick a live owner and ask it to compute bite-sized splits
                    splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                }
                else
                {
                    Range dhtRange = new Range(partitioner.getTokenFactory().fromString(range.start_token),
                                               partitioner.getTokenFactory().fromString(range.end_token),
                                               partitioner);

                    if (dhtRange.intersects(jobRange))
                    {
                        Set<Range> intersections = dhtRange.intersectionWith(jobRange);
                        assert intersections.size() == 1 : "wrapping ranges not yet supported";
                        Range intersection = intersections.iterator().next();
                        range.start_token = partitioner.getTokenFactory().toString(intersection.left);
                        range.end_token = partitioner.getTokenFactory().toString(intersection.right);
                        // for each range, pick a live owner and ask it to compute bite-sized splits
                        splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                    }
                }
            }

            // wait until we have all the results back
            for (Future<List<InputSplit>> futureInputSplits : splitfutures)
            {
                try
                {
                    splits.addAll(futureInputSplits.get());
                }
                catch (Exception e)
                {
                    throw new IOException("Could not get input splits", e);
                }
            }
        }
        finally
        {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<InputSplit>>
    {

        private final TokenRange range;
        private final Configuration conf;

        public SplitCallable(TokenRange tr, Configuration conf)
        {
            this.range = tr;
            this.conf = conf;
        }

        public List<InputSplit> call() throws Exception
        {
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            List<String> tokens = getSubSplits(keyspace, cfName, range, conf);

            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            for (int i = 0; i < endpoints.length; i++)
            {
                endpoints[i] = InetAddress.getByName(endpoints[i]).getHostName();
            }

            for (int i = 1; i < tokens.size(); i++)
            {
                ColumnFamilySplit split = new ColumnFamilySplit(tokens.get(i - 1), tokens.get(i), endpoints);
                logger.debug("adding " + split);
                splits.add(split);
            }
            return splits;
        }
    }

    private List<String> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf) throws IOException
    {
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        for (String host : range.endpoints)
        {
            try
            {
                Cassandra.Client client = createConnection(host, ConfigHelper.getRpcPort(conf), true);
                client.set_keyspace(keyspace);
                return client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
            }
            catch (IOException e)
            {
                logger.debug("failed connect to endpoint " + host, e);
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("failed connecting to all endpoints " + StringUtils.join(range.endpoints, ","));
    }

    private static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws IOException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        try
        {
            trans.open();
        }
        catch (TTransportException e)
        {
            throw new IOException("unable to connect to server", e);
        }
        return new Cassandra.Client(new TBinaryProtocol(trans));
    }

    private List<TokenRange> getRangeMap(Configuration conf) throws IOException
    {
        String[] addresses = ConfigHelper.getInitialAddress(conf).split(",");
        Cassandra.Client client = null;
        List<IOException> exceptions = new ArrayList<IOException>();
        for (String address : addresses)
        {
            try
            {
                client = createConnection(address, ConfigHelper.getRpcPort(conf), true);
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

        List<TokenRange> map;
        try
        {
            map = client.describe_ring(ConfigHelper.getInputKeyspace(conf));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        return map;
    }

    public RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new ColumnFamilyRecordReader();
    }
}
