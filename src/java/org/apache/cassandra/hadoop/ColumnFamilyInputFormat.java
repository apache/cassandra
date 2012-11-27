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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TException;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 *
 * At minimum, you need to set the CF and predicate (description of columns to extract from each row)
 * in your Hadoop job Configuration.  The ConfigHelper class is provided to make this
 * simple:
 *   ConfigHelper.setInputColumnFamily
 *   ConfigHelper.setInputSlicePredicate
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
    implements org.apache.hadoop.mapred.InputFormat<ByteBuffer, SortedMap<ByteBuffer, IColumn>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyInputFormat.class);

    public static final String MAPRED_TASK_ID = "mapred.task.id";
    // The simple fact that we need this is because the old Hadoop API wants us to "write"
    // to the key and value whereas the new asks for it.
    // I choose 8kb as the default max key size (instanciated only once), but you can
    // override it in your jobConf with this setting.
    public static final String CASSANDRA_HADOOP_MAX_KEY_SIZE = "cassandra.hadoop.max_key_size";
    public static final int    CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

    private String keyspace;
    private String cfName;
    private IPartitioner partitioner;

    private static void validateConfiguration(Configuration conf)
    {
        if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setInputColumnFamily()");
        }
        if (ConfigHelper.getInputSlicePredicate(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the predicate with setInputSlicePredicate");
        }
        if (ConfigHelper.getInputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node with setInputInitialAddress");
        if (ConfigHelper.getInputPartitioner(conf) == null)
            throw new UnsupportedOperationException("You must set the Cassandra partitioner class with setInputPartitioner");
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = context.getConfiguration();

        validateConfiguration(conf);

        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap(conf);

        keyspace = ConfigHelper.getInputKeyspace(context.getConfiguration());
        cfName = ConfigHelper.getInputColumnFamily(context.getConfiguration());
        partitioner = ConfigHelper.getInputPartitioner(context.getConfiguration());
        logger.debug("partitioner is " + partitioner);

        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = Executors.newCachedThreadPool();
        List<InputSplit> splits = new ArrayList<InputSplit>();

        try
        {
            List<Future<List<InputSplit>>> splitfutures = new ArrayList<Future<List<InputSplit>>>();
            KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            Range<Token> jobRange = null;
            if (jobKeyRange != null && jobKeyRange.start_token != null)
            {
                assert partitioner.preservesOrder() : "ConfigHelper.setInputKeyRange(..) can only be used with a order preserving paritioner";
                assert jobKeyRange.start_key == null : "only start_token supported";
                assert jobKeyRange.end_key == null : "only end_token supported";
                jobRange = new Range<Token>(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
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
                    Range<Token> dhtRange = new Range<Token>(partitioner.getTokenFactory().fromString(range.start_token),
                                                             partitioner.getTokenFactory().fromString(range.end_token),
                                                             partitioner);

                    if (dhtRange.intersects(jobRange))
                    {
                        for (Range<Token> intersection: dhtRange.intersectionWith(jobRange))
                        {
                            range.start_token = partitioner.getTokenFactory().toString(intersection.left);
                            range.end_token = partitioner.getTokenFactory().toString(intersection.right);
                            // for each range, pick a live owner and ask it to compute bite-sized splits
                            splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                        }
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
            List<CfSplit> subSplits = getSubSplits(keyspace, cfName, range, conf);
            assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";
            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            int endpointIndex = 0;
            for (String endpoint: range.rpc_endpoints)
            {
                String endpoint_address = endpoint;
                if (endpoint_address == null || endpoint_address.equals("0.0.0.0"))
                    endpoint_address = range.endpoints.get(endpointIndex);
                endpoints[endpointIndex++] = InetAddress.getByName(endpoint_address).getHostName();
            }

            Token.TokenFactory factory = partitioner.getTokenFactory();
            for (CfSplit subSplit : subSplits)
            {
                Token left = factory.fromString(subSplit.getStart_token());
                Token right = factory.fromString(subSplit.getEnd_token());
                Range<Token> range = new Range<Token>(left, right, partitioner);
                List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);
                for (Range<Token> subrange : ranges)
                {
                    ColumnFamilySplit split =
                            new ColumnFamilySplit(
                                    factory.toString(subrange.left),
                                    factory.toString(subrange.right),
                                    subSplit.getRow_count(),
                                    endpoints);

                    logger.debug("adding " + split);
                    splits.add(split);
                }
            }
            return splits;
        }
    }

    private List<CfSplit> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf) throws IOException
    {
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        for (int i = 0; i < range.rpc_endpoints.size(); i++)
        {
            String host = range.rpc_endpoints.get(i);

            if (host == null || host.equals("0.0.0.0"))
                host = range.endpoints.get(i);

            try
            {
                Cassandra.Client client = ConfigHelper.createConnection(conf, host, ConfigHelper.getInputRpcPort(conf));
                client.set_keyspace(keyspace);

                try
                {
                    return client.describe_splits_ex(cfName, range.start_token, range.end_token, splitsize);
                }
                catch (TApplicationException e)
                {
                    // fallback to guessing split size if talking to a server without describe_splits_ex method
                    if (e.getType() == TApplicationException.UNKNOWN_METHOD)
                    {
                        List<String> splitPoints = client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
                        return tokenListToSplits(splitPoints, splitsize);
                    }
                    throw e;
                }
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


    private List<CfSplit> tokenListToSplits(List<String> splitTokens, int splitsize)
    {
        List<CfSplit> splits = Lists.newArrayListWithExpectedSize(splitTokens.size() - 1);
        for (int j = 0; j < splitTokens.size() - 1; j++)
            splits.add(new CfSplit(splitTokens.get(j), splitTokens.get(j + 1), splitsize));
        return splits;
    }


    private List<TokenRange> getRangeMap(Configuration conf) throws IOException
    {
        Cassandra.Client client = ConfigHelper.getClientFromInputAddressList(conf);

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


    //
    // Old Hadoop API
    //
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException
    {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        org.apache.hadoop.mapred.InputSplit[] oldInputSplits = new org.apache.hadoop.mapred.InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++)
            oldInputSplits[i] = (ColumnFamilySplit)newInputSplits.get(i);
        return oldInputSplits;
    }

    public org.apache.hadoop.mapred.RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf jobConf, final Reporter reporter) throws IOException
    {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID)))
        {
            @Override
            public void progress()
            {
                reporter.progress();
            }
        };

        ColumnFamilyRecordReader recordReader = new ColumnFamilyRecordReader(jobConf.getInt(CASSANDRA_HADOOP_MAX_KEY_SIZE, CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT));
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit)split, tac);
        return recordReader;
    }

}
