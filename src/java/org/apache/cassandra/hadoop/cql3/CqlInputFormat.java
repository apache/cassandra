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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.datastax.driver.core.Row;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 *
 * At minimum, you need to set the KS and CF in your Hadoop job Configuration.  
 * The ConfigHelper class is provided to make this
 * simple:
 *   ConfigHelper.setInputColumnFamily
 *
 * You can also configure the number of rows per InputSplit with
 *   ConfigHelper.setInputSplitSize. The default split size is 64k rows.
 *
 *   the number of CQL rows per page
 *   CQLConfigHelper.setInputCQLPageRowSize. The default page row size is 1000. You 
 *   should set it to "as big as possible, but no bigger." It set the LIMIT for the CQL 
 *   query, so you need set it big enough to minimize the network overhead, and also
 *   not too big to avoid out of memory issue.
 *   
 *   other native protocol connection parameters in CqlConfigHelper
 */
public class CqlInputFormat extends org.apache.hadoop.mapreduce.InputFormat<Long, Row> implements org.apache.hadoop.mapred.InputFormat<Long, Row>
{
    public static final String MAPRED_TASK_ID = "mapred.task.id";
    private static final Logger logger = LoggerFactory.getLogger(CqlInputFormat.class);
    private String keyspace;
    private String cfName;
    private IPartitioner partitioner;
    private Session session;

    public RecordReader<Long, Row> getRecordReader(InputSplit split, JobConf jobConf, final Reporter reporter)
            throws IOException
    {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID)))
        {
            @Override
            public void progress()
            {
                reporter.progress();
            }
        };

        CqlRecordReader recordReader = new CqlRecordReader();
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit)split, tac);
        return recordReader;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<Long, Row> createRecordReader(
            org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException
    {
        return new CqlRecordReader();
    }

    protected void validateConfiguration(Configuration conf)
    {
        if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and table with setInputColumnFamily()");
        }
        if (ConfigHelper.getInputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node with setInputInitialAddress");
        if (ConfigHelper.getInputPartitioner(conf) == null)
            throw new UnsupportedOperationException("You must set the Cassandra partitioner class with setInputPartitioner");
    }

    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = HadoopCompat.getConfiguration(context);

        validateConfiguration(conf);

        keyspace = ConfigHelper.getInputKeyspace(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        logger.debug("partitioner is {}", partitioner);

        // canonical ranges and nodes holding replicas
        Map<TokenRange, Set<Host>> masterRangeNodes = getRangeMap(conf, keyspace);

        // canonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        List<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<>();

        try
        {
            List<Future<List<org.apache.hadoop.mapreduce.InputSplit>>> splitfutures = new ArrayList<>();
            KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            Range<Token> jobRange = null;
            if (jobKeyRange != null)
            {
                if (jobKeyRange.start_key != null)
                {
                    if (!partitioner.preservesOrder())
                        throw new UnsupportedOperationException("KeyRange based on keys can only be used with a order preserving partitioner");
                    if (jobKeyRange.start_token != null)
                        throw new IllegalArgumentException("only start_key supported");
                    if (jobKeyRange.end_token != null)
                        throw new IllegalArgumentException("only start_key supported");
                    jobRange = new Range<>(partitioner.getToken(jobKeyRange.start_key),
                                           partitioner.getToken(jobKeyRange.end_key));
                }
                else if (jobKeyRange.start_token != null)
                {
                    jobRange = new Range<>(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
                                           partitioner.getTokenFactory().fromString(jobKeyRange.end_token));
                }
                else
                {
                    logger.warn("ignoring jobKeyRange specified without start_key or start_token");
                }
            }

            session = CqlConfigHelper.getInputCluster(ConfigHelper.getInputInitialAddress(conf).split(","), conf).connect();
            Metadata metadata = session.getCluster().getMetadata();

            for (TokenRange range : masterRangeNodes.keySet())
            {
                if (jobRange == null)
                {
                    // for each tokenRange, pick a live owner and ask it to compute bite-sized splits
                    splitfutures.add(executor.submit(new SplitCallable(range, masterRangeNodes.get(range), conf)));
                }
                else
                {
                    TokenRange jobTokenRange = rangeToTokenRange(metadata, jobRange);
                    if (range.intersects(jobTokenRange))
                    {
                        for (TokenRange intersection: range.intersectWith(jobTokenRange))
                        {
                            // for each tokenRange, pick a live owner and ask it to compute bite-sized splits
                            splitfutures.add(executor.submit(new SplitCallable(intersection,  masterRangeNodes.get(range), conf)));
                        }
                    }
                }
            }

            // wait until we have all the results back
            for (Future<List<org.apache.hadoop.mapreduce.InputSplit>> futureInputSplits : splitfutures)
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

    private TokenRange rangeToTokenRange(Metadata metadata, Range<Token> range)
    {
        return metadata.newTokenRange(metadata.newToken(partitioner.getTokenFactory().toString(range.left)),
                metadata.newToken(partitioner.getTokenFactory().toString(range.right)));
    }

    private Map<TokenRange, Long> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf) throws IOException
    {
        int splitSize = ConfigHelper.getInputSplitSize(conf);
        try
        {
            return describeSplits(keyspace, cfName, range, splitSize);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private Map<TokenRange, Set<Host>> getRangeMap(Configuration conf, String keyspace)
    {
        try (Session session = CqlConfigHelper.getInputCluster(ConfigHelper.getInputInitialAddress(conf).split(","), conf).connect())
        {
            Map<TokenRange, Set<Host>> map = new HashMap<>();
            Metadata metadata = session.getCluster().getMetadata();
            for (TokenRange tokenRange : metadata.getTokenRanges())
                map.put(tokenRange, metadata.getReplicas('"' + keyspace + '"', tokenRange));
            return map;
        }
    }

    private Map<TokenRange, Long> describeSplits(String keyspace, String table, TokenRange tokenRange, int splitSize)
    {
        String query = String.format("SELECT mean_partition_size, partitions_count " +
                                     "FROM %s.%s " +
                                     "WHERE keyspace_name = ? AND table_name = ? AND range_start = ? AND range_end = ?",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.SIZE_ESTIMATES);

        ResultSet resultSet = session.execute(query, keyspace, table, tokenRange.getStart().toString(), tokenRange.getEnd().toString());

        Row row = resultSet.one();
        // If we have no data on this split, return the full split i.e., do not sub-split
        // Assume smallest granularity of partition count available from CASSANDRA-7688
        if (row == null)
        {
            Map<TokenRange, Long> wrappedTokenRange = new HashMap<>();
            wrappedTokenRange.put(tokenRange, (long) 128);
            return wrappedTokenRange;
        }

        long meanPartitionSize = row.getLong("mean_partition_size");
        long partitionCount = row.getLong("partitions_count");

        int splitCount = (int)((meanPartitionSize * partitionCount) / splitSize);
        List<TokenRange> splitRanges = tokenRange.splitEvenly(splitCount);
        Map<TokenRange, Long> rangesWithLength = new HashMap<>();
        for (TokenRange range : splitRanges)
            rangesWithLength.put(range, partitionCount/splitCount);

        return rangesWithLength;
    }

    // Old Hadoop API
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException
    {
        TaskAttemptContext tac = HadoopCompat.newTaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        InputSplit[] oldInputSplits = new InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++)
            oldInputSplits[i] = (ColumnFamilySplit)newInputSplits.get(i);
        return oldInputSplits;
    }

    /**
     * Gets a token tokenRange and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<org.apache.hadoop.mapreduce.InputSplit>>
    {

        private final TokenRange tokenRange;
        private final Set<Host> hosts;
        private final Configuration conf;

        public SplitCallable(TokenRange tr, Set<Host> hosts, Configuration conf)
        {
            this.tokenRange = tr;
            this.hosts = hosts;
            this.conf = conf;
        }

        public List<org.apache.hadoop.mapreduce.InputSplit> call() throws Exception
        {
            ArrayList<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<>();
            Map<TokenRange, Long> subSplits;
            subSplits = getSubSplits(keyspace, cfName, tokenRange, conf);
            // turn the sub-ranges into InputSplits
            String[] endpoints = new String[hosts.size()];

            // hadoop needs hostname, not ip
            int endpointIndex = 0;
            for (Host endpoint : hosts)
                endpoints[endpointIndex++] = endpoint.getAddress().getHostName();

            boolean partitionerIsOpp = partitioner instanceof OrderPreservingPartitioner || partitioner instanceof ByteOrderedPartitioner;

            for (TokenRange subSplit : subSplits.keySet())
            {
                List<TokenRange> ranges = subSplit.unwrap();
                for (TokenRange subrange : ranges)
                {
                    ColumnFamilySplit split =
                            new ColumnFamilySplit(
                                    partitionerIsOpp ?
                                            subrange.getStart().toString().substring(2) : subrange.getStart().toString(),
                                    partitionerIsOpp ?
                                            subrange.getEnd().toString().substring(2) : subrange.getStart().toString(),
                                    subSplits.get(subSplit),
                                    endpoints);

                    logger.debug("adding {}", split);
                    splits.add(split);
                }
            }
            return splits;
        }
    }
}
