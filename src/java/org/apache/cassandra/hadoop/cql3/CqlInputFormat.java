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
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TokenRange;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 *
 * At minimum, you need to set the KS and CF in your Hadoop job Configuration.
 * The ConfigHelper class is provided to make this
 * simple:
 *   ConfigHelper.setInputColumnFamily
 *
 * You can also configure the number of rows per InputSplit with
 *   1: ConfigHelper.setInputSplitSize. The default split size is 64k rows.
 *   or
 *   2: ConfigHelper.setInputSplitSizeInMb. InputSplit size in MB with new, more precise method
 *   If no value is provided for InputSplitSizeInMb, we default to using InputSplitSize.
 *
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

    public RecordReader<Long, Row> getRecordReader(InputSplit split, JobConf jobConf, final Reporter reporter)
            throws IOException
    {
        TaskAttemptContext tac = HadoopCompat.newMapContext(
                jobConf,
                TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID)),
                null,
                null,
                null,
                new ReporterWrapper(reporter),
                null);


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
        logger.trace("partitioner is {}", partitioner);

        // canonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = executorFactory().pooled("HadoopInput", 128);
        List<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<>();

        String[] inputInitialAddress = ConfigHelper.getInputInitialAddress(conf).split(",");
        try (Cluster cluster = CqlConfigHelper.getInputCluster(inputInitialAddress, conf);
             Session session = cluster.connect())
        {
            List<SplitFuture> splitfutures = new ArrayList<>();
            //TODO if the job range is defined and does perfectly match tokens, then the logic will be unable to get estimates since they are pre-computed
            // tokens: [0, 10, 20]
            // job range: [0, 10) - able to get estimate
            // job range: [5, 15) - unable to get estimate
            Pair<String, String> jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            Range<Token> jobRange = null;
            if (jobKeyRange != null)
            {
                jobRange = new Range<>(partitioner.getTokenFactory().fromString(jobKeyRange.left),
                                       partitioner.getTokenFactory().fromString(jobKeyRange.right));
            }

            Metadata metadata = cluster.getMetadata();

            // canonical ranges and nodes holding replicas
            Map<TokenRange, List<Host>> masterRangeNodes = getRangeMap(keyspace, metadata, getTargetDC(metadata, inputInitialAddress));
            for (TokenRange range : masterRangeNodes.keySet())
            {
                if (jobRange == null)
                {
                    for (TokenRange unwrapped : range.unwrap())
                    {
                        // for each tokenRange, pick a live owner and ask it for the byte-sized splits
                        SplitFuture task = new SplitFuture(new SplitCallable(unwrapped, masterRangeNodes.get(range), conf, session));
                        executor.submit(task);
                        splitfutures.add(task);
                    }
                }
                else
                {
                    TokenRange jobTokenRange = rangeToTokenRange(metadata, jobRange);
                    if (range.intersects(jobTokenRange))
                    {
                        for (TokenRange intersection: range.intersectWith(jobTokenRange))
                        {
                            for (TokenRange unwrapped : intersection.unwrap())
                            {
                                // for each tokenRange, pick a live owner and ask it for the byte-sized splits
                                SplitFuture task = new SplitFuture(new SplitCallable(unwrapped,  masterRangeNodes.get(range), conf, session));
                                executor.submit(task);
                                splitfutures.add(task);
                            }
                        }
                    }
                }
            }

            // wait until we have all the results back
            List<SplitFuture> failedTasks = new ArrayList<>();
            int maxSplits = 0;
            long expectedPartionsForFailedRanges = 0;
            for (SplitFuture task : splitfutures)
            {
                try
                {
                    List<ColumnFamilySplit> tokenRangeSplits = task.get();
                    if (tokenRangeSplits.size() > maxSplits)
                    {
                        maxSplits = tokenRangeSplits.size();
                        expectedPartionsForFailedRanges = tokenRangeSplits.get(0).getLength();
                    }
                    splits.addAll(tokenRangeSplits);
                }
                catch (Exception e)
                {
                    failedTasks.add(task);
                }
            }
            // The estimate is only stored on a single host, if that host is down then can not get the estimate
            // its more than likely that a single host could be "too large" for one split but there is no way of
            // knowning!
            // This logic attempts to guess the estimate from all the successful ranges
            if (!failedTasks.isEmpty())
            {
                // if every split failed this will be 0
                if (maxSplits == 0)
                    throwAllSplitsFailed(failedTasks);
                for (SplitFuture task : failedTasks)
                {
                    try
                    {
                        // the task failed, so this should throw
                        task.get();
                    }
                    catch (Exception cause)
                    {
                        logger.warn("Unable to get estimate for {}, the host {} had a exception; falling back to default estimate", task.splitCallable.tokenRange, task.splitCallable.hosts.get(0), cause);
                    }
                }
                for (SplitFuture task : failedTasks)
                    splits.addAll(toSplit(task.splitCallable.hosts, splitTokenRange(task.splitCallable.tokenRange, maxSplits, expectedPartionsForFailedRanges)));
            }
        }
        finally
        {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(nanoTime()));
        return splits;
    }

    private static IllegalStateException throwAllSplitsFailed(List<SplitFuture> failedTasks)
    {
        IllegalStateException exception = new IllegalStateException("No successful tasks found");
        for (SplitFuture task : failedTasks)
        {
            try
            {
                // the task failed, so this should throw
                task.get();
            }
            catch (Exception cause)
            {
                exception.addSuppressed(cause);
            }
        }
        throw exception;
    }

    private static String getTargetDC(Metadata metadata, String[] inputInitialAddress)
    {
        BiMultiValMap<InetAddress, String> addressToDc = new BiMultiValMap<>();
        Multimap<String, InetAddress> dcToAddresses = addressToDc.inverse();

        // only way to match is off the broadcast addresses, so for all hosts do a existence check
        Set<InetAddress> addresses = new HashSet<>(inputInitialAddress.length);
        for (String inputAddress : inputInitialAddress)
            addresses.addAll(parseAddress(inputAddress));

        for (Host host : metadata.getAllHosts())
        {
            InetAddress address = host.getBroadcastAddress();
            if (addresses.contains(address))
                addressToDc.put(address, host.getDatacenter());
        }

        switch (dcToAddresses.keySet().size())
        {
            case 1:
                return Iterables.getOnlyElement(dcToAddresses.keySet());
            case 0:
                throw new IllegalStateException("Input addresses could not be used to find DC; non match client metadata");
            default:
                // Mutliple DCs found, attempt to pick the first based off address list. This is to mimic the 2.1
                // behavior which would connect in order and the first node successfully able to connect to was the
                // local DC to use; since client abstracts this, we rely on existence as a proxy for connect.
                for (String inputAddress : inputInitialAddress)
                {
                    for (InetAddress add : parseAddress(inputAddress))
                    {
                        String dc = addressToDc.get(add);
                        // possible the address isn't in the cluster and the client dropped, so ignore null
                        if (dc != null)
                            return dc;
                    }
                }
                // some how we were able to connect to the cluster, find multiple DCs using matching, and yet couldn't
                // match again...
                throw new AssertionError("Unable to infer datacenter from initial addresses; multiple datacenters found "
                                         + dcToAddresses.keySet() + ", should only use addresses from one datacenter");
        }
    }

    private static List<InetAddress> parseAddress(String str)
    {
        try
        {
            return Arrays.asList(InetAddress.getAllByName(str));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private TokenRange rangeToTokenRange(Metadata metadata, Range<Token> range)
    {
        return metadata.newTokenRange(metadata.newToken(partitioner.getTokenFactory().toString(range.left)),
                metadata.newToken(partitioner.getTokenFactory().toString(range.right)));
    }

    private Map<TokenRange, Long> getSubSplits(String keyspace, String cfName, TokenRange range, Host host, Configuration conf, Session session)
    {
        int splitSize = ConfigHelper.getInputSplitSize(conf);
        int splitSizeMiB = ConfigHelper.getInputSplitSizeInMb(conf);
        return describeSplits(keyspace, cfName, range, host, splitSize, splitSizeMiB, session);
    }

    private static Map<TokenRange, List<Host>> getRangeMap(String keyspace, Metadata metadata, String targetDC)
    {
        return CqlClientHelper.getLocalPrimaryRangeForDC(keyspace, metadata, targetDC);
    }

    private Map<TokenRange, Long> describeSplits(String keyspace, String table, TokenRange tokenRange, Host host, int splitSize, int splitSizeMb, Session session)
    {
        // In 2.1 the host list was walked in-order (only move to next if IOException) and calls
        // org.apache.cassandra.service.StorageService.getSplits(java.lang.String, java.lang.String, org.apache.cassandra.dht.Range<org.apache.cassandra.dht.Token>, int)
        // that call computes totalRowCountEstimate (used to compute #splits) then splits the ring based off those estimates
        //
        // The main difference is that the estimates in 2.1 were computed based off the data, so replicas could answer the estimates
        // In 3.0 we rely on the below CQL query which is local and only computes estimates for the primary range; this
        // puts us in a sticky spot to answer, if the node fails what do we do?  3.0 behavior only matches 2.1 IFF all
        // nodes are up and healthy
        ResultSet resultSet = queryTableEstimates(session, host, keyspace, table, tokenRange);

        Row row = resultSet.one();

        long meanPartitionSize = 0;
        long partitionCount = 0;
        int splitCount = 0;

        if (row != null)
        {
            meanPartitionSize = row.getLong("mean_partition_size");
            partitionCount = row.getLong("partitions_count");

            splitCount = splitSizeMb > 0
                ? (int)(meanPartitionSize * partitionCount / splitSizeMb / 1024 / 1024)
                : (int)(partitionCount / splitSize);
        }

        // If we have no data on this split or the size estimate is 0,
        // return the full split i.e., do not sub-split
        // Assume smallest granularity of partition count available from CASSANDRA-7688
        if (splitCount == 0)
        {
            Map<TokenRange, Long> wrappedTokenRange = new HashMap<>();
            wrappedTokenRange.put(tokenRange, partitionCount == 0 ? 128L : partitionCount);
            return wrappedTokenRange;
        }

        return splitTokenRange(tokenRange, splitCount, partitionCount / splitCount);
    }

    private static ResultSet queryTableEstimates(Session session, Host host, String keyspace, String table, TokenRange tokenRange)
    {
        try
        {
            String query = String.format("SELECT mean_partition_size, partitions_count " +
                                         "FROM %s.%s " +
                                         "WHERE keyspace_name = ? AND table_name = ? AND range_type = '%s' AND range_start = ? AND range_end = ?",
                                         SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                         SystemKeyspace.TABLE_ESTIMATES,
                                         SystemKeyspace.TABLE_ESTIMATES_TYPE_LOCAL_PRIMARY);
            Statement stmt = new SimpleStatement(query, keyspace, table, tokenRange.getStart().toString(), tokenRange.getEnd().toString()).setHost(host);
            return session.execute(stmt);
        }
        catch (InvalidQueryException e)
        {
            // if the table doesn't exist, fall back to old table.  This is likely to return no records in a multi
            // DC setup, but should work fine in a single DC setup.
            String query = String.format("SELECT mean_partition_size, partitions_count " +
                                         "FROM %s.%s " +
                                         "WHERE keyspace_name = ? AND table_name = ? AND range_start = ? AND range_end = ?",
                                         SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                         SystemKeyspace.LEGACY_SIZE_ESTIMATES);

            Statement stmt = new SimpleStatement(query, keyspace, table, tokenRange.getStart().toString(), tokenRange.getEnd().toString()).setHost(host);
            return session.execute(stmt);
        }
    }

    private static Map<TokenRange, Long> splitTokenRange(TokenRange tokenRange, int splitCount, long partitionCount)
    {
        List<TokenRange> splitRanges = tokenRange.splitEvenly(splitCount);
        Map<TokenRange, Long> rangesWithLength = Maps.newHashMapWithExpectedSize(splitRanges.size());
        for (TokenRange range : splitRanges)
            rangesWithLength.put(range, partitionCount);

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
    class SplitCallable implements Callable<List<ColumnFamilySplit>>
    {

        private final TokenRange tokenRange;
        private final List<Host> hosts;
        private final Configuration conf;
        private final Session session;

        public SplitCallable(TokenRange tokenRange, List<Host> hosts, Configuration conf, Session session)
        {
            Preconditions.checkArgument(!hosts.isEmpty(), "hosts list requires at least 1 host but was empty");
            this.tokenRange = tokenRange;
            this.hosts = hosts;
            this.conf = conf;
            this.session = session;
        }

        public List<ColumnFamilySplit> call() throws Exception
        {
            Map<TokenRange, Long> subSplits = getSubSplits(keyspace, cfName, tokenRange, hosts.get(0), conf, session);
            return toSplit(hosts, subSplits);
        }

    }

    private static class SplitFuture extends FutureTask<List<ColumnFamilySplit>>
    {
        private final SplitCallable splitCallable;

        SplitFuture(SplitCallable splitCallable)
        {
            super(splitCallable);
            this.splitCallable = splitCallable;
        }
    }

    private List<ColumnFamilySplit> toSplit(List<Host> hosts, Map<TokenRange, Long> subSplits)
    {
        // turn the sub-ranges into InputSplits
        String[] endpoints = new String[hosts.size()];

        // hadoop needs hostname, not ip
        int endpointIndex = 0;
        for (Host endpoint : hosts)
            endpoints[endpointIndex++] = endpoint.getAddress().getHostName();

        boolean partitionerIsOpp = partitioner instanceof OrderPreservingPartitioner || partitioner instanceof ByteOrderedPartitioner;

        ArrayList<ColumnFamilySplit> splits = new ArrayList<>();
        for (Map.Entry<TokenRange, Long> subSplitEntry : subSplits.entrySet())
        {
            TokenRange subrange = subSplitEntry.getKey();
            ColumnFamilySplit split =
                new ColumnFamilySplit(
                    partitionerIsOpp ?
                        subrange.getStart().toString().substring(2) : subrange.getStart().toString(),
                    partitionerIsOpp ?
                        subrange.getEnd().toString().substring(2) : subrange.getEnd().toString(),
                    subSplitEntry.getValue(),
                    endpoints);

            logger.trace("adding {}", split);
            splits.add(split);
        }
        return splits;
    }
}
