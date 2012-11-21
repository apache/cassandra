/**
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

package org.apache.cassandra.service;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs latency and consistency predictions as described in
 * <a href="http://arxiv.org/pdf/1204.6082.pdf">
 * "Probabilistically Bounded Staleness for Practical Partial Quorums"</a>
 * by Bailis et al. in VLDB 2012. The predictions are of the form:
 * <p/>
 * <i>With ReplicationFactor <tt>N</tt>, read consistency level of
 * <tt>R</tt>, and write consistency level <tt>W</tt>, after
 * <tt>t</tt> seconds, <tt>p</tt>% of reads will return a version
 * within <tt>k</tt> versions of the last written; this should result
 * in a latency of <tt>L</tt> ms.</i>
 * <p/>
 * <p/>
 * These predictions should be used as a rough guideline for system
 * operators. This interface is exposed through nodetool.
 * <p/>
 * <p/>
 * The class accomplishes this by measuring latencies for reads and
 * writes, then using Monte Carlo simulation to predict behavior under
 * a given N,R, and W based on those latencies.
 * <p/>
 * <p/>
 * We capture four distributions:
 * <p/>
 * <ul>
 * <li>
 * <tt>W</tt>: time from when the coordinator sends a mutation to the time
 * that a replica begins to serve the new value(s)
 * </li>
 * <p/>
 * <li>
 * <tt>A</tt>: time from when a replica accepting a mutation sends an
 * acknowledgment to the time the coordinator hears of it
 * </li>
 * <p/>
 * <li>
 * <tt>R</tt>: time from when the coordinator sends a read request to the time
 * that the replica performs the read
 * </li>
 * <p/>
 * <li>
 * <tt>S</tt>: time from when the replica sends a read response to the time
 * when the coordinator receives it
 * </li>
 * </ul>
 * <p/>
 * <tt>A</tt> and <tt>S</tt> are mostly network-bound, while W and R
 * depend on both the network and local processing time.
 * <p/>
 * <p/>
 * <b>Caveats:</b>
 * Prediction is only as good as the latencies collected. Accurate
 * prediction requires synchronizing clocks between replicas.  We
 * collect a running sample of latencies, but, if latencies change
 * dramatically, predictions will be off.
 * <p/>
 * <p/>
 * The predictions are conservative, or worst-case, meaning we may
 * predict more staleness than in practice in the following ways:
 * <ul>
 * <li>
 * We do not account for read repair.
 * </li>
 * <li>
 * We do not account for Merkle tree exchange.
 * </li>
 * <li>
 * Multi-version staleness is particularly conservative.
 * </li>
 * <li>
 * We simulate non-local reads and writes. We assume that the
 * coordinating Cassandra node is not itself a replica for a given key.
 * </li>
 * </ul>
 * <p/>
 * <p/>
 * The predictions are optimistic in the following ways:
 * <ul>
 * <li>
 * We do not predict the impact of node failure.
 * </li>
 * <li>
 * We do not model hinted handoff.
 * </li>
 * </ul>
 *
 * @see org.apache.cassandra.thrift.ConsistencyLevel
 * @see org.apache.cassandra.locator.AbstractReplicationStrategy
 */

public class PBSPredictor implements PBSPredictorMBean
{
    private static final Logger logger = LoggerFactory.getLogger(PBSPredictor.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.service:type=PBSPredictor";
    private static final boolean DEFAULT_DO_LOG_LATENCIES = false;
    private static final int DEFAULT_MAX_LOGGED_LATENCIES = 10000;
    private static final int DEFAULT_NUMBER_TRIALS_PREDICTION = 10000;

    /*
     * We record a fixed size set of WARS latencies for read and
     * mutation operations.  We store the order in which each
     * operation arrived, and use an LRU policy to evict old
     * messages.
     *
     * This information is stored as a mapping from messageIDs to
     * latencies.
     */

    /*
     * Helper class which minimizes the number of HashMaps we maintain.
     * For a given messageId, this class maintains the startTime of the message,
     * and a queue for send times and reply times.
     *
     * sendLats corresponds to W and R, while replyLats is used for A and S.
     */
    private class MessageLatencyCollection
    {
        MessageLatencyCollection(Long startTime)
        {
            this.startTime = startTime;
            this.sendLats = new ConcurrentLinkedQueue<Long>();
            this.replyLats = new ConcurrentLinkedQueue<Long>();
        }

        void addSendLat(Long sendLat)
        {
            sendLats.add(sendLat);
        }

        void addReplyLat(Long replyLat)
        {
            replyLats.add(replyLat);
        }

        Collection<Long> getSendLats()
        {
            return sendLats;
        }

        Collection<Long> getReplyLats()
        {
            return replyLats;
        }

        Long getStartTime()
        {
            return startTime;
        }

        Long startTime;
        Collection<Long> sendLats;
        Collection<Long> replyLats;
    }

    // used for LRU replacement
    private final Queue<String> writeMessageIds = new LinkedBlockingQueue<String>();
    private final Queue<String> readMessageIds = new LinkedBlockingQueue<String>();

    private final Map<String, MessageLatencyCollection> messageIdToWriteLats = new ConcurrentHashMap<String, MessageLatencyCollection>();
    private final Map<String, MessageLatencyCollection> messageIdToReadLats = new ConcurrentHashMap<String, MessageLatencyCollection>();

    private Random random;
    private boolean initialized = false;

    private boolean logLatencies = DEFAULT_DO_LOG_LATENCIES;
    private int maxLoggedLatencies = DEFAULT_MAX_LOGGED_LATENCIES;
    private int numberTrialsPrediction = DEFAULT_NUMBER_TRIALS_PREDICTION;

    private static final PBSPredictor instance = new PBSPredictor();

    public static PBSPredictor instance()
    {
        return instance;
    }

    private PBSPredictor()
    {
        init();
    }

    public void enableConsistencyPredictionLogging()
    {
        logLatencies = true;
    }

    public void disableConsistencyPredictionLogging()
    {
        logLatencies = false;
    }

    public boolean isLoggingEnabled()
    {
        return logLatencies;
    }

    public void setMaxLoggedLatenciesForConsistencyPrediction(int maxLogged)
    {
        maxLoggedLatencies = maxLogged;
    }

    public void setNumberTrialsForConsistencyPrediction(int numTrials)
    {
        numberTrialsPrediction = numTrials;
    }

    public void init()
    {
        if (!initialized)
        {
            random = new Random();

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            try
            {
                mbs.registerMBean(this, new ObjectName(PBSPredictor.MBEAN_NAME));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            initialized = true;
        }
    }


    // used for random sampling from the latencies
    private long getRandomElement(List<Long> list)
    {
        if (list.size() == 0)
            throw new RuntimeException("Not enough data for prediction");
        return list.get(random.nextInt(list.size()));
    }

    // used for calculating the average latency of a read or write operation
    // given a set of simulated latencies
    private float listAverage(List<Long> list)
    {
        long accum = 0;
        for (long value : list)
            accum += value;
        return (float) accum / list.size();
    }

    // calculate the percentile entry of a list
    private long getPercentile(List<Long> list, float percentile)
    {
        Collections.sort(list);
        return list.get((int) (list.size() * percentile));
    }

    /*
     * For our trials, sample the latency for the (replicaNumber)th
     * reply for one of WARS
     * if replicaNumber > the number of replicas we have data for
     *   (say we have data for ReplicationFactor 2 but ask for N=3)
     * then we randomly sample from all response times
     */
    private long getRandomLatencySample(Map<Integer, List<Long>> samples, int replicaNumber)
    {
        if (samples.containsKey(replicaNumber))
        {
            return getRandomElement(samples.get(replicaNumber));
        }

        return getRandomElement(samples.get(samples.keySet().toArray()[random.nextInt(samples.keySet().size())]));
    }

    /*
     *  To perform the prediction, we randomly sample from the
     *  collected WARS latencies, simulating writes followed by reads
     *  exactly t milliseconds afterwards. We count the number of
     *  reordered reads and writes to calculate the probability of
     *  staleness along with recording operation latencies.
     */


    public PBSPredictionResult doPrediction(int n,
                                            int r,
                                            int w,
                                            float timeSinceWrite,
                                            int numberVersionsStale,
                                            float percentileLatency)
    {
        if (r > n)
            throw new IllegalArgumentException("r must be less than n");
        if (r < 0)
            throw new IllegalArgumentException("r must be positive");
        if (w > n)
            throw new IllegalArgumentException("w must be less than n");
        if (w < 0)
            throw new IllegalArgumentException("w must be positive");
        if (percentileLatency < 0 || percentileLatency > 1)
            throw new IllegalArgumentException("percentileLatency must be between 0 and 1 inclusive");
        if (numberVersionsStale < 0)
            throw new IllegalArgumentException("numberVersionsStale must be positive");

        if (!logLatencies)
            throw new IllegalStateException("Latency logging is not enabled");

        // get a mapping of {replica number : latency} for each of WARS
        Map<Integer, List<Long>> wLatencies = getOrderedWLatencies();
        Map<Integer, List<Long>> aLatencies = getOrderedALatencies();
        Map<Integer, List<Long>> rLatencies = getOrderedRLatencies();
        Map<Integer, List<Long>> sLatencies = getOrderedSLatencies();

        if (wLatencies.isEmpty() || aLatencies.isEmpty())
            throw new IllegalStateException("No write latencies have been recorded so far. Run some (non-local) inserts.");

        if (rLatencies.isEmpty() || sLatencies.isEmpty())
            throw new IllegalStateException("No read latencies have been recorded so far. Run some (non-local) reads.");

        // storage for simulated read and write latencies
        ArrayList<Long> readLatencies = new ArrayList<Long>();
        ArrayList<Long> writeLatencies = new ArrayList<Long>();

        long consistentReads = 0;

        // storage for latencies for each replica for a given Monte Carlo trial
        // arr[i] will hold the ith replica's latency for one of WARS
        ArrayList<Long> trialWLatencies = new ArrayList<Long>();
        ArrayList<Long> trialRLatencies = new ArrayList<Long>();

        ArrayList<Long> replicaWriteLatencies = new ArrayList<Long>();
        ArrayList<Long> replicaReadLatencies = new ArrayList<Long>();

        //run repeated trials and observe staleness
        for (int i = 0; i < numberTrialsPrediction; ++i)
        {
            //simulate sending a write to N replicas then sending a
            //read to N replicas and record the latencies by randomly
            //sampling from gathered latencies
            for (int replicaNo = 0; replicaNo < n; ++replicaNo)
            {
                long trialWLatency = getRandomLatencySample(wLatencies, replicaNo);
                long trialALatency = getRandomLatencySample(aLatencies, replicaNo);

                trialWLatencies.add(trialWLatency);

                replicaWriteLatencies.add(trialWLatency + trialALatency);
            }

            // reads are only sent to R replicas - so pick R random read and
            // response latencies
            for (int replicaNo = 0; replicaNo < r; ++replicaNo)
            {
                long trialRLatency = getRandomLatencySample(rLatencies, replicaNo);
                long trialSLatency = getRandomLatencySample(sLatencies, replicaNo);

                trialRLatencies.add(trialRLatency);

                replicaReadLatencies.add(trialRLatency + trialSLatency);
            }

            // the write latency for this trial is the time it takes
            // for the wth replica to respond (W+A)
            Collections.sort(replicaWriteLatencies);
            long writeLatency = replicaWriteLatencies.get(w - 1);
            writeLatencies.add(writeLatency);

            ArrayList<Long> sortedReplicaReadLatencies = new ArrayList<Long>(replicaReadLatencies);
            Collections.sort(sortedReplicaReadLatencies);

            // the read latency for this trial is the time it takes
            // for the rth replica to respond (R+S)
            readLatencies.add(sortedReplicaReadLatencies.get(r - 1));

            // were all of the read responses reordered?

            // for each of the first r messages (the ones the
            // coordinator will pick from):
            //--if the read message came in after this replica saw the
            // write, it will be consistent
            //--each read request is sent at time
            // writeLatency+timeSinceWrite

            for (int responseNumber = 0; responseNumber < r; ++responseNumber)
            {
                int replicaNumber = replicaReadLatencies.indexOf(sortedReplicaReadLatencies.get(responseNumber));

                if (writeLatency + timeSinceWrite + trialRLatencies.get(replicaNumber) >=
                    trialWLatencies.get(replicaNumber))
                {
                    consistentReads++;
                    break;
                }

                // tombstone this replica in the case that we have
                // duplicate read latencies
                replicaReadLatencies.set(replicaNumber, -1L);
            }

            // clear storage for the next trial
            trialWLatencies.clear();
            trialRLatencies.clear();

            replicaReadLatencies.clear();
            replicaWriteLatencies.clear();
        }

        float oneVersionConsistencyProbability = (float) consistentReads / numberTrialsPrediction;

        // to calculate multi-version staleness, we exponentiate the staleness probability by the number of versions
        float consistencyProbability = (float) (1 - Math.pow((double) (1 - oneVersionConsistencyProbability),
                                                             numberVersionsStale));

        float averageWriteLatency = listAverage(writeLatencies);
        float averageReadLatency = listAverage(readLatencies);

        long percentileWriteLatency = getPercentile(writeLatencies, percentileLatency);
        long percentileReadLatency = getPercentile(readLatencies, percentileLatency);

        return new PBSPredictionResult(n,
                                       r,
                                       w,
                                       timeSinceWrite,
                                       numberVersionsStale,
                                       consistencyProbability,
                                       averageReadLatency,
                                       averageWriteLatency,
                                       percentileReadLatency,
                                       percentileLatency,
                                       percentileWriteLatency,
                                       percentileLatency);
    }

    public void startWriteOperation(String id)
    {
        if (!logLatencies)
            return;

        startWriteOperation(id, System.currentTimeMillis());
    }

    public void startWriteOperation(String id, long startTime)
    {
        if (!logLatencies)
            return;

        assert (!messageIdToWriteLats.containsKey(id));

        writeMessageIds.add(id);

        // LRU replacement of latencies
        // the maximum number of entries is sloppy, but that's acceptable for our purposes
        if (writeMessageIds.size() > maxLoggedLatencies)
        {
            String toEvict = writeMessageIds.remove();
            messageIdToWriteLats.remove(toEvict);
        }

        messageIdToWriteLats.put(id, new MessageLatencyCollection(startTime));
    }

    public void startReadOperation(String id)
    {
        if (!logLatencies)
            return;

        startReadOperation(id, System.currentTimeMillis());
    }

    public void startReadOperation(String id, long startTime)
    {
        if (!logLatencies)
            return;

        assert (!messageIdToReadLats.containsKey(id));
        readMessageIds.add(id);

        // LRU replacement of latencies
        // the maximum number of entries is sloppy, but that's acceptable for our purposes
        if (readMessageIds.size() > maxLoggedLatencies)
        {
            String toEvict = readMessageIds.remove();
            messageIdToReadLats.remove(toEvict);
        }

        messageIdToReadLats.put(id, new MessageLatencyCollection(startTime));
    }

    public void logWriteResponse(String id, long constructionTime)
    {
        if (!logLatencies)
            return;

        logWriteResponse(id, constructionTime, System.currentTimeMillis());
    }

    public void logWriteResponse(String id, long responseCreationTime, long receivedTime)
    {
        if (!logLatencies)
            return;

        MessageLatencyCollection writeLatsCollection = messageIdToWriteLats.get(id);
        if (writeLatsCollection == null)
        {
            return;
        }

        Long startTime = writeLatsCollection.getStartTime();
        writeLatsCollection.addSendLat(Math.max(0, responseCreationTime - startTime));
        writeLatsCollection.addReplyLat(Math.max(0, receivedTime - responseCreationTime));
    }

    public void logReadResponse(String id, long constructionTime)
    {
        if (!logLatencies)
            return;

        logReadResponse(id, constructionTime, System.currentTimeMillis());
    }

    public void logReadResponse(String id, long responseCreationTime, long receivedTime)
    {
        if (!logLatencies)
            return;

        MessageLatencyCollection readLatsCollection = messageIdToReadLats.get(id);
        if (readLatsCollection == null)
        {
            return;
        }

        Long startTime = readLatsCollection.getStartTime();
        readLatsCollection.addSendLat(Math.max(0, responseCreationTime - startTime));
        readLatsCollection.addReplyLat(Math.max(0, receivedTime - responseCreationTime));
    }

    Map<Integer, List<Long>> getOrderedWLatencies()
    {
        Collection<Collection<Long>> allWLatencies = new ArrayList<Collection<Long>>();
        for (MessageLatencyCollection wlc : messageIdToWriteLats.values())
        {
            allWLatencies.add(wlc.getSendLats());
        }

        return getOrderedLatencies(allWLatencies);
    }

    Map<Integer, List<Long>> getOrderedALatencies()
    {
        Collection<Collection<Long>> allALatencies = new ArrayList<Collection<Long>>();
        for (MessageLatencyCollection wlc : messageIdToWriteLats.values())
            allALatencies.add(wlc.getReplyLats());
        return getOrderedLatencies(allALatencies);
    }

    Map<Integer, List<Long>> getOrderedRLatencies()
    {
        Collection<Collection<Long>> allRLatencies = new ArrayList<Collection<Long>>();
        for (MessageLatencyCollection rlc : messageIdToReadLats.values())
        {
            allRLatencies.add(rlc.getSendLats());
        }
        return getOrderedLatencies(allRLatencies);
    }

    Map<Integer, List<Long>> getOrderedSLatencies()
    {
        Collection<Collection<Long>> allSLatencies = new ArrayList<Collection<Long>>();
        for (MessageLatencyCollection rlc : messageIdToReadLats.values())
            allSLatencies.add(rlc.getReplyLats());
        return getOrderedLatencies(allSLatencies);
    }

    // Return the collected latencies indexed by response number instead of by messageID
    private Map<Integer, List<Long>> getOrderedLatencies(Collection<Collection<Long>> latencyLists)
    {
        Map<Integer, List<Long>> ret = new HashMap<Integer, List<Long>>();

        // N may vary
        int maxResponses = 0;

        for (Collection<Long> latencies : latencyLists)
        {
            List<Long> sortedLatencies = new ArrayList<Long>(latencies);
            Collections.sort(sortedLatencies);

            if (sortedLatencies.size() > maxResponses)
            {
                for (int i = maxResponses + 1; i <= sortedLatencies.size(); ++i)
                {
                    ret.put(i, new Vector<Long>());
                }

                maxResponses = sortedLatencies.size();
            }

            // indexing by 0 is awkward since we're talking about the ith response
            for (int i = 1; i <= sortedLatencies.size(); ++i)
            {
                ret.get(i).add(sortedLatencies.get(i - 1));
            }
        }

        return ret;
    }
}
