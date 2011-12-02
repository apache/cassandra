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

import java.io.*;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.*;

/**
 * AntiEntropyService encapsulates "validating" (hashing) individual column families,
 * exchanging MerkleTrees with remote nodes via a TreeRequest/Response conversation,
 * and then triggering repairs for disagreeing ranges.
 *
 * Every Tree conversation has an 'initiator', where valid trees are sent after generation
 * and where the local and remote tree will rendezvous in rendezvous(cf, endpoint, tree).
 * Once the trees rendezvous, a Differencer is executed and the service can trigger repairs
 * for disagreeing ranges.
 *
 * Tree comparison and repair triggering occur in the single threaded Stage.ANTIENTROPY.
 *
 * The steps taken to enact a repair are as follows:
 * 1. A major compaction is triggered via nodeprobe:
 *   * Nodeprobe sends TreeRequest messages to all neighbors of the target node: when a node
 *     receives a TreeRequest, it will perform a readonly compaction to immediately validate
 *     the column family.
 * 2. The compaction process validates the column family by:
 *   * Calling Validator.prepare(), which samples the column family to determine key distribution,
 *   * Calling Validator.add() in order for every row in the column family,
 *   * Calling Validator.complete() to indicate that all rows have been added.
 *     * Calling complete() indicates that a valid MerkleTree has been created for the column family.
 *     * The valid tree is returned to the requesting node via a TreeResponse.
 * 3. When a node receives a TreeResponse, it passes the tree to rendezvous(), which checks for trees to
 *    rendezvous with / compare to:
 *   * If the tree is local, it is cached, and compared to any trees that were received from neighbors.
 *   * If the tree is remote, it is immediately compared to a local tree if one is cached. Otherwise,
 *     the remote tree is stored until a local tree can be generated.
 *   * A Differencer object is enqueued for each comparison.
 * 4. Differencers are executed in Stage.ANTIENTROPY, to compare the two trees, and perform repair via the
 *    streaming api.
 */
public class AntiEntropyService
{
    private static final Logger logger = LoggerFactory.getLogger(AntiEntropyService.class);

    // singleton enforcement
    public static final AntiEntropyService instance = new AntiEntropyService();

    private static final ThreadPoolExecutor executor;
    static
    {
        executor = new JMXConfigurableThreadPoolExecutor(4,
                                                         60,
                                                         TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<Runnable>(),
                                                         new NamedThreadFactory("AntiEntropySessions"),
                                                         "internal");
    }

    /**
     * A map of active session.
     */
    private final ConcurrentMap<String, RepairSession> sessions;

    /**
     * Protected constructor. Use AntiEntropyService.instance.
     */
    protected AntiEntropyService()
    {
        sessions = new ConcurrentHashMap<String, RepairSession>();
    }

    /**
     * Requests repairs for the given table and column families, and blocks until all repairs have been completed.
     */
    public RepairFuture submitRepairSession(Range range, String tablename, String... cfnames)
    {
        RepairFuture futureTask = new RepairSession(range, tablename, cfnames).getFuture();
        executor.execute(futureTask);
        return futureTask;
    }

    public void terminateSessions()
    {
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown();
        }
    }

    // for testing only. Create a session corresponding to a fake request and
    // add it to the sessions (avoid NPE in tests)
    RepairFuture submitArtificialRepairSession(TreeRequest req, String tablename, String... cfnames)
    {
        RepairFuture futureTask = new RepairSession(req, tablename, cfnames).getFuture();
        executor.execute(futureTask);
        return futureTask;
    }

    /**
     * Return all of the neighbors with whom we share data.
     */
    static Set<InetAddress> getNeighbors(String table, Range range)
    {
        StorageService ss = StorageService.instance;
        Map<Range, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(table);
        if (!replicaSets.containsKey(range))
            return Collections.emptySet();
        Set<InetAddress> neighbors = new HashSet<InetAddress>(replicaSets.get(range));
        neighbors.remove(FBUtilities.getBroadcastAddress());
        // Excluding all node with version <= 0.7 since they don't know how to
        // create a correct merkle tree (they build it over the full range)
        Iterator<InetAddress> iter = neighbors.iterator();
        while (iter.hasNext())
        {
            InetAddress endpoint = iter.next();
            if (Gossiper.instance.getVersion(endpoint) <= MessagingService.VERSION_07)
            {
                logger.info("Excluding " + endpoint + " from repair because it is on version 0.7 or sooner. You should consider updating this node before running repair again.");
                iter.remove();
            }
        }
        return neighbors;
    }

    /**
     * Register a tree for the given request to be compared to the appropriate trees in Stage.ANTIENTROPY when they become available.
     */
    private void rendezvous(TreeRequest request, MerkleTree tree)
    {
        RepairSession session = sessions.get(request.sessionid);
        if (session == null)
        {
            logger.warn("Got a merkle tree response for unknown repair session {}: either this node has been restarted since the session was started, or the session has been interrupted for an unknown reason. ", request.sessionid);
            return;
        }

        RepairSession.RepairJob job = session.jobs.peek();
        if (job == null)
        {
            assert session.terminated();
            return;
        }

        logger.info(String.format("[repair #%s] Received merkle tree for %s from %s", session.getName(), request.cf.right, request.endpoint));

        if (job.addTree(request, tree) == 0)
        {
            logger.debug("All trees received for " + session.getName() + "/" + request.cf.right);
            job.submitDifferencers();

            // This job is complete, switching to next in line (note that only
            // one thread will can ever do this)
            session.jobs.poll();
            RepairSession.RepairJob nextJob = session.jobs.peek();
            if (nextJob == null)
                // We are done with this repair session as far as differencing
                // is considern. Just inform the session
                session.differencingDone.signalAll();
            else
                nextJob.sendTreeRequests();
        }
    }

    /**
     * Requests a tree from the given node, and returns the request that was sent.
     */
    TreeRequest request(String sessionid, InetAddress remote, Range range, String ksname, String cfname)
    {
        TreeRequest request = new TreeRequest(sessionid, remote, range, new CFPair(ksname, cfname));
        MessagingService.instance().sendOneWay(TreeRequestVerbHandler.makeVerb(request, Gossiper.instance.getVersion(remote)), remote);
        return request;
    }

    /**
     * Responds to the node that requested the given valid tree.
     * @param validator A locally generated validator
     * @param local localhost (parameterized for testing)
     */
    void respond(Validator validator, InetAddress local)
    {
        MessagingService ms = MessagingService.instance();

        try
        {
            Message message = TreeResponseVerbHandler.makeVerb(local, validator);
            if (!validator.request.endpoint.equals(FBUtilities.getBroadcastAddress()))
                logger.info(String.format("[repair #%s] Sending completed merkle tree to %s for %s", validator.request.sessionid, validator.request.endpoint, validator.request.cf));
            ms.sendOneWay(message, validator.request.endpoint);
        }
        catch (Exception e)
        {
            logger.error(String.format("[repair #%s] Error sending completed merkle tree to %s for %s ", validator.request.sessionid, validator.request.endpoint, validator.request.cf), e);
        }
    }

    /**
     * A Strategy to handle building and validating a merkle tree for a column family.
     *
     * Lifecycle:
     * 1. prepare() - Initialize tree with samples.
     * 2. add() - 0 or more times, to add hashes to the tree.
     * 3. complete() - Enqueues any operations that were blocked waiting for a valid tree.
     */
    public static class Validator implements Runnable
    {
        public final TreeRequest request;
        public final MerkleTree tree;

        // null when all rows with the min token have been consumed
        private transient long validated;
        private transient MerkleTree.TreeRange range;
        private transient MerkleTree.TreeRangeIterator ranges;
        private transient DecoratedKey lastKey;

        public final static MerkleTree.RowHash EMPTY_ROW = new MerkleTree.RowHash(null, new byte[0]);
        
        Validator(TreeRequest request)
        {
            this(request,
                 // TODO: memory usage (maxsize) should either be tunable per
                 // CF, globally, or as shared for all CFs in a cluster
                 new MerkleTree(DatabaseDescriptor.getPartitioner(), request.range, MerkleTree.RECOMMENDED_DEPTH, (int)Math.pow(2, 15)));
        }

        Validator(TreeRequest request, MerkleTree tree)
        {
            this.request = request;
            this.tree = tree;
            // Reestablishing the range because we don't serialize it (for bad
            // reason - see MerkleTree for details)
            this.tree.fullRange = this.request.range;
            validated = 0;
            range = null;
            ranges = null;
        }

        public void prepare(ColumnFamilyStore cfs)
        {
            if (tree.partitioner() instanceof RandomPartitioner)
            {
                // You can't beat an even tree distribution for md5
                tree.init();
            }
            else
            {
                List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
                for (DecoratedKey sample : cfs.keySamples(request.range))
                {
                    assert request.range.contains(sample.token): "Token " + sample.token + " is not within range " + request.range;
                    keys.add(sample);
                }

                if (keys.isEmpty())
                {
                    // use an even tree distribution
                    tree.init();
                }
                else
                {
                    int numkeys = keys.size();
                    Random random = new Random();
                    // sample the column family using random keys from the index
                    while (true)
                    {
                        DecoratedKey dk = keys.get(random.nextInt(numkeys));
                        if (!tree.split(dk.token))
                            break;
                    }
                }
            }
            logger.debug("Prepared AEService tree of size " + tree.size() + " for " + request);
            ranges = tree.invalids();
        }

        /**
         * Called (in order) for every row present in the CF.
         * Hashes the row, and adds it to the tree being built.
         *
         * There are four possible cases:
         *  1. Token is greater than range.right (we haven't generated a range for it yet),
         *  2. Token is less than/equal to range.left (the range was valid),
         *  3. Token is contained in the range (the range is in progress),
         *  4. No more invalid ranges exist.
         *
         * TODO: Because we only validate completely empty trees at the moment, we
         * do not bother dealing with case 2 and case 4 should result in an error.
         *
         * Additionally, there is a special case for the minimum token, because
         * although it sorts first, it is contained in the last possible range.
         *
         * @param row The row.
         */
        public void add(AbstractCompactedRow row)
        {
            assert request.range.contains(row.key.token) : row.key.token + " is not contained in " + request.range;
            assert lastKey == null || lastKey.compareTo(row.key) < 0
                   : "row " + row.key + " received out of order wrt " + lastKey;
            lastKey = row.key;

            if (range == null)
                range = ranges.next();

            // generate new ranges as long as case 1 is true
            while (!range.contains(row.key.token))
            {
                // add the empty hash, and move to the next range
                range.addHash(EMPTY_ROW);
                range = ranges.next();
            }

            // case 3 must be true: mix in the hashed row
            range.addHash(rowHash(row));
        }

        private MerkleTree.RowHash rowHash(AbstractCompactedRow row)
        {
            validated++;
            // MerkleTree uses XOR internally, so we want lots of output bits here
            MessageDigest digest = FBUtilities.newMessageDigest("SHA-256");
            row.update(digest);
            return new MerkleTree.RowHash(row.key.token, digest.digest());
        }

        /**
         * Registers the newly created tree for rendezvous in Stage.ANTIENTROPY.
         */
        public void complete()
        {
            completeTree();

            StageManager.getStage(Stage.ANTI_ENTROPY).execute(this);
            logger.debug("Validated " + validated + " rows into AEService tree for " + request);
        }

        void completeTree()
        {
            assert ranges != null : "Validator was not prepared()";

            if (range != null)
                range.addHash(EMPTY_ROW);
            while (ranges.hasNext())
            {
                range = ranges.next();
                range.addHash(EMPTY_ROW);
            }
        }

        /**
         * Called after the validation lifecycle to respond with the now valid tree. Runs in Stage.ANTIENTROPY.
         */
        public void run()
        {
            // respond to the request that triggered this validation
            AntiEntropyService.instance.respond(this, FBUtilities.getBroadcastAddress());
        }
    }

    /**
     * Handler for requests from remote nodes to generate a valid tree.
     * The payload is a CFPair representing the columnfamily to validate.
     */
    public static class TreeRequestVerbHandler implements IVerbHandler
    {
        public static final TreeRequestVerbHandler SERIALIZER = new TreeRequestVerbHandler();
        static Message makeVerb(TreeRequest request, int version)
        {
            try
            {
            	FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                SERIALIZER.serialize(request, dos, version);
                return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.TREE_REQUEST, bos.toByteArray(), version);
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void serialize(TreeRequest request, DataOutput dos, int version) throws IOException
        {
            dos.writeUTF(request.sessionid);
            CompactEndpointSerializationHelper.serialize(request.endpoint, dos);
            dos.writeUTF(request.cf.left);
            dos.writeUTF(request.cf.right);
            if (version > MessagingService.VERSION_07)
                AbstractBounds.serializer().serialize(request.range, dos);
        }

        public TreeRequest deserialize(DataInput dis, int version) throws IOException
        {
            String sessId = dis.readUTF();
            InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(dis);
            CFPair cfpair = new CFPair(dis.readUTF(), dis.readUTF());
            Range range;
            if (version > MessagingService.VERSION_07)
                range = (Range) AbstractBounds.serializer().deserialize(dis);
            else
                range = new Range(StorageService.getPartitioner().getMinimumToken(), StorageService.getPartitioner().getMinimumToken());

            return new TreeRequest(sessId, endpoint, range, cfpair);
        }

        /**
         * Trigger a validation compaction which will return the tree upon completion.
         */
        public void doVerb(Message message, String id)
        { 
            byte[] bytes = message.getMessageBody();
            
            DataInputStream buffer = new DataInputStream(new FastByteArrayInputStream(bytes));
            try
            {
                TreeRequest remotereq = this.deserialize(buffer, message.getVersion());
                TreeRequest request = new TreeRequest(remotereq.sessionid, message.getFrom(), remotereq.range, remotereq.cf);

                // trigger readonly-compaction
                ColumnFamilyStore store = Table.open(request.cf.left).getColumnFamilyStore(request.cf.right);
                Validator validator = new Validator(request);
                logger.debug("Queueing validation compaction for " + request);
                CompactionManager.instance.submitValidation(store, validator);
            }
            catch (IOException e)
            {
                throw new IOError(e);            
            }
        }
    }

    /**
     * Handler for responses from remote nodes which contain a valid tree.
     * The payload is a completed Validator object from the remote endpoint.
     */
    public static class TreeResponseVerbHandler implements IVerbHandler
    {
        public static final TreeResponseVerbHandler SERIALIZER = new TreeResponseVerbHandler();
        static Message makeVerb(InetAddress local, Validator validator)
        {
            try
            {
            	FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                SERIALIZER.serialize(validator, dos, Gossiper.instance.getVersion(validator.request.endpoint));
                return new Message(local, 
                                   StorageService.Verb.TREE_RESPONSE, 
                                   bos.toByteArray(), 
                                   Gossiper.instance.getVersion(validator.request.endpoint));
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void serialize(Validator v, DataOutputStream dos, int version) throws IOException
        {
            TreeRequestVerbHandler.SERIALIZER.serialize(v.request, dos, version);
            MerkleTree.serializer.serialize(v.tree, dos, version);
            dos.flush();
        }

        public Validator deserialize(DataInputStream dis, int version) throws IOException
        {
            final TreeRequest request = TreeRequestVerbHandler.SERIALIZER.deserialize(dis, version);
            try
            {
                return new Validator(request, MerkleTree.serializer.deserialize(dis, version));
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public void doVerb(Message message, String id)
        { 
            byte[] bytes = message.getMessageBody();
            DataInputStream buffer = new DataInputStream(new FastByteArrayInputStream(bytes));

            try
            {
                // deserialize the remote tree, and register it
                Validator response = this.deserialize(buffer, message.getVersion());
                TreeRequest request = new TreeRequest(response.request.sessionid, message.getFrom(), response.request.range, response.request.cf);
                AntiEntropyService.instance.rendezvous(request, response.tree);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    /**
     * A tuple of table and cf.
     */
    static class CFPair extends Pair<String,String>
    {
        public CFPair(String table, String cf)
        {
            super(table, cf);
            assert table != null && cf != null;
        }
    }

    /**
     * A tuple of table, cf, address and range that represents a location we have an outstanding TreeRequest for.
     */
    public static class TreeRequest
    {
        public final String sessionid;
        public final InetAddress endpoint;
        public final Range range;
        public final CFPair cf;

        public TreeRequest(String sessionid, InetAddress endpoint, Range range, CFPair cf)
        {
            this.sessionid = sessionid;
            this.endpoint = endpoint;
            this.cf = cf;
            this.range = range;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hashCode(sessionid, endpoint, cf, range);
        }
        
        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof TreeRequest))
                return false;
            TreeRequest that = (TreeRequest)o;
            // handles nulls properly
            return Objects.equal(sessionid, that.sessionid) && Objects.equal(endpoint, that.endpoint) && Objects.equal(cf, that.cf) && Objects.equal(range, that.range);
        }
        
        @Override
        public String toString()
        {
            return "#<TreeRequest " + sessionid + ", " + endpoint + ", " + cf + ", " + range + ">";
        }
    }

    /**
     * Triggers repairs with all neighbors for the given table, cfs and range.
     * Typical lifecycle is: start() then join(). Executed in client threads.
     */
    class RepairSession extends WrappedRunnable implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
    {
        private final String sessionName;
        private final String tablename;
        private final String[] cfnames;
        private final Range range;
        private volatile Exception exception;
        private final AtomicBoolean isFailed = new AtomicBoolean(false);

        private final Set<InetAddress> endpoints;
        final Queue<RepairJob> jobs = new ConcurrentLinkedQueue<RepairJob>();
        final Map<String, RepairJob> activeJobs = new ConcurrentHashMap<String, RepairJob>();

        private final SimpleCondition completed = new SimpleCondition();
        public final Condition differencingDone = new SimpleCondition();

        private volatile boolean terminated = false;

        public RepairSession(TreeRequest req, String tablename, String... cfnames)
        {
            this(req.sessionid, req.range, tablename, cfnames);
            AntiEntropyService.instance.sessions.put(getName(), this);
        }

        public RepairSession(Range range, String tablename, String... cfnames)
        {
            this(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress()).toString(), range, tablename, cfnames);
        }

        private RepairSession(String id, Range range, String tablename, String[] cfnames)
        {
            this.sessionName = id;
            this.tablename = tablename;
            this.cfnames = cfnames;
            assert cfnames.length > 0 : "Repairing no column families seems pointless, doesn't it";
            this.range = range;
            this.endpoints = AntiEntropyService.getNeighbors(tablename, range);
        }

        public String getName()
        {
            return sessionName;
        }

        RepairFuture getFuture()
        {
            return new RepairFuture(this);
        }

        private String repairedNodes()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(FBUtilities.getBroadcastAddress());
            for (InetAddress ep : endpoints)
                sb.append(", ").append(ep);
            return sb.toString();
        }

        // we don't care about the return value but care about it throwing exception
        public void runMayThrow() throws Exception
        {
            logger.info(String.format("[repair #%s] new session: will sync %s on range %s for %s.%s", getName(), repairedNodes(), range, tablename, Arrays.toString(cfnames)));

            if (endpoints.isEmpty())
            {
                differencingDone.signalAll();
                logger.info("[repair #%s] No neighbors to repair with on range %s: session completed", getName(), range);
                return;
            }

            // Checking all nodes are live
            for (InetAddress endpoint : endpoints)
            {
                if (!FailureDetector.instance.isAlive(endpoint))
                {
                    differencingDone.signalAll();
                    logger.info(String.format("[repair #%s] Cannot proceed on repair because a neighbor (%s) is dead: session failed", getName(), endpoint));
                    return;
                }
            }

            AntiEntropyService.instance.sessions.put(getName(), this);
            Gossiper.instance.register(this);
            FailureDetector.instance.registerFailureDetectionEventListener(this);
            try
            {
                // Create and queue a RepairJob for each column family
                for (String cfname : cfnames)
                {
                    RepairJob job = new RepairJob(cfname);
                    jobs.offer(job);
                    activeJobs.put(cfname, job);
                }

                jobs.peek().sendTreeRequests();

                // block whatever thread started this session until all requests have been returned:
                // if this thread dies, the session will still complete in the background
                completed.await();
                if (exception == null)
                {
                    logger.info(String.format("[repair #%s] session completed successfully", getName()));
                }
                else
                {
                    logger.error(String.format("[repair #%s] session completed with the following error", getName()), exception);
                    throw exception;
                }
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted while waiting for repair.");
            }
            finally
            {
                // mark this session as terminated
                terminate();
                FailureDetector.instance.unregisterFailureDetectionEventListener(this);
                Gossiper.instance.unregister(this);
                AntiEntropyService.instance.sessions.remove(getName());
            }
        }

        /**
         * @return wheather this session is terminated
         */
        public boolean terminated()
        {
            return terminated;
        }

        public void terminate()
        {
            terminated = true;
            jobs.clear();
            activeJobs.clear();
        }

        /**
         * clear all RepairJobs and terminate this session.
         */
        public void forceShutdown()
        {
            differencingDone.signalAll();
            completed.signalAll();
        }

        void completed(Differencer differencer)
        {
            logger.debug(String.format("[repair #%s] Repair completed between %s and %s on %s",
                                       getName(),
                                       differencer.r1.endpoint,
                                       differencer.r2.endpoint,
                                       differencer.cfname));
            RepairJob job = activeJobs.get(differencer.cfname);
            if (job == null)
            {
                assert terminated;
                return;
            }

            if (job.completedSynchronization(differencer))
            {
                activeJobs.remove(differencer.cfname);
                String remaining = activeJobs.size() == 0 ? "" : String.format(" (%d remaining column family to sync for this session)", activeJobs.size());
                logger.info(String.format("[repair #%s] %s is fully synced%s", getName(), differencer.cfname, remaining));
                if (activeJobs.isEmpty())
                    completed.signalAll();
            }
        }

        void failedNode(InetAddress remote)
        {
            String errorMsg = String.format("Endpoint %s died", remote);
            exception = new IOException(errorMsg);
            // If a node failed, we stop everything (though there could still be some activity in the background)
            forceShutdown();
        }

        public void onJoin(InetAddress endpoint, EndpointState epState) {}
        public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
        public void onAlive(InetAddress endpoint, EndpointState state) {}
        public void onDead(InetAddress endpoint, EndpointState state) {}

        public void onRemove(InetAddress endpoint)
        {
            convict(endpoint, Double.MAX_VALUE);
        }

        public void onRestart(InetAddress endpoint, EndpointState epState)
        {
            convict(endpoint, Double.MAX_VALUE);
        }

        public void convict(InetAddress endpoint, double phi)
        {
            if (!endpoints.contains(endpoint))
                return;

            // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
            if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
                return;

            // Though unlikely, it is possible to arrive here multiple time and we
            // want to avoid print an error message twice
            if (!isFailed.compareAndSet(false, true))
                return;

            failedNode(endpoint);
        }

        class RepairJob
        {
            private final String cfname;
            // first we send tree requests.  this tracks the endpoints remaining to hear from
            private final Set<InetAddress> remainingEndpoints = new HashSet<InetAddress>();
            // tree responses are then tracked here
            private final List<TreeResponse> trees = new ArrayList<TreeResponse>(endpoints.size() + 1);
            // once all responses are received, each tree is compared with each other, and differencer tasks
            // are submitted.  the job is done when all differencers are complete.
            private final Set<Differencer> remainingDifferencers = new HashSet<Differencer>();
            private final Condition requestsSent = new SimpleCondition();

            public RepairJob(String cfname)
            {
                this.cfname = cfname;
            }

            /**
             * Send merkle tree request to every involved neighbor.
             */
            public void sendTreeRequests()
            {
                remainingEndpoints.addAll(endpoints);
                remainingEndpoints.add(FBUtilities.getBroadcastAddress());

                // send requests to all nodes
                for (InetAddress endpoint : remainingEndpoints)
                    AntiEntropyService.instance.request(getName(), endpoint, range, tablename, cfname);

                logger.info(String.format("[repair #%s] requests for merkle tree sent for %s (to %s)", getName(), cfname, remainingEndpoints));
                requestsSent.signalAll();
            }

            /**
             * Add a new received tree and return the number of remaining tree to
             * be received for the job to be complete.
             *
             * Callers may assume exactly one addTree call will result in zero remaining endpoints.
             */
            public synchronized int addTree(TreeRequest request, MerkleTree tree)
            {
                // Wait for all request to have been performed (see #3400)
                try
                {
                    requestsSent.await();
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError("Interrupted while waiting for requests to be sent");
                }

                assert request.cf.right.equals(cfname);
                trees.add(new TreeResponse(request.endpoint, tree));
                remainingEndpoints.remove(request.endpoint);
                return remainingEndpoints.size();
            }

            /**
             * Submit differencers for running.
             * All tree *must* have been received before this is called.
             */
            public void submitDifferencers()
            {
                assert remainingEndpoints.isEmpty();

                // We need to difference all trees one against another
                for (int i = 0; i < trees.size() - 1; ++i)
                {
                    TreeResponse r1 = trees.get(i);
                    for (int j = i + 1; j < trees.size(); ++j)
                    {
                        TreeResponse r2 = trees.get(j);
                        Differencer differencer = new Differencer(cfname, r1, r2);
                        logger.debug("Queueing comparison {}", differencer);
                        remainingDifferencers.add(differencer);
                        StageManager.getStage(Stage.ANTI_ENTROPY).execute(differencer);
                    }
                }
                trees.clear(); // allows gc to do its thing
            }

            /**
             * @return true if the @param differencer was the last remaining
             */
            synchronized boolean completedSynchronization(Differencer differencer)
            {
                remainingDifferencers.remove(differencer);
                return remainingDifferencers.isEmpty();
            }
        }

        /**
         * Runs on the node that initiated a request to compare two trees, and launch repairs for disagreeing ranges.
         */
        class Differencer implements Runnable
        {
            public final String cfname;
            public final TreeResponse r1;
            public final TreeResponse r2;
            public List<Range> differences;

            Differencer(String cfname, TreeResponse r1, TreeResponse r2)
            {
                this.cfname = cfname;
                this.r1 = r1;
                this.r2 = r2;
                this.differences = new ArrayList<Range>();
            }

            /**
             * Compares our trees, and triggers repairs for any ranges that mismatch.
             */
            public void run()
            {
                // restore partitioners (in case we were serialized)
                if (r1.tree.partitioner() == null)
                    r1.tree.partitioner(StorageService.getPartitioner());
                if (r2.tree.partitioner() == null)
                    r2.tree.partitioner(StorageService.getPartitioner());

                // compare trees, and collect differences
                differences.addAll(MerkleTree.difference(r1.tree, r2.tree));

                // choose a repair method based on the significance of the difference
                String format = String.format("[repair #%s] Endpoints %s and %s %%s for %s", getName(), r1.endpoint, r2.endpoint, cfname);
                if (differences.isEmpty())
                {
                    logger.info(String.format(format, "are consistent"));
                    completed(this);
                    return;
                }

                // non-0 difference: perform streaming repair
                logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
                performStreamingRepair();
            }

            /**
             * Starts sending/receiving our list of differences to/from the remote endpoint: creates a callback
             * that will be called out of band once the streams complete.
             */
            void performStreamingRepair()
            {
                Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        completed(Differencer.this);
                    }
                };
                StreamingRepairTask task = StreamingRepairTask.create(r1.endpoint, r2.endpoint, tablename, cfname, differences, callback);

                // Pre 1.0, nodes don't know how to handle forwarded streaming task so don't bother
                if (task.isLocalTask() || Gossiper.instance.getVersion(task.dst) >= MessagingService.VERSION_10)
                    task.run();
            }

            public String toString()
            {
                return "#<Differencer " + r1.endpoint + "<->" + r2.endpoint + "/" + range + ">";
            }
        }
    }

    static class TreeResponse
    {
        public final InetAddress endpoint;
        public final MerkleTree tree;

        TreeResponse(InetAddress endpoint, MerkleTree tree)
        {
            this.endpoint = endpoint;
            this.tree = tree;
        }
    }

    public static class RepairFuture extends FutureTask
    {
        public final RepairSession session;

        RepairFuture(RepairSession session)
        {
            super(session, null);
            this.session = session;
        }
    }
}
