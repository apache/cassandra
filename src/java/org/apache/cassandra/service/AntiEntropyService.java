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
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.streaming.StreamIn;
import org.apache.cassandra.streaming.StreamOut;
import org.apache.cassandra.streaming.StreamOutSession;
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
        neighbors.remove(FBUtilities.getLocalAddress());
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
        assert session != null;

        RepairSession.RepairJob job = session.jobs.peek();
        assert job != null : "A repair should have at least some jobs scheduled";

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
            logger.info("Sending AEService tree for " + validator.request);
            ms.sendOneWay(message, validator.request.endpoint);
        }
        catch (Exception e)
        {
            logger.error("Could not send valid tree for request " + validator.request, e);
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

        // the minimum token sorts first, but falls into the last range
        private transient List<MerkleTree.RowHash> minrows;
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
            minrows = new ArrayList<MerkleTree.RowHash>();
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
                    assert request.range.contains(sample.token);
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
         *
         * @return A meaningless object.
         */
        public void run()
        {
            // respond to the request that triggered this validation
            AntiEntropyService.instance.respond(this, FBUtilities.getLocalAddress());
        }
    }

    /**
     * Handler for requests from remote nodes to generate a valid tree.
     * The payload is a CFPair representing the columnfamily to validate.
     */
    public static class TreeRequestVerbHandler implements IVerbHandler, ICompactSerializer<TreeRequest>
    {
        public static final TreeRequestVerbHandler SERIALIZER = new TreeRequestVerbHandler();
        static Message makeVerb(TreeRequest request, int version)
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                SERIALIZER.serialize(request, dos, version);
                return new Message(FBUtilities.getLocalAddress(), StorageService.Verb.TREE_REQUEST, bos.toByteArray(), version);
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void serialize(TreeRequest request, DataOutputStream dos, int version) throws IOException
        {
            dos.writeUTF(request.sessionid);
            CompactEndpointSerializationHelper.serialize(request.endpoint, dos);
            dos.writeUTF(request.cf.left);
            dos.writeUTF(request.cf.right);
            if (version > MessagingService.VERSION_07)
                AbstractBounds.serializer().serialize(request.range, dos);
        }

        public TreeRequest deserialize(DataInputStream dis, int version) throws IOException
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
            
            DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(bytes));
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
    public static class TreeResponseVerbHandler implements IVerbHandler, ICompactSerializer<Validator>
    {
        public static final TreeResponseVerbHandler SERIALIZER = new TreeResponseVerbHandler();
        static Message makeVerb(InetAddress local, Validator validator)
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
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
            DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(bytes));

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
     * A tuple of a local and remote tree.
     */
    static class TreePair extends Pair<MerkleTree,MerkleTree>
    {
        public TreePair(MerkleTree local, MerkleTree remote)
        {
            super(local, remote);
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

        public RepairSession(TreeRequest req, String tablename, String... cfnames)
        {
            this(req.sessionid, req.range, tablename, cfnames);
            AntiEntropyService.instance.sessions.put(getName(), this);
        }

        public RepairSession(Range range, String tablename, String... cfnames)
        {
            this("manual-repair-" + UUID.randomUUID(), range, tablename, cfnames);
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

        // we don't care about the return value but care about it throwing exception
        public void runMayThrow() throws Exception
        {
            if (endpoints.isEmpty())
            {
                differencingDone.signalAll();
                logger.info("No neighbors to repair with for " + tablename + " on " + range + ": " + getName() + " completed.");
                return;
            }

            // Checking all nodes are live
            for (InetAddress endpoint : endpoints)
            {
                if (!FailureDetector.instance.isAlive(endpoint))
                {
                    differencingDone.signalAll();
                    logger.info("Could not proceed on repair because a neighbor (" + endpoint + ") is dead: " + getName() + " failed.");
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
                if (exception != null)
                    throw exception;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted while waiting for repair: repair will continue in the background.");
            }
            finally
            {
                FailureDetector.instance.unregisterFailureDetectionEventListener(this);
                Gossiper.instance.unregister(this);
                AntiEntropyService.instance.sessions.remove(getName());
            }
        }

        void completed(InetAddress remote, String cfname)
        {
            logger.debug("Repair completed for {} on {}", remote, cfname);
            RepairJob job = activeJobs.get(cfname);
            if (job.completedSynchronizationJob(remote))
            {
                activeJobs.remove(cfname);
                if (activeJobs.isEmpty())
                    completed.signalAll();
            }
        }

        void failedNode(InetAddress remote)
        {
            String errorMsg = String.format("Problem during repair session %s, endpoint %s died", sessionName, remote);
            logger.error(errorMsg);
            exception = new IOException(errorMsg);
            // If a node failed, we stop everything (though there could still be some activity in the background)
            jobs.clear();
            activeJobs.clear();
            differencingDone.signalAll();
            completed.signalAll();
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
            private final Set<InetAddress> requestedEndpoints = new HashSet<InetAddress>();
            private final Map<InetAddress, MerkleTree> trees = new HashMap<InetAddress, MerkleTree>();
            private final Set<InetAddress> syncJobs = new HashSet<InetAddress>();

            public RepairJob(String cfname)
            {
                this.cfname = cfname;
            }

            /**
             * Send merkle tree request to every involved neighbor.
             */
            public void sendTreeRequests()
            {
                requestedEndpoints.addAll(endpoints);
                requestedEndpoints.add(FBUtilities.getLocalAddress());

                // send requests to all nodes
                for (InetAddress endpoint : requestedEndpoints)
                    AntiEntropyService.instance.request(getName(), endpoint, range, tablename, cfname);
            }

            /**
             * Add a new received tree and return the number of remaining tree to
             * be received for the job to be complete.
             */
            public synchronized int addTree(TreeRequest request, MerkleTree tree)
            {
                assert request.cf.right.equals(cfname);
                trees.put(request.endpoint, tree);
                requestedEndpoints.remove(request.endpoint);
                return requestedEndpoints.size();
            }

            /**
             * Submit differencers for running.
             * All tree *must* have been received before this is called.
             */
            public void submitDifferencers()
            {
                assert requestedEndpoints.size() == 0;

                // Right now, we only difference local host against each other. CASSANDRA-2610 will fix that.
                // In the meantime ugly special casing will work good enough.
                MerkleTree localTree = trees.get(FBUtilities.getLocalAddress());
                assert localTree != null;
                for (Map.Entry<InetAddress, MerkleTree> entry : trees.entrySet())
                {
                    if (entry.getKey().equals(FBUtilities.getLocalAddress()))
                        continue;

                    Differencer differencer = new Differencer(cfname, entry.getKey(), entry.getValue(), localTree);
                    syncJobs.add(entry.getKey());
                    logger.debug("Queueing comparison " + differencer);
                    StageManager.getStage(Stage.ANTI_ENTROPY).execute(differencer);
                }
                trees.clear(); // allows gc to do its thing
            }

            synchronized boolean completedSynchronizationJob(InetAddress remote)
            {
                syncJobs.remove(remote);
                return syncJobs.isEmpty();
            }
        }

        /**
         * Runs on the node that initiated a request to compare two trees, and launch repairs for disagreeing ranges.
         */
        class Differencer implements Runnable
        {
            public final String cfname;
            public final InetAddress remote;
            public final MerkleTree ltree;
            public final MerkleTree rtree;
            public List<Range> differences;

            Differencer(String cfname, InetAddress remote, MerkleTree ltree, MerkleTree rtree)
            {
                this.cfname = cfname;
                this.remote = remote;
                this.ltree = ltree;
                this.rtree = rtree;
                this.differences = new ArrayList<Range>();
            }

            /**
             * Compares our trees, and triggers repairs for any ranges that mismatch.
             */
            public void run()
            {
                InetAddress local = FBUtilities.getLocalAddress();

                // restore partitioners (in case we were serialized)
                if (ltree.partitioner() == null)
                    ltree.partitioner(StorageService.getPartitioner());
                if (rtree.partitioner() == null)
                    rtree.partitioner(StorageService.getPartitioner());

                // compare trees, and collect differences
                differences.addAll(MerkleTree.difference(ltree, rtree));

                // choose a repair method based on the significance of the difference
                String format = "Endpoints " + local + " and " + remote + " %s for " + cfname + " on " + range;
                if (differences.isEmpty())
                {
                    logger.info(String.format(format, "are consistent"));
                    completed(remote, cfname);
                    return;
                }

                // non-0 difference: perform streaming repair
                logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
                try
                {
                    performStreamingRepair();
                }
                catch(IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            /**
             * Starts sending/receiving our list of differences to/from the remote endpoint: creates a callback
             * that will be called out of band once the streams complete.
             */
            void performStreamingRepair() throws IOException
            {
                logger.info("Performing streaming repair of " + differences.size() + " ranges with " + remote + " for " + range);
                ColumnFamilyStore cfstore = Table.open(tablename).getColumnFamilyStore(cfname);
                try
                {
                    Collection<SSTableReader> sstables = cfstore.getSSTables();
                    Callback callback = new Callback();
                    // send ranges to the remote node
                    StreamOutSession outsession = StreamOutSession.create(tablename, remote, callback);
                    StreamOut.transferSSTables(outsession, sstables, differences, OperationType.AES);
                    // request ranges from the remote node
                    StreamIn.requestRanges(remote, tablename, differences, callback, OperationType.AES);
                }
                catch(Exception e)
                {
                    throw new IOException("Streaming repair failed.", e);
                }
            }

            public String toString()
            {
                return "#<Differencer " + remote + "/" + range + ">";
            }

            /**
             * When a repair is necessary, this callback is created to wait for the inbound
             * and outbound streams to complete.
             */
            class Callback extends WrappedRunnable
            {
                // we expect one callback for the receive, and one for the send
                private final AtomicInteger outstanding = new AtomicInteger(2);

                protected void runMayThrow() throws Exception
                {
                    if (outstanding.decrementAndGet() > 0)
                        // waiting on more calls
                        return;

                    // all calls finished successfully
                    completed(remote, cfname);
                    logger.info(String.format("Finished streaming repair with %s for %s", remote, range));
                }
            }
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
