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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Objects;
import org.apache.cassandra.gms.Gossiper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.AbstractCompactedRow;
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

    // timeout for outstanding requests (48 hours)
    public final static long REQUEST_TIMEOUT = 48*60*60*1000;

    /**
     * Map of outstanding sessions to requests. Once both trees reach the rendezvous, the local node
     * will queue a Differencer to compare them.
     *
     * This map is only accessed from Stage.ANTIENTROPY, so it is not synchronized.
     */
    private final ExpiringMap<String, Map<TreeRequest, TreePair>> requests;

    /**
     * A map of repair session ids to a Queue of TreeRequests that have been performed since the session was started.
     */
    private final ConcurrentMap<String, RepairSession.Callback> sessions;

    /**
     * Protected constructor. Use AntiEntropyService.instance.
     */
    protected AntiEntropyService()
    {
        requests = new ExpiringMap<String, Map<TreeRequest, TreePair>>(REQUEST_TIMEOUT);
        sessions = new ConcurrentHashMap<String, RepairSession.Callback>();
    }

    /**
     * Requests repairs for the given table and column families, and blocks until all repairs have been completed.
     * TODO: Should add retries: if nodes go offline before they respond to the requests, this could block forever.
     */
    public RepairSession getRepairSession(Range range, String tablename, String... cfnames)
    {
        return new RepairSession(range, tablename, cfnames);
    }
    
    RepairSession getArtificialRepairSession(TreeRequest req, String tablename, String... cfnames)
    {
        return new RepairSession(req, tablename, cfnames);
    }

    /**
     * Called by Differencer when a full repair round trip has been completed between the given CF and endpoints.
     */
    void completedRequest(TreeRequest request)
    {
        // indicate to the waiting session that this request completed
        sessions.get(request.sessionid).completed(request);
    }

    /**
     * Returns the map of waiting rendezvous endpoints to trees for the given session.
     * Should only be called within Stage.ANTIENTROPY.
     */
    private Map<TreeRequest, TreePair> rendezvousPairs(String sessionid)
    {
        Map<TreeRequest, TreePair> ctrees = requests.get(sessionid);
        if (ctrees == null)
        {
            ctrees = new HashMap<TreeRequest, TreePair>();
            requests.put(sessionid, ctrees);
        }
        return ctrees;
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
        for (InetAddress endpoint : neighbors)
        {
            if (Gossiper.instance.getVersion(endpoint) <= MessagingService.VERSION_07)
            {
                logger.info("Excluding " + endpoint + " from repair because it is on version 0.7 or sooner. You should consider updating this node before running repair again.");
                neighbors.remove(endpoint);
            }
        }
        return neighbors;
    }

    /**
     * Register a tree for the given request to be compared to the appropriate trees in Stage.ANTIENTROPY when they become available.
     */
    private void rendezvous(TreeRequest request, MerkleTree tree)
    {
        InetAddress LOCAL = FBUtilities.getLocalAddress();

        // the rendezvous pairs for this session
        Map<TreeRequest, TreePair> ctrees = rendezvousPairs(request.sessionid);

        List<Differencer> differencers = new ArrayList<Differencer>();
        if (LOCAL.equals(request.endpoint))
        {
            // we're registering a local tree: rendezvous with remote requests for the session
            for (InetAddress neighbor : getNeighbors(request.cf.left, request.range))
            {
                TreeRequest remotereq = new TreeRequest(request.sessionid, neighbor, request.range, request.cf);
                TreePair waiting = ctrees.remove(remotereq);
                if (waiting != null && waiting.right != null)
                {
                    // the neighbor beat us to the rendezvous: queue differencing
                    // FIXME: Differencer should take a TreeRequest
                    differencers.add(new Differencer(remotereq, tree, waiting.right));
                    continue;
                }

                // else, the local tree is first to the rendezvous: store and wait
                ctrees.put(remotereq, new TreePair(tree, null));
                logger.debug("Stored local tree for " + request + " to wait for " + remotereq);
            }
        }
        else
        {
            // we're registering a remote tree: rendezvous with the local tree
            TreePair waiting = ctrees.remove(request);
            if (waiting != null && waiting.left != null)
            {
                // the local tree beat us to the rendezvous: queue differencing
                differencers.add(new Differencer(request, waiting.left, tree));
            }
            else
            {
                // else, the remote tree is first to the rendezvous: store and wait
                ctrees.put(request, new TreePair(null, tree));
                logger.debug("Stored remote tree for " + request + " to wait for local tree.");
            }
        }

        for (Differencer differencer : differencers)
        {
            logger.info("Queueing comparison " + differencer);
            StageManager.getStage(Stage.ANTI_ENTROPY).execute(differencer);
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
            assert ranges != null : "Validator was not prepared()";

            if (range != null)
                range.addHash(EMPTY_ROW);
            while (ranges.hasNext())
            {
                range = ranges.next();
                range.addHash(EMPTY_ROW);
            }

            StageManager.getStage(Stage.ANTI_ENTROPY).execute(this);
            logger.debug("Validated " + validated + " rows into AEService tree for " + request);
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
     * Runs on the node that initiated a request to compare two trees, and launch repairs for disagreeing ranges.
     */
    public static class Differencer implements Runnable
    {
        public final TreeRequest request;
        public final MerkleTree ltree;
        public final MerkleTree rtree;
        public List<Range> differences;

        public Differencer(TreeRequest request, MerkleTree ltree, MerkleTree rtree)
        {
            this.request = request;
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
            String format = "Endpoints " + local + " and " + request.endpoint + " %s for " + request.cf + " on " + request.range;
            if (differences.isEmpty())
            {
                logger.info(String.format(format, "are consistent"));
                AntiEntropyService.instance.completedRequest(request);
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
            logger.info("Performing streaming repair of " + differences.size() + " ranges for " + request);
            ColumnFamilyStore cfstore = Table.open(request.cf.left).getColumnFamilyStore(request.cf.right);
            try
            {
                Collection<SSTableReader> sstables = cfstore.getSSTables();
                Callback callback = new Callback();
                // send ranges to the remote node
                StreamOutSession outsession = StreamOutSession.create(request.cf.left, request.endpoint, callback);
                StreamOut.transferSSTables(outsession, sstables, differences, OperationType.AES);
                // request ranges from the remote node
                StreamIn.requestRanges(request.endpoint, request.cf.left, differences, callback, OperationType.AES);
            }
            catch(Exception e)
            {
                throw new IOException("Streaming repair failed.", e);
            }
        }

        public String toString()
        {
            return "#<Differencer " + request + ">";
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
                logger.info("Finished streaming repair for " + request);
                AntiEntropyService.instance.completedRequest(request);
            }
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
    class RepairSession extends Thread
    {
        private final String tablename;
        private final String[] cfnames;
        private final SimpleCondition requestsMade;
        private final ConcurrentHashMap<TreeRequest,Object> requests;
        private final Range range;
        
        public RepairSession(TreeRequest req, String tablename, String... cfnames)
        {
            super(req.sessionid);
            this.range = req.range;
            this.tablename = tablename;
            this.cfnames = cfnames;
            requestsMade = new SimpleCondition();
            this.requests = new ConcurrentHashMap<TreeRequest,Object>();
            requests.put(req, this);
            Callback callback = new Callback();
            AntiEntropyService.instance.sessions.put(getName(), callback);
        }
        
        public RepairSession(Range range, String tablename, String... cfnames)
        {
            super("manual-repair-" + UUID.randomUUID());
            this.tablename = tablename;
            this.cfnames = cfnames;
            this.range = range;
            this.requestsMade = new SimpleCondition();
            this.requests = new ConcurrentHashMap<TreeRequest,Object>();
        }

        /**
         * Waits until all requests for the session have been sent out: to wait for the session to end, call join().
         */
        public void blockUntilRunning() throws InterruptedException
        {
            requestsMade.await();
        }

        @Override
        public void run()
        {
            Set<InetAddress> endpoints = AntiEntropyService.getNeighbors(tablename, range);
            if (endpoints.isEmpty())
            {
                requestsMade.signalAll();
                logger.info("No neighbors to repair with for " + tablename + " on " + range + ": " + getName() + " completed.");
                return;
            }

            // begin a repair session
            Callback callback = new Callback();
            AntiEntropyService.instance.sessions.put(getName(), callback);
            try
            {
                // request that all relevant endpoints generate trees
                for (String cfname : cfnames)
                {
                    // send requests to remote nodes and record them
                    for (InetAddress endpoint : endpoints)
                        requests.put(AntiEntropyService.instance.request(getName(), endpoint, range, tablename, cfname), this);
                    // send but don't record an outstanding request to the local node
                    AntiEntropyService.instance.request(getName(), FBUtilities.getLocalAddress(), range, tablename, cfname);
                }
                logger.info("Waiting for repair requests: " + requests.keySet());
                requestsMade.signalAll();

                // block whatever thread started this session until all requests have been returned:
                // if this thread dies, the session will still complete in the background
                callback.completed.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted while waiting for repair: repair will continue in the background.");
            }
        }

        /**
         * Receives notifications of completed requests, and sets a condition when all requests
         * triggered by this session have completed.
         */
        class Callback
        {
            public final SimpleCondition completed = new SimpleCondition();
            public void completed(TreeRequest request)
            {
                // don't mark any requests completed until all requests have been made
                try
                {
                    blockUntilRunning();
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
                requests.remove(request);
                logger.info("{} completed successfully: {} outstanding.", request, requests.size());
                if (!requests.isEmpty())
                    return;

                // all requests completed
                logger.info("Repair session " + getName() + " completed successfully.");
                AntiEntropyService.instance.sessions.remove(getName());
                completed.signalAll();
            }
        }
    }
}
