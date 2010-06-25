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
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.AbstractCompactedRow;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.StreamIn;
import org.apache.cassandra.streaming.StreamOut;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamOutManager;
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
 * Tree comparison and repair triggering occur in the single threaded AE_SERVICE_STAGE.
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
 * 4. Differencers are executed in AE_SERVICE_STAGE, to compare the two trees, and perform repair via the
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
     * This map is only accessed from AE_SERVICE_STAGE, so it is not synchronized.
     */
    private final ExpiringMap<String, Map<TreeRequest, TreePair>> requests;

    /**
     * A map of repair session ids to a Queue of TreeRequests that have been performed since the session was started.
     */
    private final ConcurrentMap<String, BlockingQueue<TreeRequest>> sessions;

    /**
     * Protected constructor. Use AntiEntropyService.instance.
     */
    protected AntiEntropyService()
    {
        requests = new ExpiringMap<String, Map<TreeRequest, TreePair>>(REQUEST_TIMEOUT);
        sessions = new ConcurrentHashMap<String, BlockingQueue<TreeRequest>>();
    }

    /**
     * Requests repairs for the given table and column families, and blocks until all repairs have been completed.
     * TODO: Should add retries: if nodes go offline before they respond to the requests, this could block forever.
     */
    public RepairSession getRepairSession(String tablename, String... cfnames)
    {
        return new RepairSession(tablename, cfnames);
    }

    /**
     * Called by Differencer when a full repair round trip has been completed between the given CF and endpoints.
     */
    void completedRequest(TreeRequest request)
    {
        // indicate to the waiting session that this request completed
        BlockingQueue<TreeRequest> session = sessions.get(request.sessionid);
        if (session == null)
            // repair client disconnected: ignore
            return;
        session.offer(request);
    }

    /**
     * Returns the map of waiting rendezvous endpoints to trees for the given session.
     * Should only be called within AE_SERVICE_STAGE.
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
    static Set<InetAddress> getNeighbors(String table)
    {
        StorageService ss = StorageService.instance;
        Set<InetAddress> neighbors = new HashSet<InetAddress>();
        Map<Range, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(table);
        for (Range range : ss.getLocalRanges(table))
        {
            // for every range stored locally (replica or original) collect neighbors storing copies
            neighbors.addAll(replicaSets.get(range));
        }
        neighbors.remove(FBUtilities.getLocalAddress());
        return neighbors;
    }

    /**
     * Register a tree for the given request to be compared to the appropriate trees in AE_SERVICE_STAGE when they become available.
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
            for (InetAddress neighbor : getNeighbors(request.cf.left))
            {
                TreeRequest remotereq = new TreeRequest(request.sessionid, neighbor, request.cf);
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
            StageManager.getStage(StageManager.AE_SERVICE_STAGE).execute(differencer);
        }
    }

    /**
     * Requests a tree from the given node, and returns the request that was sent.
     */
    TreeRequest request(String sessionid, InetAddress remote, String ksname, String cfname)
    {
        TreeRequest request = new TreeRequest(sessionid, remote, new CFPair(ksname, cfname));
        MessagingService.instance.sendOneWay(TreeRequestVerbHandler.makeVerb(request), remote);
        return request;
    }

    /**
     * Responds to the node that requested the given valid tree.
     * @param validator A locally generated validator
     * @param local localhost (parameterized for testing)
     */
    void respond(Validator validator, InetAddress local)
    {
        MessagingService ms = MessagingService.instance;

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
     * @return The tree pair for the given request if it exists.
     */
    TreePair getRendezvousPair_TestsOnly(TreeRequest request)
    {
        System.out.println(request + "\tvs\t" + rendezvousPairs(request.sessionid).keySet());
        return rendezvousPairs(request.sessionid).get(request);
    }

    /**
     * A Strategy to handle building and validating a merkle tree for a column family.
     *
     * Lifecycle:
     * 1. prepare() - Initialize tree with samples.
     * 2. add() - 0 or more times, to add hashes to the tree.
     * 3. complete() - Enqueues any operations that were blocked waiting for a valid tree.
     */
    public static class Validator implements Callable<Object>
    {
        public final TreeRequest request;
        public final MerkleTree tree;

        // the minimum token sorts first, but falls into the last range
        private transient List<MerkleTree.RowHash> minrows;
        // null when all rows with the min token have been consumed
        private transient Token mintoken;
        private transient long validated;
        private transient MerkleTree.TreeRange range;
        private transient MerkleTree.TreeRangeIterator ranges;

        public final static MerkleTree.RowHash EMPTY_ROW = new MerkleTree.RowHash(null, new byte[0]);
        
        Validator(TreeRequest request)
        {
            this(request,
                 // TODO: memory usage (maxsize) should either be tunable per
                 // CF, globally, or as shared for all CFs in a cluster
                 new MerkleTree(DatabaseDescriptor.getPartitioner(), MerkleTree.RECOMMENDED_DEPTH, (int)Math.pow(2, 15)));
        }

        Validator(TreeRequest request, MerkleTree tree)
        {
            this.request = request;
            this.tree = tree;
            minrows = new ArrayList<MerkleTree.RowHash>();
            mintoken = null;
            validated = 0;
            range = null;
            ranges = null;
        }
        
        public void prepare(ColumnFamilyStore cfs)
        {
            List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
            for (DecoratedKey sample : cfs.allKeySamples())
                keys.add(sample);

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
            mintoken = tree.partitioner().getMinimumToken();
            ranges = tree.invalids(new Range(mintoken, mintoken));
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
            if (mintoken != null)
            {
                assert ranges != null : "Validator was not prepared()";

                // check for the minimum token special case
                if (row.key.token.compareTo(mintoken) == 0)
                {
                    // and store it to be appended when we complete
                    minrows.add(rowHash(row));
                    return;
                }
                mintoken = null;
            }

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
            MessageDigest digest = null;
            try
            {
                digest = MessageDigest.getInstance("SHA-256");
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new AssertionError(e);
            }
            row.update(digest);
            return new MerkleTree.RowHash(row.key.token, digest.digest());
        }

        /**
         * Registers the newly created tree for rendezvous in AE_SERVICE_STAGE.
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
            // add rows with the minimum token to the final range
            if (!minrows.isEmpty())
                for (MerkleTree.RowHash minrow : minrows)
                    range.addHash(minrow);

            StageManager.getStage(StageManager.AE_SERVICE_STAGE).submit(this);
            logger.debug("Validated " + validated + " rows into AEService tree for " + request);
        }
        
        /**
         * Called after the validation lifecycle to respond with the now valid tree. Runs in AE_SERVICE_STAGE.
         *
         * @return A meaningless object.
         */
        public Object call() throws Exception
        {
            // respond to the request that triggered this validation
            AntiEntropyService.instance.respond(this, FBUtilities.getLocalAddress());

            // return any old object
            return AntiEntropyService.class;
        }
    }

    /**
     * Runs on the node that initiated a request to compares two trees, and launch repairs for disagreeing ranges.
     */
    public static class Differencer implements Runnable
    {
        public final TreeRequest request;
        public final MerkleTree ltree;
        public final MerkleTree rtree;
        public final List<MerkleTree.TreeRange> differences;

        public Differencer(TreeRequest request, MerkleTree ltree, MerkleTree rtree)
        {
            this.request = request;
            this.ltree = ltree;
            this.rtree = rtree;
            differences = new ArrayList<MerkleTree.TreeRange>();
        }

        /**
         * Compares our trees, and triggers repairs for any ranges that mismatch.
         */
        public void run()
        {
            InetAddress local = FBUtilities.getLocalAddress();
            StorageService ss = StorageService.instance;

            // restore partitioners (in case we were serialized)
            if (ltree.partitioner() == null)
                ltree.partitioner(StorageService.getPartitioner());
            if (rtree.partitioner() == null)
                rtree.partitioner(StorageService.getPartitioner());

            // determine the ranges where responsibility overlaps
            Set<Range> interesting = new HashSet(ss.getRangesForEndpoint(request.cf.left, local));
            interesting.retainAll(ss.getRangesForEndpoint(request.cf.left, request.endpoint));

            // compare trees, and filter out uninteresting differences
            for (MerkleTree.TreeRange diff : MerkleTree.difference(ltree, rtree))
            {
                for (Range localrange: interesting)
                {
                    if (diff.intersects(localrange))
                    {
                        differences.add(diff);
                        break; // the inner loop
                    }
                }
            }
            
            // choose a repair method based on the significance of the difference
            float difference = differenceFraction();
            if (difference == 0.0)
            {
                logger.info("Endpoints " + local + " and " + request.endpoint + " are consistent for " + request.cf);
            }
            else
            {
                try
                {
                    performStreamingRepair();
                }
                catch(IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            // repair was completed successfully: notify any waiting sessions
            AntiEntropyService.instance.completedRequest(request);
        }
        
        /**
         * @return the fraction of the keyspace that is different, as represented by our
         * list of different ranges. A range at depth 0 == 1.0, at depth 1 == 0.5, etc.
         */
        float differenceFraction()
        {
            double fraction = 0.0;
            for (MerkleTree.TreeRange diff : differences)
                fraction += 1.0 / Math.pow(2, diff.depth);
            return (float)fraction;
        }

        /**
         * Sends our list of differences to the remote endpoint using the
         * Streaming API.
         */
        void performStreamingRepair() throws IOException
        {
            logger.info("Performing streaming repair of " + differences.size() + " ranges for " + request);
            ColumnFamilyStore cfstore = Table.open(request.cf.left).getColumnFamilyStore(request.cf.right);
            try
            {
                final List<Range> ranges = new ArrayList<Range>(differences);
                final Collection<SSTableReader> sstables = cfstore.getSSTables();
                // send ranges to the remote node
                Future f = StageManager.getStage(StageManager.STREAM_STAGE).submit(new WrappedRunnable() 
                {
                    protected void runMayThrow() throws Exception
                    {
                        StreamOut.transferSSTables(request.endpoint, request.cf.left, sstables, ranges);
                        StreamOutManager.remove(request.endpoint);
                    }
                });
                // request ranges from the remote node
                // FIXME: no way to block for the 'requestRanges' call to complete, or to request a
                // particular cf: see CASSANDRA-1189
                StreamIn.requestRanges(request.endpoint, request.cf.left, ranges);
                
                // wait until streaming has completed
                f.get();
            }
            catch(Exception e)
            {
                throw new IOException("Streaming repair failed.", e);
            }
            logger.info("Finished streaming repair for " + request);
        }

        public String toString()
        {
            return "#<Differencer " + request + ">";
        }
    }

    /**
     * Handler for requests from remote nodes to generate a valid tree.
     * The payload is a CFPair representing the columnfamily to validate.
     */
    public static class TreeRequestVerbHandler implements IVerbHandler, ICompactSerializer<TreeRequest>
    {
        public static final TreeRequestVerbHandler SERIALIZER = new TreeRequestVerbHandler();
        static Message makeVerb(TreeRequest request)
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                SERIALIZER.serialize(request, dos);
                return new Message(FBUtilities.getLocalAddress(),
                                   StageManager.AE_SERVICE_STAGE,
                                   StorageService.Verb.TREE_REQUEST,
                                   bos.toByteArray());
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void serialize(TreeRequest request, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(request.sessionid);
            CompactEndpointSerializationHelper.serialize(request.endpoint, dos);
            dos.writeUTF(request.cf.left);
            dos.writeUTF(request.cf.right);
        }

        public TreeRequest deserialize(DataInputStream dis) throws IOException
        {
            return new TreeRequest(dis.readUTF(),
                                   CompactEndpointSerializationHelper.deserialize(dis),
                                   new CFPair(dis.readUTF(), dis.readUTF()));
        }

        /**
         * Trigger a validation compaction which will return the tree upon completion.
         */
        public void doVerb(Message message)
        { 
            byte[] bytes = message.getMessageBody();
            
            DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(bytes));
            try
            {
                TreeRequest remotereq = this.deserialize(buffer);
                TreeRequest request = new TreeRequest(remotereq.sessionid, message.getFrom(), remotereq.cf);

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
                SERIALIZER.serialize(validator, dos);
                return new Message(local, StageManager.AE_SERVICE_STAGE, StorageService.Verb.TREE_RESPONSE, bos.toByteArray());
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void serialize(Validator v, DataOutputStream dos) throws IOException
        {
            TreeRequestVerbHandler.SERIALIZER.serialize(v.request, dos);
            ObjectOutputStream oos = new ObjectOutputStream(dos);
            oos.writeObject(v.tree);
            oos.flush();
        }

        public Validator deserialize(DataInputStream dis) throws IOException
        {
            final TreeRequest request = TreeRequestVerbHandler.SERIALIZER.deserialize(dis);
            ObjectInputStream ois = new ObjectInputStream(dis);
            try
            {
                return new Validator(request, (MerkleTree)ois.readObject());
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public void doVerb(Message message)
        { 
            byte[] bytes = message.getMessageBody();
            DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(bytes));

            try
            {
                // deserialize the remote tree, and register it
                Validator response = this.deserialize(buffer);
                TreeRequest request = new TreeRequest(response.request.sessionid, message.getFrom(), response.request.cf);
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
     * A triple of table, cf and address that represents a location we have an outstanding TreeRequest for.
     */
    public static class TreeRequest
    {
        public final String sessionid;
        public final InetAddress endpoint;
        public final CFPair cf;

        public TreeRequest(String sessionid, InetAddress endpoint, CFPair cf)
        {
            this.sessionid = sessionid;
            this.endpoint = endpoint;
            this.cf = cf;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hashCode(sessionid, endpoint, cf);
        }
        
        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof TreeRequest))
                return false;
            TreeRequest that = (TreeRequest)o;
            // handles nulls properly
            return Objects.equal(sessionid, that.sessionid) && Objects.equal(endpoint, that.endpoint) && Objects.equal(cf, that.cf);
        }
        
        @Override
        public String toString()
        {
            return "#<TreeRequest " + sessionid + ", " + endpoint + ", " + cf + ">";
        }
    }

    /**
     * Triggers repairs with all neighbors for the given table and cfs. Typical lifecycle is: start() then join().
     */
    class RepairSession extends Thread
    {
        private final String tablename;
        private final String[] cfnames;
        private final SimpleCondition requestsMade;
        public RepairSession(String tablename, String... cfnames)
        {
            super("manual-repair-" + UUID.randomUUID());
            this.tablename = tablename;
            this.cfnames = cfnames;
            this.requestsMade = new SimpleCondition();
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
            // begin a repair session
            BlockingQueue<TreeRequest> completed = new LinkedBlockingQueue<TreeRequest>();
            AntiEntropyService.this.sessions.put(getName(), completed);
            try
            {
                // request that all relevant endpoints generate trees
                Set<TreeRequest> requests = new HashSet<TreeRequest>();
                Set<InetAddress> endpoints = AntiEntropyService.getNeighbors(tablename);
                for (String cfname : cfnames)
                {
                    // send requests to remote nodes and record them
                    for (InetAddress endpoint : endpoints)
                        requests.add(AntiEntropyService.this.request(getName(), endpoint, tablename, cfname));
                    // send but don't record an outstanding request to the local node
                    AntiEntropyService.this.request(getName(), FBUtilities.getLocalAddress(), tablename, cfname);
                }
                requestsMade.signalAll();

                // block until all requests have been returned by completedRequest calls
                logger.info("Waiting for repair requests to: " + requests);
                while (!requests.isEmpty())
                {
                    TreeRequest request = completed.take();
                    logger.info("Repair request to " + request + " completed successfully.");
                    requests.remove(request);
                }
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted while waiting for repair: repair will continue in the background.");
            }
            finally
            {
                AntiEntropyService.this.sessions.remove(getName());
            }
        }
    }
}
