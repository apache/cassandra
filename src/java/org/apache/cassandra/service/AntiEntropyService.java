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
import java.util.*;
import java.util.concurrent.*;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.CompactionIterator.CompactedRow;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.IndexSummary;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.streaming.StreamOut;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;

import org.apache.log4j.Logger;

import com.google.common.collect.Collections2;
import com.google.common.base.Predicates;

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
 * 1. A major compaction is triggered either via nodeprobe, or automatically:
 *   * Nodeprobe sends TreeRequest messages to all neighbors of the target node: when a node
 *     receives a TreeRequest, it will perform a readonly compaction to immediately validate
 *     the column family.
 *   * Automatic compactions will also validate a column family and broadcast TreeResponses, but
 *     since TreeRequest messages are not sent to neighboring nodes, repairs will only occur if two
 *     nodes happen to perform automatic compactions within TREE_STORE_TIMEOUT of one another.
 * 2. The compaction process validates the column family by:
 *   * Calling getValidator(), which can return a NoopValidator if validation should not be performed,
 *   * Calling IValidator.prepare(), which samples the column family to determine key distribution,
 *   * Calling IValidator.add() in order for every row in the column family,
 *   * Calling IValidator.complete() to indicate that all rows have been added.
 *     * If getValidator decided that the column family should be validated, calling complete()
 *       indicates that a valid MerkleTree has been created for the column family.
 *     * The valid tree is broadcast to neighboring nodes via TreeResponse, and stored locally.
 * 3. When a node receives a TreeResponse, it passes the tree to rendezvous(), which checks for trees to
 *    rendezvous with / compare to:
 *   * If the tree is local, it is cached, and compared to any trees that were received from neighbors.
 *   * If the tree is remote, it is immediately compared to a local tree if one is cached. Otherwise,
 *     the remote tree is stored until a local tree can be generated.
 *   * A Differencer object is enqueued for each comparison.
 * 4. Differencers are executed in AE_SERVICE_STAGE, to compare the two trees.
 *   * Based on the fraction of disagreement between the trees, the differencer will
 *     either perform repair via the io.Streaming api, or via RangeCommand read repairs.
 */
public class AntiEntropyService
{
    private static final Logger logger = Logger.getLogger(AntiEntropyService.class);

    // millisecond lifetime to store trees before they become stale
    public final static long TREE_STORE_TIMEOUT = 600000;
    // max millisecond frequency that natural (automatic) repairs should run at
    public final static long NATURAL_REPAIR_FREQUENCY = 3600000;

    // singleton enforcement
    public static final AntiEntropyService instance = new AntiEntropyService();

    /**
     * Map of CFPair to timestamp of the beginning of the last natural repair.
     */
    private final ConcurrentMap<CFPair, Long> naturalRepairs;

    /**
     * Map of column families to remote endpoints that need to rendezvous. The
     * first endpoint to arrive at the rendezvous will store its tree in the
     * appropriate slot of the TreePair object, and the second to arrive will
     * remove the stored tree, and compare it.
     *
     * This map is only accessed from AE_SERVICE_STAGE, so it is not synchronized.
     */
    private final Map<CFPair, ExpiringMap<InetAddress, TreePair>> trees;

    /**
     * Protected constructor. Use AntiEntropyService.instance.
     */
    protected AntiEntropyService()
    {
        naturalRepairs = new ConcurrentHashMap<CFPair, Long>();
        trees = new HashMap<CFPair, ExpiringMap<InetAddress, TreePair>>();
    }

    /**
     * Returns the map of waiting rendezvous endpoints to trees for the given cf.
     * Should only be called within AE_SERVICE_STAGE.
     *
     * @param cf Column family to fetch trees for.
     * @return The store of trees for the given cf.
     */
    private ExpiringMap<InetAddress, TreePair> rendezvousPairs(CFPair cf)
    {
        ExpiringMap<InetAddress, TreePair> ctrees = trees.get(cf);
        if (ctrees == null)
        {
            ctrees = new ExpiringMap<InetAddress, TreePair>(TREE_STORE_TIMEOUT);
            trees.put(cf, ctrees);
        }
        return ctrees;
    }

    /**
     * Return all of the neighbors with whom we share data.
     */
    private static Collection<InetAddress> getNeighbors(String table)
    {
        InetAddress local = FBUtilities.getLocalAddress();
        StorageService ss = StorageService.instance;
        return Collections2.filter(ss.getNaturalEndpoints(table, ss.getLocalToken()),
                                   Predicates.not(Predicates.equalTo(local)));
    }

    /**
     * Register a tree from the given endpoint to be compared to the appropriate trees
     * in AE_SERVICE_STAGE when they become available.
     *
     * @param cf The column family of the tree.
     * @param endpoint The endpoint which owns the given tree.
     * @param tree The tree for the endpoint.
     */
    private void rendezvous(CFPair cf, InetAddress endpoint, MerkleTree tree)
    {
        InetAddress LOCAL = FBUtilities.getLocalAddress();

        // return the rendezvous pairs for this cf
        ExpiringMap<InetAddress, TreePair> ctrees = rendezvousPairs(cf);

        List<Differencer> differencers = new ArrayList<Differencer>();
        if (LOCAL.equals(endpoint))
        {
            // we're registering a local tree: rendezvous with all remote trees
            for (InetAddress neighbor : getNeighbors(cf.left))
            {
                TreePair waiting = ctrees.remove(neighbor);
                if (waiting != null && waiting.right != null)
                {
                    // the neighbor beat us to the rendezvous: queue differencing
                    differencers.add(new Differencer(cf, LOCAL, neighbor,
                                                     tree, waiting.right));
                    continue;
                }

                // else, the local tree is first to the rendezvous: store and wait
                ctrees.put(neighbor, new TreePair(tree, null));
                logger.debug("Stored local tree for " + cf + " to wait for " + neighbor);
            }
        }
        else
        {
            // we're registering a remote tree: rendezvous with the local tree
            TreePair waiting = ctrees.remove(endpoint);
            if (waiting != null && waiting.left != null)
            {
                // the local tree beat us to the rendezvous: queue differencing
                differencers.add(new Differencer(cf, LOCAL, endpoint,
                                                 waiting.left, tree));
            }
            else
            {
                // else, the remote tree is first to the rendezvous: store and wait
                ctrees.put(endpoint, new TreePair(null, tree));
                logger.debug("Stored remote tree for " + cf + " from " + endpoint);
            }
        }

        for (Differencer differencer : differencers)
        {
            logger.info("Queueing comparison " + differencer);
            StageManager.getStage(StageManager.AE_SERVICE_STAGE).execute(differencer);
        }
    }

    /**
     * Called by a Validator to send a valid tree to endpoints storing
     * replicas of local data.
     *
     * @param validator A locally generated validator.
     * @param local The local endpoint.
     * @param neighbors A list of neighbor endpoints to send the tree to.
     */
    void notifyNeighbors(Validator validator, InetAddress local, Collection<InetAddress> neighbors)
    {
        MessagingService ms = MessagingService.instance;

        try
        {
            Message message = TreeResponseVerbHandler.makeVerb(local, validator);
            logger.info("Sending AEService tree for " + validator.cf + " to: " + neighbors);
            for (InetAddress neighbor : neighbors)
                ms.sendOneWay(message, neighbor);
        }
        catch (Exception e)
        {
            logger.error("Could not send valid tree to endpoints: " + neighbors, e);
        }
    }

    /**
     * Should only be used in AE_SERVICE_STAGE or for testing.
     *
     * @param table Table containing cf.
     * @param cf The column family.
     * @param remote The remote endpoint for the rendezvous.
     * @return The tree pair for the given rendezvous if it exists, else  null.
     */
    TreePair getRendezvousPair_TestsOnly(String table, String cf, InetAddress remote)
    {
        return rendezvousPairs(new CFPair(table, cf)).get(remote);
    }

    /**
     * Should only be used for testing.
     */
    void clearNaturalRepairs_TestsOnly()
    {
        naturalRepairs.clear();
    }

    /**
     * @param cf The column family.
     * @return True if enough time has elapsed since the beginning of the last natural repair.
     */
    private boolean shouldRunNaturally(CFPair cf)
    {
        Long curtime = System.currentTimeMillis();
        Long pretime = naturalRepairs.putIfAbsent(cf, curtime);
        if (pretime != null)
        {
            if (pretime < (curtime - NATURAL_REPAIR_FREQUENCY))
                // replace pretime with curtime, unless someone beat us to it
                return naturalRepairs.replace(cf, pretime, curtime);
            // need to wait longer
            logger.debug("Skipping natural repair: last occurred " + (curtime - pretime) + "ms ago.");
            return false;
        }
        return true;
    }

    /**
     * Return a Validator object which can be used to collect hashes for a column family.
     * A Validator must be prepared() before use, and completed() afterward.
     *
     * @param table The table name containing the column family.
     * @param cf The column family name.
     * @param initiator Endpoint that initially triggered this validation, or null if
     * the validation is occuring due to a natural major compaction.
     * @param major True if the validator will see all of the data contained in the column family.
     * @return A Validator.
     */
    public IValidator getValidator(String table, String cf, InetAddress initiator, boolean major)
    {
        if (!major || table.equals(Table.SYSTEM_TABLE))
            return new NoopValidator();
        if (StorageService.instance.getTokenMetadata().sortedTokens().size()  < 1)
            // gossiper isn't started
            return new NoopValidator();
        CFPair cfpair = new CFPair(table, cf);
        if (initiator == null && !shouldRunNaturally(cfpair))
            return new NoopValidator();
        return new Validator(cfpair);
    }

    /**
     * A Strategy to handle building and validating a merkle tree for a column family.
     *
     * Lifecycle:
     * 1. prepare() - Initialize tree with samples.
     * 2. add() - 0 or more times, to add hashes to the tree.
     * 3. complete() - Enqueues any operations that were blocked waiting for a valid tree.
     */
    public static interface IValidator
    {
        public void prepare();
        public void add(CompactedRow row);
        public void complete();
    }

    /**
     * The IValidator to be used in normal operation.
     */
    public static class Validator implements IValidator, Callable<Object>
    {
        public final CFPair cf; // TODO keep a CFS reference as a field instead of its string representation
        public final MerkleTree tree;

        // the minimum token sorts first, but falls into the last range
        private transient List<MerkleTree.RowHash> minrows;
        // null when all rows with the min token have been consumed
        private transient Token mintoken;
        private transient long validated;
        private transient MerkleTree.TreeRange range;
        private transient MerkleTree.TreeRangeIterator ranges;

        public final static MerkleTree.RowHash EMPTY_ROW = new MerkleTree.RowHash(null, new byte[0]);
        
        Validator(CFPair cf)
        {
            this(cf,
                 // TODO: memory usage (maxsize) should either be tunable per
                 // CF, globally, or as shared for all CFs in a cluster
                 new MerkleTree(DatabaseDescriptor.getPartitioner(), MerkleTree.RECOMMENDED_DEPTH, (int)Math.pow(2, 15)));
        }

        Validator(CFPair cf, MerkleTree tree)
        {
            assert cf != null && tree != null;
            this.cf = cf;
            this.tree = tree;
            minrows = new ArrayList<MerkleTree.RowHash>();
            mintoken = null;
            validated = 0;
            range = null;
            ranges = null;
        }
        
        public void prepare()
        {
            List<DecoratedKey> keys = new ArrayList<DecoratedKey>();
            ColumnFamilyStore cfs;
            try
            {
                cfs = Table.open(cf.left).getColumnFamilyStore(cf.right);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            if (cfs != null) // TODO test w/ valid CF definitions, this if{} shouldn't be necessary
            {
                for (IndexSummary.KeyPosition info: cfs.allIndexPositions())
                    keys.add(info.key);
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
            logger.debug("Prepared AEService tree of size " + tree.size() + " for " + cf);
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
        public void add(CompactedRow row)
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

        private MerkleTree.RowHash rowHash(CompactedRow row)
        {
            validated++;
            // MerkleTree uses XOR internally, so we want lots of output bits here
            byte[] rowhash = FBUtilities.hash("SHA-256", row.key.key.getBytes(), row.buffer.getData());
            return new MerkleTree.RowHash(row.key.token, rowhash);
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
            logger.debug("Validated " + validated + " rows into AEService tree for " + cf);
        }
        
        /**
         * Called after the validation lifecycle to trigger additional action
         * with the now valid tree. Runs in AE_SERVICE_STAGE.
         *
         * @return A meaningless object.
         */
        public Object call() throws Exception
        {
            AntiEntropyService aes = AntiEntropyService.instance;
            InetAddress local = FBUtilities.getLocalAddress();

            Collection<InetAddress> neighbors = getNeighbors(cf.left);

            // store the local tree and then broadcast it to our neighbors
            aes.rendezvous(cf, local, tree);
            aes.notifyNeighbors(this, local, neighbors);

            // return any old object
            return AntiEntropyService.class;
        }
    }

    /**
     * The IValidator to be used before a cluster has stabilized, or when repairs
     * are disabled.
     */
    public static class NoopValidator implements IValidator
    {
        /**
         * Does nothing.
         */
        public void prepare()
        {
            // noop
        }

        /**
         * Does nothing.
         */
        public void add(CompactedRow row)
        {
            // noop
        }

        /**
         * Does nothing.
         */
        public void complete()
        {
            // noop
        }
    }

    /**
     * Compares two trees, and launches repairs for disagreeing ranges.
     */
    public static class Differencer implements Runnable
    {
        public final CFPair cf;
        public final InetAddress local;
        public final InetAddress remote;
        public final MerkleTree ltree;
        public final MerkleTree rtree;
        public final List<MerkleTree.TreeRange> differences;

        public Differencer(CFPair cf, InetAddress local, InetAddress remote, MerkleTree ltree, MerkleTree rtree)
        {
            this.cf = cf;
            this.local = local;
            this.remote = remote;
            this.ltree = ltree;
            this.rtree = rtree;
            differences = new ArrayList<MerkleTree.TreeRange>();
        }

        /**
         * Compares our trees, and triggers repairs for any ranges that mismatch.
         */
        public void run()
        {
            StorageService ss = StorageService.instance;

            // restore partitioners (in case we were serialized)
            if (ltree.partitioner() == null)
                ltree.partitioner(ss.getPartitioner());
            if (rtree.partitioner() == null)
                rtree.partitioner(ss.getPartitioner());

            // determine the ranges where responsibility overlaps
            Set<Range> interesting = new HashSet(ss.getRangesForEndPoint(cf.left, local));
            interesting.retainAll(ss.getRangesForEndPoint(cf.left, remote));

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
            try
            {
                if (difference == 0.0)
                {
                    logger.debug("Endpoints " + local + " and " + remote + " are consistent for " + cf);
                    return;
                }

                if (difference < 0.05)
                    performRangeRepair();
                else
                    performStreamingRepair();
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
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
         * Sends our list of differences to the remote endpoint using read
         * repairs via the query API.
         */
        void performRangeRepair() throws IOException
        {
            logger.info("Performing range read repair of " + differences.size() + " ranges for " + cf);
            // FIXME
            logger.debug("Finished range read repair for " + cf);
        }

        /**
         * Sends our list of differences to the remote endpoint using the
         * Streaming API.
         */
        void performStreamingRepair() throws IOException
        {
            logger.info("Performing streaming repair of " + differences.size() + " ranges to " + remote + " for " + cf);
            ColumnFamilyStore cfstore = Table.open(cf.left).getColumnFamilyStore(cf.right);
            try
            {
                List<Range> ranges = new ArrayList<Range>(differences);
                List<SSTableReader> sstables = CompactionManager.instance.submitAnticompaction(cfstore, ranges, remote).get();
                StreamOut.transferSSTables(remote, sstables, cf.left);
            }
            catch(Exception e)
            {
                throw new IOException("Streaming repair failed.", e);
            }
            logger.debug("Finished streaming repair to " + remote + " for " + cf);
        }

        public String toString()
        {
            return "#<Differencer " + cf + " local=" + local + " remote=" + remote + ">";
        }
    }

    /**
     * Handler for requests from remote nodes to generate a valid tree.
     * The payload is a CFPair representing the columnfamily to validate.
     */
    public static class TreeRequestVerbHandler implements IVerbHandler, ICompactSerializer<CFPair>
    {
        public static final TreeRequestVerbHandler SERIALIZER = new TreeRequestVerbHandler();
        static Message makeVerb(String table, String cf)
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                SERIALIZER.serialize(new CFPair(table, cf), dos);
                return new Message(FBUtilities.getLocalAddress(), StageManager.AE_SERVICE_STAGE, StorageService.Verb.TREE_REQUEST, bos.toByteArray());
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void serialize(CFPair treerequest, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(treerequest.left);
            dos.writeUTF(treerequest.right);
        }

        public CFPair deserialize(DataInputStream dis) throws IOException
        {
            return new CFPair(dis.readUTF(), dis.readUTF());
        }

        /**
         * Trigger a readonly compaction which will broadcast the tree upon completion.
         */
        public void doVerb(Message message)
        { 
            byte[] bytes = message.getMessageBody();
            
            ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);
            try
            {
                CFPair request = this.deserialize(new DataInputStream(buffer));

                // trigger readonly-compaction
                logger.debug("Queueing readonly compaction for request from " + message.getFrom() + " for " + request);
                Table table = Table.open(request.left);
                CompactionManager.instance.submitReadonly(table.getColumnFamilyStore(request.right),
                                                          message.getFrom());
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
            TreeRequestVerbHandler.SERIALIZER.serialize(v.cf, dos);
            ObjectOutputStream oos = new ObjectOutputStream(dos);
            oos.writeObject(v.tree);
            oos.flush();
        }

        public Validator deserialize(DataInputStream dis) throws IOException
        {
            final CFPair cf = TreeRequestVerbHandler.SERIALIZER.deserialize(dis);
            ObjectInputStream ois = new ObjectInputStream(dis);
            try
            {
                return new Validator(cf, (MerkleTree)ois.readObject());
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public void doVerb(Message message)
        { 
            byte[] bytes = message.getMessageBody();
            ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);

            try
            {
                // deserialize the remote tree, and register it
                Validator rvalidator = this.deserialize(new DataInputStream(buffer));
                AntiEntropyService.instance.rendezvous(rvalidator.cf, message.getFrom(), rvalidator.tree);
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
     * A tuple of a local and remote tree. One of the trees should be null, but
     * not both.
     */
    static class TreePair extends Pair<MerkleTree,MerkleTree>
    {
        public TreePair(MerkleTree local, MerkleTree remote)
        {
            super(local, remote);
            assert local != null ^ remote != null;
        }
    }
}
