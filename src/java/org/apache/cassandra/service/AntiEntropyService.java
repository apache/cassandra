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

import org.apache.cassandra.concurrent.SingleThreadedStage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.CompactionIterator.CompactedRow;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.Streaming;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Cachetable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

import org.apache.log4j.Logger;

import com.google.common.collect.Collections2;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

/**
 * AntiEntropyService encapsulates "validating" (hashing) individual column families,
 * exchanging MerkleTrees with remote nodes via a TreeRequest/Response conversation,
 * and then triggering repairs for disagreeing ranges.
 *
 * Every Tree conversation has an 'initiator', where valid trees are sent after generation
 * and where the local and remote tree will rendezvous in register(cf, endpoint, tree).
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
 *     nodes happen to perform automatic compactions within TREE_CACHE_LIFETIME of one another.
 * 2. The compaction process validates the column family by:
 *   * Calling getValidator(), which can return a NoopValidator if validation should not be performed,
 *   * Calling IValidator.prepare(), which samples the column family to determine key distribution,
 *   * Calling IValidator.add() in order for every row in the column family,
 *   * Calling IValidator.complete() to indicate that all rows have been added.
 *     * If getValidator decided that the column family should be validated, calling complete()
 *       indicates that a valid MerkleTree has been created for the column family.
 *     * The valid tree is broadcast to neighboring nodes via TreeResponse, and stored locally.
 * 3. When a node receives a TreeResponse, it passes the tree to register(), which checks for trees to
 *    rendezvous with / compare to:
 *   * If the tree is local, it is cached, and compared to any trees that were received from neighbors.
 *   * If the tree is remote, it is immediately compared to a local tree if one is cached. Otherwise,
 *     the remote tree is stored until a local tree can be generated.
 *   * A Differencer object is enqueued for each comparison.
 * 4. Differencers are executed in AE_SERVICE_STAGE, to compare the two trees.
 *   * Based on the fraction of disagreement between the trees, the differencer will
 *     either perform repair via the io.Streaming api, or via RangeCommand read repairs.
 * 5. TODO: Because a local tree is stored for TREE_CACHE_LIFETIME, it is possible to perform
 *    redundant repairs when repairs are triggered manually. Because of the SSTable architecture,
 *    this doesn't cause any problems except excess data transfer, but:
 *   * One possible solution is to maintain the local tree in memory by invalidating ranges when they
 *     change, and only performing partial compactions/validations.
 *   * Another would be to only communicate with one neighbor at a time, meaning that an additional
 *     compaction is required for every neighbor.
 */
public class AntiEntropyService
{
    private static final Logger logger = Logger.getLogger(AntiEntropyService.class);

    public final static String AE_SERVICE_STAGE = "AE-SERVICE-STAGE";
    public final static String TREE_REQUEST_VERB = "TREE-REQUEST-VERB";
    public final static String TREE_RESPONSE_VERB = "TREE-RESPONSE-VERB";

    // millisecond lifetime to store remote trees before they become stale
    public final static long TREE_CACHE_LIFETIME = 600000;

    // singleton enforcement
    private static volatile AntiEntropyService aeService;

    /**
     * Map of endpoints to recently generated trees for their column families.
     * Remote trees are removed from the map once they have been compared to
     * local trees, but local trees are cached for multiple comparisons.
     */
    private final ConcurrentLinkedHashMap<InetAddress, Cachetable<CFTuple, MerkleTree>> trees;

    public static AntiEntropyService instance()
    {
        if ( aeService == null )
        {
            synchronized ( AntiEntropyService.class )
            {
                if ( aeService == null )
                {
                    aeService = new AntiEntropyService();
                }
            }
        }
        return aeService;
    }

    /**
     * Private constructor. Use AntiEntropyService.instance()
     */
    private AntiEntropyService()
    {
        StageManager.registerStage(AE_SERVICE_STAGE, new SingleThreadedStage(AE_SERVICE_STAGE));

        MessagingService.instance().registerVerbHandlers(TREE_REQUEST_VERB, new TreeRequestVerbHandler());
        MessagingService.instance().registerVerbHandlers(TREE_RESPONSE_VERB, new TreeResponseVerbHandler());
        trees = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.LRU,
                                               DatabaseDescriptor.getReplicationFactor()+1);
    }

    /**
     * @param endpoint Endpoint to fetch trees for.
     * @return The store of trees for the given endpoint.
     */
    private Cachetable<CFTuple, MerkleTree> cacheForEndpoint(InetAddress endpoint)
    {
        Cachetable<CFTuple, MerkleTree> etrees = trees.get(endpoint);
        if (etrees == null)
        {
            // double check the creation
            Cachetable<CFTuple, MerkleTree> probable = new Cachetable<CFTuple, MerkleTree>(TREE_CACHE_LIFETIME);
            if ((etrees = trees.putIfAbsent(endpoint, probable)) == null)
            {
                // created new store for this endpoint
                etrees = probable;
            }
        }
        return etrees;
    }

    /**
     * Register a tree from the given endpoint to be compared to neighbor trees
     * in AE_SERVICE_STAGE when they become available.
     *
     * @param cf The column family of the tree.
     * @param endpoint The endpoint which owns the given tree.
     * @param tree The tree for the endpoint.
     */
    void register(CFTuple cf, InetAddress endpoint, MerkleTree tree)
    {
        InetAddress LOCAL = FBUtilities.getLocalAddress();

        // store the tree, possibly replacing an older copy
        Cachetable<CFTuple, MerkleTree> etrees = cacheForEndpoint(endpoint);

        List<Differencer> differencers = new ArrayList<Differencer>();
        if (LOCAL.equals(endpoint))
        {
            // we stored a local tree: queue differencing for all remote trees
            for (Map.Entry<InetAddress, Cachetable<CFTuple, MerkleTree>> entry : trees.entrySet())
            {
                if (LOCAL.equals(entry.getKey()))
                {
                    // don't compare to ourself
                    continue;
                }
                MerkleTree remotetree = entry.getValue().remove(cf);
                if (remotetree == null)
                {
                    // no tree stored for this endpoint at the moment
                    continue;
                }

                differencers.add(new Differencer(cf, LOCAL, entry.getKey(), tree, remotetree));
            }
            etrees.put(cf, tree);
            logger.debug("Cached local tree for " + cf);
        }
        else
        {
            // we stored a remote tree: queue differencing for local tree
            MerkleTree localtree = cacheForEndpoint(LOCAL).get(cf);
            if (localtree != null)
            {
                // compare immediately
                differencers.add(new Differencer(cf, LOCAL, endpoint, localtree, tree));
            }
            else
            {
                // cache for later comparison
                etrees.put(cf, tree);
                logger.debug("Cached remote tree from " + endpoint + " for " + cf);
            }
        }

        for (Differencer differencer : differencers)
        {
            logger.debug("Queueing comparison " + differencer);
            StageManager.getStage(AE_SERVICE_STAGE).execute(differencer);
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
        MessagingService ms = MessagingService.instance();

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
     * @param endpoint The endpoint that generated the tree.
     * @return the cached tree for the given cf and endpoint.
     */
    MerkleTree getCachedTree(String table, String cf, InetAddress endpoint)
    {
        return cacheForEndpoint(endpoint).get(new CFTuple(table, cf));
    }

    /**
     * Return a Validator object which can be used to collect hashes for a column family.
     * A Validator must be prepared() before use, and completed() afterward.
     *
     * @param table The table name containing the column family.
     * @param cf The column family name.
     * @param initiator Endpoint that initially triggered this validation, or null if
     * the validation will not see all of the data contained in the column family.
     * @return A Validator.
     */
    public IValidator getValidator(String table, String cf, InetAddress initiator)
    {
        if (initiator == null || table.equals(Table.SYSTEM_TABLE))
            return new NoopValidator();
        else if (StorageService.instance().getTokenMetadata().sortedTokens().size()  < 1)
            // gossiper isn't started
            return new NoopValidator();
        else
            return new Validator(new CFTuple(table, cf), initiator);
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
        public final CFTuple cf;
        public final InetAddress initiator;
        public final MerkleTree tree;

        // the minimum token sorts first, but falls into the last range
        private transient List<MerkleTree.RowHash> minrows;
        // null when all rows with the min token have been consumed
        private transient Token mintoken;
        private transient long validated;
        private transient MerkleTree.TreeRange range;
        private transient MerkleTree.TreeRangeIterator ranges;

        public final static Predicate<DecoratedKey> DKPRED = Predicates.alwaysTrue();
        public final static MerkleTree.RowHash EMPTY_ROW = new MerkleTree.RowHash(null, new byte[0]);
        
        Validator(CFTuple cf, InetAddress initiator)
        {
            this(cf,
                 initiator,
                 // TODO: memory usage (maxsize) should either be tunable per
                 // CF, globally, or as shared for all CFs in a cluster
                 new MerkleTree(DatabaseDescriptor.getPartitioner(), MerkleTree.RECOMMENDED_DEPTH, (int)Math.pow(2, 15)));
        }

        Validator(CFTuple cf, InetAddress initiator, MerkleTree tree)
        {
            assert cf != null && initiator != null && tree != null;
            this.cf = cf;
            this.initiator = initiator;
            this.tree = tree;
            minrows = new ArrayList<MerkleTree.RowHash>();
            mintoken = null;
            validated = 0;
            range = null;
            ranges = null;
        }
        
        public void prepare()
        {
            Predicate<SSTable> cfpred = new Predicate<SSTable>()
            {
                public boolean apply(SSTable ss)
                {
                    return cf.table.equals(ss.getTableName()) && cf.cf.equals(ss.getColumnFamilyName());
                }
            };
            List<DecoratedKey> keys = SSTableReader.getIndexedDecoratedKeysFor(cfpred, DKPRED);

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
         * Depending on the initiator for the validation, either registers
         * trees to be compared locally in AE_SERVICE_STAGE, or remotely.
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

            StageManager.getStage(AE_SERVICE_STAGE).execute(this);
            logger.debug("Validated " + validated + " rows into AEService tree for " + cf);
        }
        
        /**
         * Called after the valdation lifecycle to trigger additional action
         * with the now valid tree. Runs in AE_SERVICE_STAGE: depending on
         * which node initiated validation, performs different actions.
         *
         * @return A meaningless object.
         */
        public Object call() throws Exception
        {
            AntiEntropyService aes = AntiEntropyService.instance();
            InetAddress local = FBUtilities.getLocalAddress();
            StorageService ss = StorageService.instance();

            Collection<InetAddress> neighbors = Collections2.filter(ss.getNaturalEndpoints(ss.getLocalToken()),
                                                                    Predicates.not(Predicates.equalTo(local)));

            // cache the local tree and then broadcast it to our neighbors
            aes.register(cf, local, tree);
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
        public final CFTuple cf;
        public final InetAddress local;
        public final InetAddress remote;
        public final MerkleTree ltree;
        public final MerkleTree rtree;
        public final List<MerkleTree.TreeRange> differences;

        public Differencer(CFTuple cf, InetAddress local, InetAddress remote, MerkleTree ltree, MerkleTree rtree)
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
            StorageService ss = StorageService.instance();

            // restore partitioners (in case we were serialized)
            if (ltree.partitioner() == null)
                ltree.partitioner(ss.getPartitioner());
            if (rtree.partitioner() == null)
                rtree.partitioner(ss.getPartitioner());

            // determine the ranges where responsibility overlaps
            Set<Range> interesting = new HashSet(ss.getRangesForEndPoint(local));
            interesting.retainAll(ss.getRangesForEndPoint(remote));

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
            ColumnFamilyStore cfstore = Table.open(cf.table).getColumnFamilyStore(cf.cf);
            try
            {
                List<Range> ranges = new ArrayList<Range>(differences);
                List<SSTableReader> sstables = CompactionManager.instance.submitAnti(cfstore, ranges, remote).get();
                Streaming.transferSSTables(remote, sstables, cf.table);
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
     *
     * The payload is an EndpointCF triple representing the columnfamily to validate
     * and the initiating endpoint.
     */
    public static class TreeRequestVerbHandler implements IVerbHandler, ICompactSerializer<CFTuple>
    {
        public static final TreeRequestVerbHandler SERIALIZER = new TreeRequestVerbHandler();
        static Message makeVerb(String table, String cf)
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                SERIALIZER.serialize(new CFTuple(table, cf), dos);
                return new Message(FBUtilities.getLocalAddress(), AE_SERVICE_STAGE, TREE_REQUEST_VERB, bos.toByteArray());
            }
            catch(IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void serialize(CFTuple treerequest, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(treerequest.table);
            dos.writeUTF(treerequest.cf);
        }

        public CFTuple deserialize(DataInputStream dis) throws IOException
        {
            return new CFTuple(dis.readUTF(), dis.readUTF());
        }

        /**
         * If we have a recently generated cached tree, respond with it immediately:
         * Otherwise, trigger a readonly compaction which will broadcast the tree
         * upon completion.
         */
        public void doVerb(Message message)
        { 
            byte[] bytes = message.getMessageBody();
            DataInputBuffer buffer = new DataInputBuffer();
            buffer.reset(bytes, bytes.length);

            try
            {
                CFTuple request = this.deserialize(buffer);

                // check for cached local tree
                InetAddress local = FBUtilities.getLocalAddress();
                MerkleTree cached = AntiEntropyService.instance().getCachedTree(request.table, request.cf, local);
                if (cached != null)
                {
                    if (local.equals(message.getFrom()))
                    {
                        // we are the requestor, and we already have a cached tree
                        return;
                    }
                    // respond immediately with the recently generated tree
                    Validator valid = new Validator(request, message.getFrom(), cached);
                    Message response = TreeResponseVerbHandler.makeVerb(local, valid);
                    MessagingService.instance().sendOneWay(response, message.getFrom());
                    logger.debug("Answered request from " + message.getFrom() + " for " + request + " with cached tree.");
                    return;
                }

                // trigger readonly-compaction
                logger.debug("Queueing readonly compaction for request from " + message.getFrom() + " for " + request);
                Table table = Table.open(request.table);
                CompactionManager.instance.submitReadonly(table.getColumnFamilyStore(request.cf), message.getFrom());
            }
            catch (IOException e)
            {
                throw new IOError(e);            
            }
        }
    }

    /**
     * Handler for responses from remote nodes that contain a valid tree.
     *
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
                return new Message(local, AE_SERVICE_STAGE, TREE_RESPONSE_VERB, bos.toByteArray());
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
            oos.writeObject(v.initiator);
            oos.writeObject(v.tree);
            oos.flush();
        }

        public Validator deserialize(DataInputStream dis) throws IOException
        {
            final CFTuple cf = TreeRequestVerbHandler.SERIALIZER.deserialize(dis);
            ObjectInputStream ois = new ObjectInputStream(dis);
            try
            {
                Validator v = new Validator(cf, (InetAddress)ois.readObject(), (MerkleTree)ois.readObject());
                return v;
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public void doVerb(Message message)
        { 
            byte[] bytes = message.getMessageBody();
            DataInputBuffer buffer = new DataInputBuffer();
            buffer.reset(bytes, bytes.length);

            try
            {
                // deserialize the remote tree, and register it
                Validator rvalidator = this.deserialize(buffer);
                AntiEntropyService.instance().register(rvalidator.cf, message.getFrom(), rvalidator.tree);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    /**
     * A tuple of table and cf.
     * TODO: Use utils.Pair once it implements hashCode/equals.
     */
    static final class CFTuple
    {
        public final String table;
        public final String cf;
        public CFTuple(String table, String cf)
        {
            assert table != null && cf != null;
            this.table = table;
            this.cf = cf;
        }

        @Override
        public int hashCode()
        {
            int hashCode = 31 + table.hashCode();
            return 31*hashCode + cf.hashCode();
        }
        
        @Override
        public boolean equals(Object o)
        {
            if(!(o instanceof CFTuple))
                return false;
            CFTuple that = (CFTuple)o;
            return table.equals(that.table) && cf.equals(that.cf);
        }
        
        @Override
        public String toString()
        {
            return "[" + table + "][" + cf + "]";
        }
    }
}
