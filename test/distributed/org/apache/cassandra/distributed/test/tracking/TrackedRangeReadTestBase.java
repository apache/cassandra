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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

/**
 * The cluster and the oracle harness the tracked range read cases are written in terms of. The cases themselves live
 * in {@link TrackedRangeReadTest}, so that a case is nothing but the data it writes, the query it runs and the
 * assertion it makes, and so that a second suite can be written against the same harness without moving any of it.
 */
public abstract class TrackedRangeReadTestBase extends TestBaseImpl
{
    protected static final int REPLICAS = 3;

    protected static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        /*
         * Five production mechanisms are off or pinned across this fixture: hinted handoff, background reconciliation
         * and the reconciler's regular priority queue here, read_repair = 'NONE' on every table definition the cases
         * pass in, and disableautocompaction in createKeyspace. Every case is built on the replicas disagreeing, and
         * what decides whether a mechanism may be turned off is whether it can heal that disagreement, mask it, or
         * race the read that is supposed to resolve it.
         *
         * Background reconciliation is a second implementation of the repair the read under test performs, on a timer
         * rather than on a read. It converges the replicas within a few seconds of the writes, so it races the pull
         * and the augment the read does and, when it wins, has removed the divergence before the read starts. A case
         * whose divergence was reconciled away first still returns the right answer, and returns it without running
         * any of the code the case exists to cover, so it cannot be left to race: off.
         *
         * Hinted handoff stores a mutation a coordinator failed to deliver and replays it later, which is exactly the
         * rows a case withholds from a replica arriving of their own accord. For a node local executeInternal write it
         * is inert, since such a write is not a failed delivery and stores no hint. For the coordinated write prefix it
         * is not: dropping a MUTATION_REQ is a failed delivery, and the hint for it would replay the withheld rows to
         * the replica the case is keeping them from. Off.
         *
         * The failed write retry is that same healing on a different trigger, and the yaml setting above does not gate
         * it. A dropped MUTATION_REQ expires the coordinator's callback, TrackedWriteResponseHandler.onFailure hands
         * the write to MutationTrackingService.retryFailedWrite, and that schedules the active reconciler at regular
         * priority, which brings the mutation to the replica that never acknowledged it within about two seconds - well
         * before the read under test runs. Pausing the regular priority queue removes that healer and nothing else: the
         * high priority queue, which is what a tracked read's own reconciliation runs at, keeps draining. Like hints,
         * it is reachable only from the coordinated write prefix, because an executeInternal write has no coordinator
         * callback to expire.
         *
         * read_repair = 'NONE' is for the oracle keyspace. Both keyspaces are created from the same table definition,
         * and on the untracked one - replication_factor 3, read at ALL - classic read repair is the live path. NONE
         * does not change the answer: the resolver merges every replica's response either way, and only the write
         * back of the merged rows is suppressed. What it removes is the oracle's ability to change its own replicas
         * as a side effect of being read, which is what keeps the oracle a pure function of its writes however often
         * it is observed. On the tracked keyspace the clause is dead, because the tracked read path constructs no
         * ReadRepair at all.
         *
         * disableautocompaction can neither create nor remove divergence: compaction is node local and its output is
         * the merge of its input, so it cannot invent a row a replica never received or drop one it did. Past
         * gc_grace it could purge a deletion a case relies on, but at the default of ten days no run comes near that.
         * It pins sstable and index layout, and nothing more.
         */
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                               .set("hinted_handoff_enabled", false)
                                               .set("mutation_tracking.background_reconciliation_enabled", false))
                         .start();
        // the third mechanism above
        cluster.forEach(() -> MutationTrackingService.instance().pauseActiveReconcilerRegularPriority());
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    /** The tracked keyspace of a case that writes its own schema down rather than going through {@link #createKeyspace}. */
    protected static void createTrackedKeyspace(String keyspace)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));
    }

    /*
     * Everything below is one harness. A case that uses it runs its query twice, against two keyspaces that differ
     * only in replication_type, over identical data written the identical way, and asserts the tracked answer
     * equals the untracked one. No expected result is written down anywhere, so a mismatch is attributable to
     * mutation tracking and nothing else, and no case can be scored wrong because the expectation was guessed.
     *
     * Each case also has to earn its place, which is what the probe argument is for. It runs on the tracked
     * keyspace after the writes and before the read under test, and every assertion in it is a node local
     * executeInternal that never enters StorageProxy and so cannot reconcile away the state it is measuring.
     * A case built on divergent replicas proves there that the divergence is still present and that the data
     * replica could not have answered on its own; a case built on convergent ones proves that every replica
     * already holds everything the answer needs, which is what separates a wrong answer from absent data.
     */

    /** Passed as a page size to read the whole range in one request. */
    protected static final int UNPAGED = 0;

    /**
     * Writes the same data to a tracked keyspace and to an otherwise identical untracked one, reads the untracked
     * one for the expected answer, runs {@code probe} against the tracked one, and asserts the tracked read
     * returns what the untracked read did.
     *
     * @param pageSize the page size to read at, or {@link #UNPAGED}
     * @param probe    given the tracked keyspace and the oracle's answer, run after the writes and before the read
     * @return the tracked keyspace, for any assertion a case wants to make about what the read left behind
     */
    protected static String assertTrackedMatchesOracle(String name, String table, String[] writes, String select,
                                                       int pageSize, BiConsumer<String, Object[][]> probe)
    {
        String untracked = createKeyspace(name + "_oracle", table, false);
        write(untracked, writes);
        Object[][] expected = read(untracked, select, pageSize);

        String tracked = createKeyspace(name, table, true);
        write(tracked, writes);
        probe.accept(tracked, expected);

        assertRows(read(tracked, select, pageSize), expected);
        return tracked;
    }

    private static String createKeyspace(String keyspace, String table, boolean tracked)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
                                          + (tracked ? " AND replication_type='tracked'" : ""), keyspace));
        // a table definition may carry index DDL after the CREATE TABLE, one statement per semicolon
        for (String statement : table.split(";"))
            cluster.schemaChange(withKeyspace(statement, keyspace));
        // an index is not queryable until every replica has finished building it, and a read that reaches one that
        // has not fails with INDEX_BUILD_IN_PROGRESS rather than waiting for it; for a keyspace with no index at all
        // this finds nothing to wait for and returns
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());
        return keyspace;
    }

    /**
     * A statement prefixed with a node number is applied with {@code executeInternal}, which lands it on that node
     * alone and leaves the replicas divergent. One prefixed with {@code *} goes through the coordinator at ALL and
     * leaves them identical. One prefixed with {@code !} and a node number goes through a coordinator that is not that
     * node, at QUORUM, with the mutation to that node dropped in flight, so the replicas are left divergent by a route
     * a client can actually take: see {@link #writeMissingOn}.
     * <p>
     * The two divergent prefixes leave the same rows missing from the same replica. What the coordinated form adds is
     * the path it takes to get there - a real client write, so the mutation is allocated an id, is sent by
     * TrackedWriteRequest, and arrives at its fellow replicas as a MUTATION_REQ, none of which an executeInternal
     * write goes near.
     */
    private static void write(String keyspace, String[] writes)
    {
        for (String write : writes)
        {
            int colon = write.indexOf(':');
            String target = write.substring(0, colon);
            String cql = withKeyspace(write.substring(colon + 1), keyspace);
            if (target.equals("*"))
                cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL);
            else if (target.charAt(0) == '!')
                writeMissingOn(Integer.parseInt(target.substring(1)), cql);
            else
                cluster.get(Integer.parseInt(target)).executeInternal(cql);
        }
    }

    /**
     * The divergence a client can cause: a write that succeeded while one replica missed it. The coordinator is a
     * replica other than {@code missing}, so it is the leader for the mutation - it allocates the id out of its own
     * log, exactly as the node named by an {@code executeInternal} prefix does - and {@code missing} is left short one
     * tracked mutation that only reconciliation can bring it.
     * <p>
     * {@link Verb#MUTATION_REQ} is the verb the mutation travels on. Mutation tracking adds verbs for the hop a
     * coordinator that is not a replica takes to reach a leader ({@code MT_FORWARD_WRITE_REQ}) and for reconciliation's
     * own delivery ({@code MT_PUSH_MUTATION_REQ}), but the leader sends the mutation itself to its fellow replicas as
     * an ordinary MUTATION_REQ carrying a mutation id: {@code TrackedWriteRequest.sendToReplicas}. Dropping it inbound
     * rather than outbound means the drop holds however the write is routed.
     * <p>
     * QUORUM is what the write is made at, because it is the consistency level this state legitimately arises at: two
     * of three replicas acknowledge, the client is told the write succeeded, and the third never applied it. ALL would
     * have to time out and have the timeout swallowed, which is both a different production state - a write the client
     * was told failed - and a swallowed failure the case could then hide behind.
     */
    private static void writeMissingOn(int missing, String cql)
    {
        AssertingLatch mutationDropped = new AssertingLatch("a MUTATION_REQ to node " + missing + " to be dropped");
        IMessageFilters.Filter dropped = cluster.filters().inbound().verbs(Verb.MUTATION_REQ.id).to(missing)
                                               .messagesMatching((from, to, message) -> {
                                                   mutationDropped.countDown();
                                                   return true;
                                               })
                                               .drop();
        try
        {
            cluster.coordinator(missing == 1 ? 2 : 1).execute(cql, ConsistencyLevel.QUORUM);
            // The write returns once a quorum acknowledges, and the two replicas that did apply it are a quorum on
            // their own, so the mutation to `missing` can still be in flight then. Turning the filter off at that
            // point lets it land, leaving the case with no divergence to test, so wait for the drop instead.
            mutationDropped.await();
        }
        finally
        {
            // off() rather than reset(), so a case that installs a filter of its own keeps it
            dropped.off();
        }
    }

    private static Object[][] read(String keyspace, String select, int pageSize)
    {
        String cql = withKeyspace(select, keyspace);
        if (pageSize == UNPAGED)
            return cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL);

        List<Object[]> rows = new ArrayList<>();
        Iterator<Object[]> paged = cluster.coordinator(1).executeWithPaging(cql, ConsistencyLevel.ALL, pageSize);
        while (paged.hasNext())
            rows.add(paged.next());
        return rows.toArray(new Object[0][]);
    }

    /** What one node answers on its own, off its own memtables and sstables, reconciling nothing. */
    protected static Object[][] nodeLocal(String keyspace, int node, String select)
    {
        return cluster.get(node).executeInternal(withKeyspace(select, keyspace));
    }

    /**
     * The nodes that are full replicas of the partition {@code (pk0, pk1)}, decided by the same question
     * {@code Keyspace.applyInternalTracked} asks before it writes: is the token in one of this node's full local
     * ranges. Every table the harness uses has that partition key, and reading the answer out of the replication
     * strategy rather than off a hardcoded ring means the model cannot drift from the placement the writes actually
     * got. With no transient replicas every node's full local ranges cover the ring, so this is always all three
     * nodes.
     */
    protected static Set<Integer> fullReplicasFor(String keyspace, int pk0, String pk1)
    {
        Set<Integer> full = new TreeSet<>();
        for (int node = 1; node <= REPLICAS; node++)
        {
            boolean isFull = cluster.get(node).callOnInstance(() -> {
                AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
                Token token = DatabaseDescriptor.getPartitioner()
                                                .getToken(CompositeType.build(ByteBufferAccessor.instance,
                                                                              Int32Type.instance.decompose(pk0),
                                                                              UTF8Type.instance.decompose(pk1)));
                for (Range<Token> range : strategy.getLocalRanges(ClusterMetadata.current()).onlyFull().ranges())
                    if (range.contains(token))
                        return true;
                return false;
            });
            if (isFull)
                full.add(node);
        }
        return full;
    }

    /**
     * The rows of {@code rows} that node {@code node} can possibly hold in its own memtables and sstables: the ones
     * whose partition it is a full replica of. With no transient replicas that is every row.
     * <p>
     * The first two columns of every row are {@code pk0} and {@code pk1}, which every select the harness hands to a
     * placement aware check begins with.
     */
    protected static Object[][] materializedOn(String keyspace, int node, Object[][] rows)
    {
        List<Object[]> held = new ArrayList<>();
        for (Object[] row : rows)
            if (fullReplicasFor(keyspace, (Integer) row[0], (String) row[1]).contains(node))
                held.add(row);
        return held.toArray(new Object[0][]);
    }

    /**
     * The unstressed case check, for the cases built on divergent replicas. If what the data replica holds on its own
     * already answers the query then the read never has to reconcile anything, and the case would pass with the read
     * path completely broken.
     * <p>
     * Node 1 is the data replica of every one of these reads, and deterministically so: {@link #read} coordinates on
     * node 1, TrackedRead.start prefers the local replica whenever it is a full one, and at RF=3 on three nodes every
     * node is a full replica of every range. So the replica whose materialized data the answer is built from is the
     * one these fixtures leave stale, and it is the same one every run.
     */
    protected static void assertDataReplicaCannotAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        Assert.assertFalse("Not stressed: the data replica answers this query correctly on its own",
                           Arrays.deepEquals(nodeLocal(keyspace, 1, select), oracle));
    }

    /**
     * The converse, for the cases that write through the coordinator and so have no divergence in them: every
     * replica already holds everything the answer needs, which is what makes a short coordinator answer a defect
     * in the read path rather than data that was never there.
     */
    protected static void assertEveryReplicaCanAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        for (int node = 1; node <= REPLICAS; node++)
            assertRows(nodeLocal(keyspace, node, select), oracle);
    }

    /**
     * An indexed range read that scans part of its range, because a page's limit is reached before the end of it,
     * and is then handed a key past everything it scanned by reconciliation.
     * <p>
     * The read remembers the last key it scanned so that the matches it did not get to can be recognised as a short
     * read, and so that the follow up read knows where to resume. Reconciliation used to move that key up to the one
     * it delivered, which claimed the whole span up to it even though only that one partition was read. The matches
     * left over from the index scan then fell inside the claimed span, so instead of being treated as a short read
     * they were emitted with nothing behind them:
     * <pre>
     * java.lang.IllegalStateException: Received match for key without initial or followup read: 000400000003...
     *     at org.apache.cassandra.service.reads.tracked.PartialTrackedIndexRead$FilteringCompletedIndexRead$UnfilteredResultIterator.computeNext(PartialTrackedIndexRead.java:807)
     *     at org.apache.cassandra.db.partitions.PartitionIterators$Serializer.serialize(PartitionIterators.java:247)
     *     at org.apache.cassandra.service.reads.tracked.PartialTrackedIndexRead$FilteringCompletedIndexRead.response(PartialTrackedIndexRead.java:838)
     * </pre>
     * The replica throws instead of responding, so the read never completes and the client sees a timeout.
     * <p>
     * With the default Murmur3 partitioner a range scan visits (3,'c'), then (2,'b'), then (1,'a'). A page size of
     * one stops the scan after (3,'c') and leaves (2,'b') among the matches it did not get to, (1,'a') is the key
     * reconciliation delivers, and (3,'c') is written with a value the row filter rejects so that the page's limit
     * is still unspent when the read moves past it and reaches (2,'b').
     */
    protected static void indexedRangeReadHandedAKeyPastTheScannedRange(String name, String table)
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (3, 'c', 1, 100, 0) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (2, 'b', 1, 100, 1) USING TIMESTAMP 11",
            // the data replica misses it, so reconciliation has to deliver it, and last in token order, so it is
            // past the scan
            "!1:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'a', 1, 100, 1) USING TIMESTAMP 12"
        };
        String select = "SELECT pk0, pk1, ck, v, w FROM %s.tbl WHERE v = 100 AND w = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle(name, table, writes, select, 1,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
