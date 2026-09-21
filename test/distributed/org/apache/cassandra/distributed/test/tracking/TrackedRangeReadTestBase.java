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
import java.util.function.BiConsumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationTrackingService;

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

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
