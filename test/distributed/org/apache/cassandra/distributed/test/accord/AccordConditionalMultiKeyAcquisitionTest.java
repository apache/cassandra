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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FailingConsumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exercises overlapping conditional acquisitions, all-or-nothing rejection, and agreement between
 * resource ownership and a separate allocation record. Controlled races also check transactional
 * snapshots during acquisition; final-state checks alone cannot establish atomic visibility.
 */
public class AccordConditionalMultiKeyAcquisitionTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordConditionalMultiKeyAcquisitionTest.class);
    private static final int BUYERS = 12;
    private static final String KS = "cond_multikey_acquisition";

    @BeforeClass
    public static void setUp() throws IOException
    {
        setupCluster(Function.identity(), 4);
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = " +
                                    "{'class': 'SimpleStrategy', 'replication_factor': 3}");
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @Test
    public void sameGroupHasExactlyOneBuyer() throws Exception
    {
        Map<Integer, List<String>> requests = new TreeMap<>();
        for (int i = 0; i < BUYERS; i++)
            requests.put(i, Arrays.asList("A/1/1", "A/1/2", "A/1/3", "A/1/4"));
        race(requests, 1);
    }

    @Test
    public void overlappingGroupsDoNotLeavePartialPurchases() throws Exception
    {
        Map<Integer, List<String>> requests = new TreeMap<>();
        for (int i = 0; i < BUYERS; i++)
            requests.put(i, Arrays.asList("A/1/1", "B/2/" + i, "C/3/" + i));
        race(requests, 1);
    }

    @Test
    public void disjointGroupsAllSucceed() throws Exception
    {
        Map<Integer, List<String>> requests = new TreeMap<>();
        for (int i = 0; i < BUYERS; i++)
            requests.put(i, Arrays.asList("A/1/" + i, "B/2/" + i, "C/3/" + i));
        race(requests, BUYERS);
    }

    @Test
    public void mixedSizeDisjointGroupsAllSucceed() throws Exception
    {
        Map<Integer, List<String>> requests = mixedSizeRequests(false);
        race(requests, requests.size());
    }

    @Test
    public void mixedSizeOverlappingGroupsHaveOneWinnerPerPair() throws Exception
    {
        Map<Integer, List<String>> requests = mixedSizeRequests(true);
        race(requests, requests.size() / 2);
    }

    private static Map<Integer, List<String>> mixedSizeRequests(boolean overlap)
    {
        int[] sizes = { 2, 3, 4, 5, 8, 10, 16, 30, 3, 2, 5, 4 };
        Map<Integer, List<String>> requests = new TreeMap<>();
        for (int buyer = 0; buyer < sizes.length; ++buyer)
        {
            List<String> selected = new ArrayList<>();
            for (int seat = 0; seat < sizes[buyer]; ++seat)
                selected.add(buyer + "/" + seat);
            // Each pair shares only its final seat; all other proposed writes belong to that buyer.
            if (overlap)
                selected.set(selected.size() - 1, "shared/" + buyer / 2);
            requests.put(buyer, selected);
        }
        return requests;
    }

    @Test
    public void conflictAtEveryPositionLeavesOtherSeatsUntouched() throws Exception
    {
        withTables(cluster -> {
            for (int position = 0; position < 3; ++position)
            {
                List<String> selected = Arrays.asList(position + "/a", position + "/b", position + "/c");
                int owner = position * 2;
                Map<Integer, List<String>> requests = new TreeMap<>();
                requests.put(owner, Arrays.asList(selected.get(position)));
                requests.put(owner + 1, selected);
                Snapshot snapshot = new Snapshot(requests);
                executeWithRetry(cluster, purchase(owner, requests.get(owner)));
                Object[][] before = snapshot.read(cluster);
                assertEquals(1, snapshot.assertValid(before));
                executeWithRetry(cluster, purchase(owner + 1, selected));
                assertArrayEquals("Rejected acquisition changed ownership or allocation records", before, snapshot.read(cluster));
            }
        });
    }

    @Test
    public void existingBuyerCannotAcquireAnotherGroup() throws Exception
    {
        withTables(cluster -> {
            Map<Integer, List<String>> requests = new TreeMap<>();
            requests.put(0, Arrays.asList("a", "b"));
            requests.put(1, Arrays.asList("c", "d"));
            Snapshot snapshot = new Snapshot(requests);
            executeWithRetry(cluster, purchase(0, requests.get(0)));
            Object[][] before = snapshot.read(cluster);
            assertEquals(1, snapshot.assertValid(before));
            executeWithRetry(cluster, purchase(0, requests.get(1)));
            assertArrayEquals("Existing allocation must prevent acquisition of free seats", before, snapshot.read(cluster));
        });
    }

    @Test
    public void replayDoesNotChangeAllocations() throws Exception
    {
        withTables(cluster -> {
            Map<Integer, List<String>> requests = new TreeMap<>();
            requests.put(0, Arrays.asList("a", "b"));
            requests.put(1, Arrays.asList("b", "c"));
            Snapshot snapshot = new Snapshot(requests);
            executeWithRetry(cluster, purchase(0, requests.get(0)));
            executeWithRetry(cluster, purchase(1, requests.get(1)));
            Object[][] before = snapshot.read(cluster);
            assertEquals(1, snapshot.assertValid(before));
            for (Map.Entry<Integer, List<String>> request : requests.entrySet())
            {
                executeWithRetry(cluster, purchase(request.getKey(), request.getValue()));
                assertArrayEquals("Replay changed the allocation", before, snapshot.read(cluster));
            }
        });
    }

    @Test
    public void cyclicOverlapHasOneCompleteAllocation() throws Exception
    {
        Map<Integer, List<String>> requests = new TreeMap<>();
        requests.put(0, Arrays.asList("a", "b"));
        requests.put(1, Arrays.asList("b", "c"));
        requests.put(2, Arrays.asList("c", "a"));
        controlledRace(requests, 0, 1, 2);
    }

    @Test
    public void concurrentDuplicatesHaveOneAllocation() throws Exception
    {
        Map<Integer, List<String>> requests = new TreeMap<>();
        requests.put(0, Arrays.asList("a", "b", "c"));
        controlledRace(requests, 0, 0, 0);
    }

    private String seats()
    {
        return KS + ".seats_" + accordTableName;
    }

    private String purchases()
    {
        return KS + ".purchases_" + accordTableName;
    }

    private void withTables(FailingConsumer<Cluster> action) throws Exception
    {
        test(Arrays.asList("CREATE TABLE " + seats() + " (event int, seat text, buyer int, " +
                           "PRIMARY KEY ((event, seat))) WITH transactional_mode = 'full'",
                           "CREATE TABLE " + purchases() + " (event int, buyer int, seats frozen<set<text>>, " +
                           "PRIMARY KEY ((event, buyer))) WITH transactional_mode = 'full'"), action);
    }

    private void race(Map<Integer, List<String>> requests, int expectedWinners) throws Exception
    {
        withTables(cluster -> {
            ExecutorService executor = Executors.newFixedThreadPool(BUYERS);
            List<Future<?>> futures = new ArrayList<>();
            try
            {
                for (Map.Entry<Integer, List<String>> request : requests.entrySet())
                    futures.add(executor.submit(() -> executeWithRetry(cluster, cluster.get(1 + request.getKey() % 3),
                                                                      purchase(request.getKey(), request.getValue()))));
                for (Future<?> future : futures)
                    future.get(60, SECONDS);
            }
            finally
            {
                executor.shutdownNow();
                assertTrue("Purchase workers did not stop", executor.awaitTermination(30, SECONDS));
            }
            Snapshot snapshot = new Snapshot(requests);
            assertEquals("Unexpected number of allocations", expectedWinners, snapshot.assertValid(snapshot.read(cluster)));
        });
    }

    private void controlledRace(Map<Integer, List<String>> requests, int... buyers) throws Exception
    {
        withTables(cluster -> {
            Snapshot snapshot = new Snapshot(requests);
            assertEquals(0, snapshot.assertValid(snapshot.read(cluster)));
            ExecutorService executor = Executors.newFixedThreadPool(4);
            AtomicBoolean finished = new AtomicBoolean();
            List<Future<?>> writers = new ArrayList<>();
            // Hold PreAccept replies so all operations reach replicas before any can finish coordination.
            // Each writer uses a different coordinator; node 4 runs the transactional observer.
            List<Runnable> held = new ArrayList<>();
            Set<Integer> coordinators = new HashSet<>();
            CountDownLatch entered = new CountDownLatch(4);
            AtomicBoolean released = new AtomicBoolean();
            IMessageFilters.Filter filter = cluster.filters().outbound().verbs(Verb.ACCORD_PRE_ACCEPT_RSP.id)
                                                  .messagesMatching((from, to, message) -> {
                synchronized (held)
                {
                    if (released.get())
                        return false;
                    held.add(() -> cluster.get(to).receiveMessage(message));
                    if (coordinators.add(to))
                        entered.countDown();
                    return true;
                }
            }).drop();
            Runnable release = () -> {
                List<Runnable> pending;
                synchronized (held)
                {
                    released.set(true);
                    pending = new ArrayList<>(held);
                    held.clear();
                }
                filter.off();
                pending.forEach(Runnable::run);
            };
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    final int index = i;
                    writers.add(executor.submit(() -> executeWithRetry(cluster, cluster.get(index + 1),
                                               purchase(buyers[index], requests.get(buyers[index])))));
                }
                Future<?> observer = executor.submit(() -> {
                    do
                    {
                        snapshot.assertValid(snapshot.read(cluster));
                    }
                    while (!finished.get());
                });
                assertTrue("Writers and observer did not all enter PreAccept", entered.await(30, SECONDS));
                for (Future<?> writer : writers)
                    assertTrue("Acquisition completed before the controlled overlap", !writer.isDone());
                release.run();
                for (Future<?> writer : writers)
                    writer.get(60, SECONDS);
                finished.set(true);
                observer.get(60, SECONDS);
                assertEquals(1, snapshot.assertValid(snapshot.read(cluster)));
            }
            finally
            {
                finished.set(true);
                try
                {
                    release.run();
                }
                finally
                {
                    executor.shutdownNow();
                    assertTrue("Acquisition workers did not stop", executor.awaitTermination(30, SECONDS));
                }
            }
        });
    }

    /** Builds and checks a transactional snapshot with a fixed column order. */
    private class Snapshot
    {
        final Map<Integer, List<String>> requests;
        final List<String> allSeats;
        final List<Integer> buyers;
        final String query;

        Snapshot(Map<Integer, List<String>> requests)
        {
            this.requests = requests;
            allSeats = requests.values().stream().flatMap(List::stream).distinct().sorted().collect(Collectors.toList());
            buyers = requests.keySet().stream().sorted().collect(Collectors.toList());
            StringBuilder txn = new StringBuilder("BEGIN TRANSACTION\n");
            List<String> columns = new ArrayList<>();
            for (int i = 0; i < allSeats.size(); ++i)
            {
                txn.append("LET s").append(i).append(" = (SELECT buyer FROM ").append(seats())
                   .append(" WHERE event=1 AND seat='").append(allSeats.get(i)).append("');\n");
                columns.add("s" + i + ".buyer");
            }
            for (int buyer : buyers)
            {
                txn.append("LET p").append(buyer).append(" = (SELECT seats FROM ").append(purchases())
                   .append(" WHERE event=1 AND buyer=").append(buyer).append(");\n");
                columns.add("p" + buyer + ".seats");
            }
            query = txn.append("SELECT ").append(String.join(", ", columns)).append(";\nCOMMIT TRANSACTION").toString();
        }

        Object[][] read(Cluster cluster)
        {
            return executeWithRetry(cluster, cluster.get(4), query).toObjectArrays();
        }

        int assertValid(Object[][] rows)
        {
            assertEquals("Expected one snapshot row", 1, rows.length);
            assertEquals(allSeats.size() + buyers.size(), rows[0].length);
            Map<String, Integer> owners = new HashMap<>();
            int winners = 0;
            for (int i = 0; i < buyers.size(); ++i)
            {
                int buyer = buyers.get(i);
                Object allocation = rows[0][allSeats.size() + i];
                if (allocation == null)
                    continue;
                ++winners;
                List<String> selected = requests.get(buyer);
                assertEquals("Wrong allocation for buyer " + buyer, new HashSet<>(selected), allocation);
                for (String seat : selected)
                    assertEquals("Seat allocated twice: " + seat, null, owners.put(seat, buyer));
            }
            for (int i = 0; i < allSeats.size(); ++i)
                assertEquals("Ownership disagrees with allocations for " + allSeats.get(i), owners.get(allSeats.get(i)), rows[0][i]);
            return winners;
        }
    }

    private String purchase(int buyer, List<String> selected)
    {
        StringBuilder txn = new StringBuilder("BEGIN TRANSACTION\n");
        txn.append("LET previous = (SELECT seats FROM ").append(purchases())
           .append(" WHERE event=1 AND buyer=").append(buyer).append(");\n");
        for (int i = 0; i < selected.size(); i++)
            txn.append("LET s").append(i).append(" = (SELECT buyer FROM ").append(seats())
               .append(" WHERE event=1 AND seat='").append(selected.get(i)).append("');\n");
        txn.append("IF previous IS NULL");
        for (int i = 0; i < selected.size(); i++)
            txn.append(" AND s").append(i).append(" IS NULL");
        txn.append(" THEN\n");
        for (String seat : selected)
            txn.append("INSERT INTO ").append(seats()).append(" (event, seat, buyer) VALUES (1, '")
               .append(seat).append("', ").append(buyer).append(");\n");
        txn.append("INSERT INTO ").append(purchases()).append(" (event, buyer, seats) VALUES (1, ")
           .append(buyer).append(", {")
           .append(selected.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ")))
           .append("});\nEND IF\nCOMMIT TRANSACTION");
        return txn.toString();
    }
}
