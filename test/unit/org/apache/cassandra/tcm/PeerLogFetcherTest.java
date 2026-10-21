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

package org.apache.cassandra.tcm;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundSink;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.utils.concurrent.Future;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PeerLogFetcherTest
{
    private static final InetAddressAndPort REMOTE = InetAddressAndPort.getByNameUnchecked("127.0.0.2");

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * Regression test for CASSANDRA-21384 (deadlock between GlobalLogFollower and GossipStage).
     *
     * When a node discovers a peer at a higher epoch (e.g. while processing gossip), it calls
     * {@link PeerLogFetcher#asyncFetchLog}. Through {@link EpochAwareDebounce} that runs
     * synchronously on the calling thread (GossipStage). The fetch attaches a continuation via
     * {@code Future.map} that does {@code log.append(logState)} followed by the blocking
     * {@code log.waitForHighestConsecutive()}.
     *
     * The bug: the original code attached that continuation *after* dispatching the internode
     * request. If the response completed the request promise before the {@code map()} call (a
     * small but real race, easily won over loopback), {@code map()} on an already-completed
     * promise runs the mapper inline on the caller thread. That means the blocking
     * {@code waitForHighestConsecutive()} ran on GossipStage, which then could not service the
     * {@code Gossiper.runInGossipStageBlocking()} call made by the GlobalLogFollower's post-commit
     * listeners -> circular wait.
     *
     * This test forces exactly that "promise already completed before the continuation is
     * attached" ordering: the request promise is completed from a dedicated responder thread and
     * the caller is blocked until that has happened. It then asserts that the append/wait
     * continuation ran on the responder (messaging) thread and never on the caller thread.
     * Against the pre-fix code the continuation runs on the caller thread and the assertion fails.
     */
    @Test
    public void continuationNeverRunsOnCallerThread() throws Exception
    {
        LocalLog log = LocalLog.logSpec()
                               .async()
                               .withInitialState(new ClusterMetadata(Murmur3Partitioner.instance))
                               .createLog();
        ClusterMetadataService cms = new ClusterMetadataService(new UniformRangePlacement(),
                                                                MetadataSnapshots.NO_OP,
                                                                log,
                                                                new AtomicLongBackedProcessor(log),
                                                                Commit.Replicator.NO_OP,
                                                                false);
        ClusterMetadataService.setInstance(cms);
        log.readyUnchecked();

        // Records the thread that executes the append/wait continuation. The filter fires inside
        // log.append(...) - the first thing the continuation does - and drops the entry so no real
        // processing is required.
        AtomicReference<Thread> continuationThread = new AtomicReference<>();
        Predicate<Entry> captureThenDrop = entry -> {
            continuationThread.compareAndSet(null, Thread.currentThread());
            return true;
        };

        ExecutorService responder = Executors.newSingleThreadExecutor(r -> new Thread(r, "test-fetch-responder"));
        AtomicReference<Thread> responderThread = new AtomicReference<>();
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicBoolean intercepted = new AtomicBoolean(false);

        // Intercept the outbound TCM_FETCH_PEER_LOG_REQ. Complete the request promise from the
        // responder thread (mimicking the messaging response callback) and block the caller here
        // until that has happened, so the promise is already done when the caller returns from the
        // send. The message is always dropped (return false) so nothing is put on the wire.
        OutboundSink.Filter interceptor = (message, to, type) -> {
            if (message.verb() != Verb.TCM_FETCH_PEER_LOG_REQ)
                return true;

            if (intercepted.compareAndSet(false, true))
            {
                long id = message.id();
                Message<?> request = message;
                responder.execute(() -> {
                    responderThread.set(Thread.currentThread());
                    LogState logState = LogState.make(new ClusterMetadata(Murmur3Partitioner.instance));
                    Message<LogState> response = request.responseWith(logState);
                    MessagingService.instance().callbacks.removeAndRespond(id, to, response);
                    delivered.countDown();
                });
                assertTrue("responder did not deliver in time",
                           Uninterruptibles.awaitUninterruptibly(delivered, 30, TimeUnit.SECONDS));
            }
            return false;
        };
        MessagingService.instance().outboundSink.add(interceptor);

        try
        {
            log.addFilter(captureThenDrop);
            Thread callerThread = Thread.currentThread();

            PeerLogFetcher fetcher = new PeerLogFetcher(log);
            Future<ClusterMetadata> result = fetcher.asyncFetchLog(REMOTE, Epoch.FIRST);
            // The continuation has run by the time asyncFetchLog returns (inline on the caller for
            // the buggy code, on the responder thread otherwise). Wait for the future to settle -
            // it completes exceptionally here since the fetched epoch never reaches FIRST, which is
            // irrelevant: we only care which thread ran the continuation.
            result.awaitUninterruptibly(30, TimeUnit.SECONDS);

            Thread ran = continuationThread.get();
            assertNotNull("The append/wait continuation never ran", ran);
            assertNotSame("CASSANDRA-21384: the peer-log append/wait continuation must not run on the " +
                          "caller (e.g. GossipStage) thread, otherwise it can deadlock with GlobalLogFollower",
                          callerThread, ran);
            assertSame("The continuation should run on the messaging response thread",
                       responderThread.get(), ran);
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(interceptor);
            responder.shutdownNow();
            log.close();
            ClusterMetadataService.unsetInstance();
        }
    }

    /**
     * Regression test for CASSANDRA-21384 that reproduces the *actual* circular wait from the
     * ticket end-to-end (not just the thread on which the continuation runs), and verifies it with
     * a thread dump.
     *
     * It runs the peer-log fetch on the real, single-threaded {@link Stage#GOSSIP} and registers a
     * post-commit {@link ChangeListener} that - like the production listeners - calls
     * {@link Gossiper#runInGossipStageBlocking} while an epoch is enacted. On the pre-fix code the
     * fetch's append/wait continuation runs inline on the GossipStage thread and blocks it, so:
     *
     * <pre>
     *   GossipStage       : blocked in PeerLogFetcher -> LocalLog.waitForHighestConsecutive()
     *                       (waiting for the log to advance)
     *   GlobalLogFollower : enacting the epoch -> ChangeListener -> Gossiper.runInGossipStageBlocking()
     *                       (waiting for the single GossipStage thread, which is stuck above)
     * </pre>
     *
     * The test asserts the fetch returns within a timeout (i.e. no deadlock). If it does deadlock
     * it captures a full thread dump and asserts the two stacks are exactly that cycle before
     * failing. On the fixed code the continuation runs on the messaging thread, GossipStage returns
     * from the fetch immediately, and the follower's task is serviced - the test passes.
     */
    @Test
    public void doesNotDeadlockBetweenGossipStageAndLogFollower() throws Exception
    {
        LocalLog log = LocalLog.logSpec()
                               .async()
                               .withInitialState(new ClusterMetadata(Murmur3Partitioner.instance))
                               .createLog();
        ClusterMetadataService cms = new ClusterMetadataService(new UniformRangePlacement(),
                                                                MetadataSnapshots.NO_OP,
                                                                log,
                                                                new AtomicLongBackedProcessor(log),
                                                                Commit.Replicator.NO_OP,
                                                                false);
        ClusterMetadataService.setInstance(cms);
        log.readyUnchecked();

        // Models the production post-commit listeners (e.g. LegacyStateListener) which block on the
        // gossip stage while the follower enacts an epoch. Only armed for the fetch under test.
        AtomicBoolean armed = new AtomicBoolean(false);
        log.addListener(new ChangeListener()
        {
            @Override
            public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
            {
                if (armed.get())
                    Gossiper.runInGossipStageBlocking(() -> {});
            }
        });

        // Releases the caller from the intercepted send once the request promise has been completed
        // (buggy path) or the continuation has begun appending on the responder thread (fixed path).
        // The latter frees the GossipStage thread before the follower needs it, so the fixed code
        // does not deadlock.
        CountDownLatch releaseSend = new CountDownLatch(1);
        log.addFilter(entry -> {
            releaseSend.countDown();
            return false; // keep the entry so the follower enacts it and fires the listener
        });

        ExecutorService responder = Executors.newSingleThreadExecutor(r -> new Thread(r, "test-fetch-responder"));

        // Complete the request promise from the responder thread (mimicking the messaging response
        // callback), holding the caller inside the send until that has happened - so the buggy
        // code's map() sees an already-completed promise and runs the continuation inline on the
        // GossipStage thread. The message is always dropped so nothing hits the network.
        OutboundSink.Filter interceptor = (message, to, type) -> {
            if (message.verb() != Verb.TCM_FETCH_PEER_LOG_REQ)
                return true;

            long id = message.id();
            Message<?> request = message;
            responder.execute(() -> {
                // A snapshot at FIRST that the follower will enact, advancing the epoch and firing
                // the post-commit listener above.
                LogState logState = LogState.make(new ClusterMetadata(Murmur3Partitioner.instance).forceEpoch(Epoch.FIRST));
                Message<LogState> response = request.responseWith(logState);
                MessagingService.instance().callbacks.removeAndRespond(id, to, response);
                releaseSend.countDown();
            });
            Uninterruptibles.awaitUninterruptibly(releaseSend, 30, TimeUnit.SECONDS);
            return false;
        };
        MessagingService.instance().outboundSink.add(interceptor);

        AtomicReference<Future<ClusterMetadata>> fetchFuture = new AtomicReference<>();
        CountDownLatch fetchReturned = new CountDownLatch(1);
        try
        {
            PeerLogFetcher fetcher = new PeerLogFetcher(log);
            armed.set(true);
            // Run the fetch on the real, single-threaded GossipStage, exactly as the gossip path does.
            Stage.GOSSIP.execute(() -> {
                try
                {
                    fetchFuture.set(fetcher.asyncFetchLog(REMOTE, Epoch.FIRST));
                }
                finally
                {
                    fetchReturned.countDown();
                }
            });

            if (!fetchReturned.await(20, TimeUnit.SECONDS))
            {
                // GossipStage never returned from the fetch: the deadlock. Prove it is exactly the
                // GlobalLogFollower <-> GossipStage cycle, then fail with the dump attached.
                String dump = fullThreadDump();
                StackTraceElement[] gossip = stackOf("GossipStage");
                StackTraceElement[] follower = stackOf("GlobalLogFollower");

                // break the deadlock so we don't leak the shared GossipStage thread
                interruptThreads("GossipStage");

                assertTrue("Expected GossipStage to be blocked advancing the log inside the peer-log " +
                           "fetch continuation, but was:\n" + dump,
                           stackContains(gossip, "PeerLogFetcher") && stackContains(gossip, "LocalLog"));
                assertTrue("Expected GlobalLogFollower to be blocked handing off to the gossip stage, " +
                           "but was:\n" + dump,
                           stackContains(follower, "runInGossipStageBlocking"));
                fail("CASSANDRA-21384: reproduced deadlock between GlobalLogFollower and GossipStage:\n" + dump);
            }

            // No deadlock: the fetch returned on the gossip stage. Let it settle to be sure the
            // whole chain (responder -> follower -> gossip stage) completed.
            Future<ClusterMetadata> f = fetchFuture.get();
            assertNotNull("peer-log fetch future was not captured", f);
            f.awaitUninterruptibly(20, TimeUnit.SECONDS);
        }
        finally
        {
            armed.set(false);
            MessagingService.instance().outboundSink.remove(interceptor);
            responder.shutdownNow();
            log.close();
            ClusterMetadataService.unsetInstance();
        }
    }

    private static StackTraceElement[] stackOf(String namePrefix)
    {
        for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet())
            if (e.getKey().getName().startsWith(namePrefix))
                return e.getValue();
        return null;
    }

    private static boolean stackContains(StackTraceElement[] stack, String needle)
    {
        if (stack == null)
            return false;
        for (StackTraceElement frame : stack)
            if (frame.toString().contains(needle))
                return true;
        return false;
    }

    private static void interruptThreads(String namePrefix)
    {
        for (Thread t : Thread.getAllStackTraces().keySet())
            if (t.getName().startsWith(namePrefix))
                t.interrupt();
    }

    private static String fullThreadDump()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet())
        {
            Thread t = e.getKey();
            sb.append('"').append(t.getName()).append("\" ").append(t.getState()).append('\n');
            for (StackTraceElement frame : e.getValue())
                sb.append("\tat ").append(frame).append('\n');
            sb.append('\n');
        }
        return sb.toString();
    }
}
