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

package org.apache.cassandra.distributed.impl;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.apache.cassandra.utils.concurrent.WaitQueue.Signal;

public class TestChangeListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(TestChangeListener.class);
    public static final TestChangeListener instance = new TestChangeListener();

    public static void register()
    {
        logger.debug("Registered TestChangeListener");
        ClusterMetadataService.instance().log().addListener(instance);
    }

    NavigableMap<Epoch, CommitBarrier> preCommitBarriers = new ConcurrentSkipListMap<>();
    NavigableMap<Epoch, CommitBarrier> postCommitBarriers = new ConcurrentSkipListMap<>();
    private final WaitQueue waiters = WaitQueue.newWaitQueue();

    private class CommitBarrier
    {
        private final CountDownLatch waiting = CountDownLatch.newCountDownLatch(1);
        private final Runnable onPaused;
        private final String desc;

        private CommitBarrier(Runnable onPaused, String desc)
        {
            this.onPaused = onPaused;
            this.desc = desc;
        }

        private void await()
        {
            logger.debug("Notifying paused: {}", desc);
            Signal s = waiters.register();
            waiting.decrement();
            onPaused.run();
            s.awaitUninterruptibly();
            logger.debug("Unpaused: {}", desc);
        }
    }
    @Override
    public void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        CommitBarrier commitBarrier = preCommitBarriers.remove(next.epoch);
        if (commitBarrier != null)
            commitBarrier.await();
    }

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        CommitBarrier commitBarrier = postCommitBarriers.remove(next.epoch);
        if (commitBarrier != null)
            commitBarrier.await();
    }

    public void pauseBefore(Epoch epoch, Runnable onPaused)
    {
        preCommitBarriers.put(epoch, new CommitBarrier(onPaused, "pre-commit " + epoch));
    }

    public void pauseAfter(Epoch epoch, Runnable onPaused)
    {
        postCommitBarriers.put(epoch, new CommitBarrier(onPaused, "post-commit " + epoch));
    }

    public void unpause()
    {
        logger.info("Unpausing all precommit and post commit barriers");
        waiters.signalAll();
    }

    public void clearAndUnpause()
    {
        preCommitBarriers.clear();
        postCommitBarriers.clear();
        waiters.signalAll();
    }
}
