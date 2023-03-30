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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public class TestChangeListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(TestChangeListener.class);
    public static final TestChangeListener instance = new TestChangeListener();

    public static void register()
    {
        logger.debug("Registered TestChangeListener");
        ClusterMetadataService.instance().log().addListener(instance);
    }

    private final List<Predicate<Epoch>> preCommitPredicates = new ArrayList<>();
    private final List<Predicate<Epoch>> postCommitPredicates = new ArrayList<>();
    private final WaitQueue waiters = WaitQueue.newWaitQueue();

    @Override
    public void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next)
    {
        Iterator<Predicate<Epoch>> iter = preCommitPredicates.iterator();
        while (iter.hasNext())
        {
            if (iter.next().test(next.epoch))
            {
                logger.debug("Epoch matches pre-commit predicate, pausing");
                pause();
                iter.remove();
            }
        }
    }

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next)
    {
        Iterator<Predicate<Epoch>> iter = postCommitPredicates.iterator();
        while (iter.hasNext())
        {
            if (iter.next().test(next.epoch))
            {
                logger.debug("Epoch matches post-commit predicate, pausing");
                pause();
                iter.remove();
            }
        }
    }

    public void pauseBefore(Epoch epoch, Runnable onMatch)
    {
        logger.debug("Requesting pause before enacting {}", epoch);
        preCommitPredicates.add((e) -> {
            if (e.is(epoch))
            {
                onMatch.run();
                return true;
            }
            return false;
        });
    }

    public void pauseAfter(Epoch epoch, Runnable onMatch)
    {
        logger.debug("Requesting pause after enacting {}", epoch);
        postCommitPredicates.add((e) -> {
            if (e.is(epoch))
            {
                onMatch.run();
                return true;
            }
            return false;
        });
    }

    public void pause()
    {
        WaitQueue.Signal signal = waiters.register();
        logger.debug("Log follower is paused, waiting...");
        signal.awaitUninterruptibly();
        logger.debug("Resumed log follower...");
    }

    public void unpause()
    {
        logger.debug("Unpausing log follower");
        waiters.signalAll();
    }
}
