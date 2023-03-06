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

package org.apache.cassandra.distributed.test.log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProcessor implements ClusterMetadataService.Processor
{
    private static final Logger logger = LoggerFactory.getLogger(TestProcessor.class);
    private final AtomicBoolean isPaused = new AtomicBoolean();
    private final WaitQueue waiters;
    private final List<Predicate<Transformation>> waitPredicates;
    private final List<BiFunction<Transformation, Commit.Result, Boolean>> commitPredicates;
    private final ClusterMetadataService.Processor delegate;

    public TestProcessor(ClusterMetadataService.Processor delegate)
    {
        this.waiters = WaitQueue.newWaitQueue();
        this.waitPredicates = new ArrayList<>();
        this.commitPredicates = new ArrayList<>();
        this.delegate = delegate;
    }

    @Override
    public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
    {
        maybePause(transform);
        waitIfPaused();
        Commit.Result commited = delegate.commit(entryId, transform, lastKnown);
        testCommitPredicates(transform, commited);
        return commited;
    }

    @Override
    public ClusterMetadata replayAndWait()
    {
        return delegate.replayAndWait();
    }

    protected void waitIfPaused()
    {
        if (isPaused())
        {
            logger.debug("Test processor is paused, waiting...");
            WaitQueue.Signal signal = waiters.register();
            if (!isPaused())
                signal.cancel();
            else
                signal.awaitUninterruptibly();
            logger.debug("Resumed test processor...");
        }
    }

    public boolean isPaused()
    {
        return isPaused.get();
    }

    public void pauseIf(Predicate<Transformation> predicate, Runnable onMatch)
    {
        waitPredicates.add((e) -> {
            if (predicate.test(e))
            {
                onMatch.run();
                return true;
            }
            return false;
        });
    }

    public void registerCommitPredicate(BiFunction<Transformation, Commit.Result, Boolean> fn)
    {
        commitPredicates.add(fn);
    }

    private void maybePause(Transformation transform)
    {
        Iterator<Predicate<Transformation>> iter = waitPredicates.iterator();

        while (iter.hasNext())
        {
            if (iter.next().test(transform))
            {
                pause();
                iter.remove();
            }
        }
    }

    private void testCommitPredicates(Transformation transform, Commit.Result result)
    {
        commitPredicates.removeIf(predicate -> predicate.apply(transform, result));
    }

    public void pause()
    {
        isPaused.set(true);
    }

    public void unpause()
    {
        isPaused.set(false);
        waiters.signalAll();
    }
}
