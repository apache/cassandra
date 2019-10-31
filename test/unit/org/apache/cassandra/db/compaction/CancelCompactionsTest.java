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

package org.apache.cassandra.db.compaction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.CompactionMetrics;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class CancelCompactionsTest extends CQLTester
{
    @Test
    public void testStandardCompactionTaskCancellation() throws Throwable
    {
        createTable("create table %s (id int primary key, something int)");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (int i = 0; i < 10; i++)
        {
            execute("insert into %s (id, something) values (?,?)", i, i);
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }
        AbstractCompactionTask ct = null;

        for (List<AbstractCompactionStrategy> css : getCurrentColumnFamilyStore().getCompactionStrategyManager().getStrategies())
        {
            for (AbstractCompactionStrategy cs : css)
            {
                ct = cs.getNextBackgroundTask(0);
                if (ct != null)
                    break;
            }
            if (ct != null)
                break;
        }
        assertNotNull(ct);

        CountDownLatch waitForBeginCompaction = new CountDownLatch(1);
        CountDownLatch waitForStart = new CountDownLatch(1);
        Iterable<CFMetaData> metadatas = Collections.singleton(getCurrentColumnFamilyStore().metadata);
        /*
        Here we ask strategies to pause & interrupt compactions right before calling beginCompaction in CompactionTask
        The code running in the separate thread below mimics CFS#runWithCompactionsDisabled but we only allow
        the real beginCompaction to be called after pausing & interrupting.
         */
        Thread t = new Thread(() -> {
            Uninterruptibles.awaitUninterruptibly(waitForBeginCompaction);
            getCurrentColumnFamilyStore().getCompactionStrategyManager().pause();
            CompactionManager.instance.interruptCompactionFor(metadatas, false);
            waitForStart.countDown();
            CompactionManager.instance.waitForCessation(Collections.singleton(getCurrentColumnFamilyStore()));
            getCurrentColumnFamilyStore().getCompactionStrategyManager().resume();
        });
        t.start();

        try
        {
            ct.execute(new CompactionMetrics()
            {
                @Override
                public void beginCompaction(CompactionInfo.Holder ci)
                {
                    waitForBeginCompaction.countDown();
                    Uninterruptibles.awaitUninterruptibly(waitForStart);
                    super.beginCompaction(ci);
                }
            });
            fail("execute should throw CompactionInterruptedException");
        }
        catch (CompactionInterruptedException cie)
        {
            // expected
        }
        finally
        {
            ct.transaction.abort();
            t.join();
        }
    }
}
