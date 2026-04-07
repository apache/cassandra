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

package org.apache.cassandra.db.compression;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Scheduling-skeleton tests for {@link CompressionDictionaryAutoTrainingManager}.
 * <p>
 * They exercise the real {@code scheduleWithFixedDelay} loop through the {@code @VisibleForTesting}
 * seams ({@code start(delay, interval, unit)}, {@code checkAllTables()}, {@code getTables()},
 * {@code isFirstCMSMember()}, {@code checkTable()}) and deliberately do NOT touch real tables, CMS
 * membership, sampling, training or dictionary comparison.
 */
public class CompressionDictionaryAutoTrainingManagerLifecycleTest
{
    private static final String USER_KS = "auto_training_test_ks";

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void schedulerLoopsWhileStartedAndStopsOnClose()
    {
        AtomicInteger invocations = new AtomicInteger();

        CompressionDictionaryAutoTrainingManager manager = new CompressionDictionaryAutoTrainingManager()
        {
            @Override
            void checkAllTables()
            {
                invocations.incrementAndGet();
            }
        };

        int intervalSeconds = 5;
        int countAtClose;
        try
        {
            manager.start(intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

            // The scheduled check must fire repeatedly (>= 3 times) on its fixed schedule, not just once.
            await().atMost(35, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(invocations.get())
                                        .as("checkAllTables() should be invoked repeatedly on the %ds schedule", intervalSeconds)
                                        .isGreaterThanOrEqualTo(3));
        }
        finally
        {
            countAtClose = invocations.get();
            manager.close();
        }

        // After close() the scheduled task is cancelled, so the counter must stop advancing: assert it holds
        // (bar a single already-in-flight tick) for more than two intervals.
        await("no further invocations after close()")
        .during(2L * intervalSeconds, TimeUnit.SECONDS)
        .atMost(2L * intervalSeconds + 5, TimeUnit.SECONDS)
        .until(() -> invocations.get() <= countAtClose + 1);
    }

    /**
     * Drives the real {@link CompressionDictionaryAutoTrainingManager#checkAllTables()} via the scheduler
     * (never called directly, never overridden) and steers it only through the {@code getTables()} and
     * {@code isFirstCMSMember()} seams to walk its branches:
     * <ol>
     *   <li>CMS leader, no tables - nothing is checked;</li>
     *   <li>CMS leader, tables that are all ineligible — one per {@code isEligibleTable()} branch, so every
     *       branch is exercised and none reach {@code checkTable()};</li>
     *   <li>not the CMS leader - the loop keeps firing but bails before touching any table.</li>
     * </ol>
     */
    @Test
    public void checkAllTablesHonoursCmsLeadershipAndTableEligibility()
    {
        ControllableAutoTrainingManager manager = new ControllableAutoTrainingManager();
        int intervalSeconds = 2;
        try
        {
            manager.start(intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

            // ---- case 1: this node IS the CMS leader, but there are no tables to check ----
            manager.cmsLeader = true;
            manager.tables = Collections.emptyList();
            awaitAtLeast(manager.cmsMemberChecks, manager.cmsMemberChecks.get() + 2);
            assertThat(manager.tableChecks.get())
            .as("no tables -> checkTable() must never run")
            .isZero();

            // ---- case 2: CMS leader, but every table is ineligible - one per isEligibleTable() branch ----
            ColumnFamilyStore systemKs = mock(ColumnFamilyStore.class);
            when(systemKs.getKeyspaceName()).thenReturn("system");

            ColumnFamilyStore index = mock(ColumnFamilyStore.class);
            when(index.getKeyspaceName()).thenReturn(USER_KS);
            when(index.isIndex()).thenReturn(true);

            ColumnFamilyStore view = mock(ColumnFamilyStore.class);
            TableMetadata viewMeta = mock(TableMetadata.class);
            when(view.getKeyspaceName()).thenReturn(USER_KS);
            when(view.metadata()).thenReturn(viewMeta);
            when(viewMeta.isView()).thenReturn(true);

            ColumnFamilyStore staticCompact = mock(ColumnFamilyStore.class);
            TableMetadata staticMeta = mock(TableMetadata.class);
            when(staticCompact.getKeyspaceName()).thenReturn(USER_KS);
            when(staticCompact.metadata()).thenReturn(staticMeta);
            when(staticMeta.isStaticCompactTable()).thenReturn(true);

            manager.tables = Arrays.asList(systemKs, index, view, staticCompact);
            awaitAtLeast(manager.cmsMemberChecks, manager.cmsMemberChecks.get() + 2);

            assertThat(manager.tableChecks.get())
            .as("every table is ineligible -> checkTable() must never run")
            .isZero();

            // every isEligibleTable() branch was actually reached/evaluated
            verify(systemKs, atLeastOnce()).getKeyspaceName();
            verify(index, atLeastOnce()).isIndex();
            verify(viewMeta, atLeastOnce()).isView();
            verify(staticMeta, atLeastOnce()).isStaticCompactTable();

            // ---- case 3: NOT the CMS leader -> checkAllTables() bails before iterating tables, every cycle ----
            manager.cmsLeader = false;
            manager.tableChecks.set(0);
            awaitAtLeast(manager.cmsMemberChecks, manager.cmsMemberChecks.get() + 3);
            assertThat(manager.tableChecks.get())
            .as("not CMS leader -> tables are never iterated")
            .isZero();
        }
        finally
        {
            manager.close();
        }
    }

    /**
     * Once {@link CompressionDictionaryAutoTrainingManager#close()} is called the scheduled check is
     * cancelled (and further guarded by the {@code closed} flag), so no auto-training work may happen
     * afterwards: {@code isFirstCMSMember()} must never be consulted again, no matter how much time passes.
     */
    @Test
    public void noChecksHappenAfterClose()
    {
        ControllableAutoTrainingManager manager = new ControllableAutoTrainingManager();
        int intervalSeconds = 2;

        manager.start(intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        // make sure it is genuinely running and consulting CMS membership before we close it
        awaitAtLeast(manager.cmsMemberChecks, 3);

        manager.close();
        int checksAtClose = manager.cmsMemberChecks.get();

        // over several further intervals the count must stay frozen - i.e. the loop really stopped
        await("isFirstCMSMember() must not be invoked after close()")
        .during(3L * intervalSeconds, TimeUnit.SECONDS)
        .atMost(3L * intervalSeconds + 5, TimeUnit.SECONDS)
        .until(() -> manager.cmsMemberChecks.get() == checksAtClose);
    }

    /**
     * Waits (bounded) until {@code counter} reaches {@code target}, i.e. the scheduled check has fired
     * enough times.
     */
    private static void awaitAtLeast(AtomicInteger counter, int target)
    {
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(counter.get())
                                    .as("scheduled check should reach at least %d invocations", target)
                                    .isGreaterThanOrEqualTo(target));
    }

    /**
     * A manager whose CMS-leadership answer and table set are test-controlled, and which records how often
     * the loop consults CMS membership and how often a table reached {@code checkTable()}. {@code checkTable()}
     * is stubbed to a counter so no real per-table work (training, comparison) ever runs.
     */
    private static class ControllableAutoTrainingManager extends CompressionDictionaryAutoTrainingManager
    {
        volatile boolean cmsLeader = true;
        volatile Iterable<ColumnFamilyStore> tables = Collections.emptyList();
        final AtomicInteger cmsMemberChecks = new AtomicInteger();
        final AtomicInteger tableChecks = new AtomicInteger();

        @Override
        boolean isFirstCMSMember()
        {
            cmsMemberChecks.incrementAndGet();
            return cmsLeader;
        }

        @Override
        Iterable<ColumnFamilyStore> getTables()
        {
            return tables;
        }

        @Override
        void checkTable(ColumnFamilyStore cfs)
        {
            tableChecks.incrementAndGet();
        }
    }
}
