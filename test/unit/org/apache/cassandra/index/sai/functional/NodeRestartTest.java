/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.functional;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.assertj.core.api.Assertions;

public class NodeRestartTest extends SAITester
{
    // Failure during the pre-join and initialization tasks shouldn't fail node restart.
    @Test
    public void shouldSurviveRestartWithPreJoinAndInitFailures() throws Throwable
    {
        createSingleRowIndex();

        Injection ssTableIndexValidationError = newFailureOnEntry("error_at_sstable_index_validation",
                                                                  StorageAttachedIndex.class,
                                                                  "findNonIndexedSSTables",
                                                                  RuntimeException.class);

        // This barrier allows us to wait until the 2i initialization task, which validates the index, has run:
        Injections.Barrier initTaskLatch =
                Injections.newBarrier("failing_init_task_barrier", 1, false)
                          .add(InvokePointBuilder.newInvokePoint().atExceptionExit().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild"))
                          .build();

        Injections.inject(ssTableIndexValidationError, initTaskLatch, perSSTableValidationCounter, perColumnValidationCounter);

        simulateNodeRestart(false);

        // Wait until the init task runs and fails...
        initTaskLatch.await();

        // The node should accept a simple query:
        assertNumRows(1, "SELECT * FROM %%s");

        // We should have completed no actual SSTable validations:
        assertValidationCount(0, 0);

        Assertions.assertThat(getNotQueryableIndexes()).isNotEmpty();
    }

    // We don't allow the node to actually join the ring before a valid index is ready to accept queries.
    @Test
    public void shouldQueryAfterRestartButBeforeInitializationTask() throws Throwable
    {
        createSingleRowIndex();

        // This barrier prevents the 2i initialization task, which makes the index queryable, from running:
        Injections.Barrier initTaskLatch =
                Injections.newBarrier("pause_init_task_entry", 2, false)
                          .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild"))
                          .build();

        Injections.Barrier initTaskLatchExit =
                Injections.newBarrier("pause_init_task_exit", 1, false)
                          .add(InvokePointBuilder.newInvokePoint().atExit().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild"))
                          .build();

        // Make sure we re-introduce existing counter injections...
        Injections.inject(initTaskLatch, initTaskLatchExit, perSSTableValidationCounter, perColumnValidationCounter);

        simulateNodeRestart(false);

        waitForAssert(() -> Assert.assertEquals(1, initTaskLatch.getCount()));

        // If we do not make the index queryable before it starts accepting queries, this will fail:
        assertNumRows(1, "SELECT * FROM %%s WHERE v1 >= 0");

        // Allow the init task to run, and then wait for it to finish...
        initTaskLatch.countDown();
        initTaskLatchExit.await();

        // This will fail if the init task doesn't skip validation (after the pre-join task has already run):
        assertValidationCount(1, 1);
    }

    @Test
    public void shouldRestartWithExistingIndexComponents() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        flush();

        IndexIdentifier numericIndexIdentifier = createIndexIdentifier(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));
        IndexIdentifier literalIndexIdentifier = createIndexIdentifier(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")));
        IndexTermType numericIndexTermType = createIndexTermType(Int32Type.instance);
        IndexTermType literalIndexTermType = createIndexTermType(UTF8Type.instance);
        verifyIndexFiles(numericIndexTermType, numericIndexIdentifier, 1);
        verifyIndexFiles(literalIndexTermType, literalIndexIdentifier, 1);
        assertNumRows(1, "SELECT * FROM %%s WHERE v1 >= 0");
        assertNumRows(1, "SELECT * FROM %%s WHERE v2 = '0'");
        assertValidationCount(0, 0);

        simulateNodeRestart();

        verifyIndexFiles(numericIndexTermType, numericIndexIdentifier, 1);
        verifyIndexFiles(literalIndexTermType, literalIndexIdentifier, 1);

        assertNumRows(1, "SELECT * FROM %%s WHERE v1 >= 0");
        assertNumRows(1, "SELECT * FROM %%s WHERE v2 = '0'");

        waitForTableIndexesQueryable();

        // index components are included after restart
        verifyIndexComponentsIncludedInSSTable();
    }

    // We skip validation in the pre-join task if the initialization task has already run and made the index queryable.
    @Test
    public void shouldAvoidPreJoinValidationIfInitTaskHasRun() throws Throwable
    {
        createSingleRowIndex();

        //TODO We should be able to use a latch here to avoid having a pause
        Injection preJoinPause =
                Injections.newPause("pause_pre_join_task", 5000)
                          .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startPreJoinTask"))
                          .build();

        // Delay the pre-join task, thereby allowing the initialization task to run first:
        Injections.Barrier preJoinTaskLatch =
                Injections.newBarrierAwait("init_task_barrier", 1, false)
                          .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("getPreJoinTask"))
                          .build();

        // This barrier allows us to wait until the 2i initialization task, which validates the index, has run:
        Injections.Barrier initTaskLatch =
                Injections.newBarrierCountDown("init_task_barrier", 1, false)
                          .add(InvokePointBuilder.newInvokePoint().atExit().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild"))
                          .build();
        // Make sure we re-introduce existing counter injections...
        Injections.inject(preJoinPause, preJoinTaskLatch, initTaskLatch, perSSTableValidationCounter, perColumnValidationCounter);

        simulateNodeRestart(false);

        // This will fail if the pre-join task doesn't skip validation (after the init task has already run):
        assertValidationCount(0, 0);
        assertNumRows(1, "SELECT * FROM %%s WHERE v1 >= 0");
    }

    void createSingleRowIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        flush();

        IndexIdentifier numericIndexIdentifier = createIndexIdentifier(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));
        IndexTermType numericIndexTermType = createIndexTermType(Int32Type.instance);

        waitForTableIndexesQueryable();
        verifyIndexFiles(numericIndexTermType, numericIndexIdentifier, 1);
        assertNumRows(1, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(0, 0);
    }
}
