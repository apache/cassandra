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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.cms.Initialize;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for timeout and exception paths in the TCM commit pipeline that were
 * identified as uncovered by code coverage analysis:
 *
 * 1. handleCommitResult TimeoutException — commit succeeds but follower can't enact
 * 2. handleCommitResult failure callback — commit returns failure
 */
public class CommitTimeoutPathsTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setDefaultKeyspaceRF(1);
        Guardrails.instance.setMinimumReplicationFactorThreshold(1, 1);
        try
        {
            DatabaseDescriptor.setBroadcastAddress(java.net.InetAddress.getByName("127.0.0.1"));
        }
        catch (java.net.UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @After
    public void cleanup()
    {
        try { ClusterMetadataService.unsetInstance(); } catch (Exception ignored) {}
    }

    /**
     * When processor.commit() returns a Failure result, handleCommitResult should invoke
     * the onFailure handler. We inject a Processor that always returns failure.
     */
    @Test
    public void testHandleCommitResultFailureCallback() throws Exception
    {
        ClusterMetadata initial = new ClusterMetadata(Murmur3Partitioner.instance);
        LocalLog log = LocalLog.logSpec()
                               .sync()
                               .withInitialState(initial)
                               .createLog();

        // Create a Processor that can fail commits
        AtomicBoolean succeed = new AtomicBoolean(true);
        AtomicLongBackedProcessor realProcessor = new AtomicLongBackedProcessor(log);
        Processor testProcessor = new Processor()
        {
            @Override
            public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry retryPolicy)
            {
                if (succeed.get()) return realProcessor.commit(entryId, transform, lastKnown, retryPolicy);
                else return Commit.Result.failed(ExceptionCode.SERVER_ERROR, "injected failure for testing");
            }

            @Override
            public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
            {
                if (succeed.get()) return realProcessor.fetchLogAndWait(waitFor, retryPolicy);
                else return ClusterMetadata.current();
            }
        };

        ClusterMetadataService service = new ClusterMetadataService(new UniformRangePlacement(),
                                                                     MetadataSnapshots.NO_OP,
                                                                     log,
                                                                     testProcessor,
                                                                     Commit.Replicator.NO_OP,
                                                                     true);
        ClusterMetadataService.setInstance(service);
        log.readyUnchecked();
        log.unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());
        service.commit(new Initialize(ClusterMetadata.current()) {
            public Result execute(ClusterMetadata prev)
            {
                ClusterMetadata next = baseState;
                DistributedSchema initialSchema = new DistributedSchema(prev.schema.getKeyspaces());
                ClusterMetadata.Transformer transformer = next.transformer().with(initialSchema);
                return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
            }
        });

        succeed.set(false);

        // Now test the failure path via commit(transform, onSuccess, onFailure)
        AtomicReference<String> failureMessage = new AtomicReference<>();
        AtomicReference<ExceptionCode> failureCode = new AtomicReference<>();

        Transformation noopTransform = new Transformation()
        {
            public Kind kind() { return Kind.CUSTOM; }
            public Result execute(ClusterMetadata metadata)
            {
                return Transformation.success(metadata.transformer(), MetaStrategy.affectedRanges(metadata));
            }
        };

        String result = service.commit(noopTransform,
                                       metadata -> "success", (code, message) -> {
                                           failureCode.set(code);
                                           failureMessage.set(message);
                                           return "failure:" + message;
                                       });

        assertEquals("failure:injected failure for testing", result);
        assertEquals(ExceptionCode.SERVER_ERROR, failureCode.get());
        assertNotNull(failureMessage.get());
        assertTrue(failureMessage.get().contains("injected failure"));
    }

    /**
     * When commit succeeds but awaitAtLeast(epoch) fails because the log can't catch up
     * to the committed epoch, handleCommitResult should throw IllegalStateException.
     */
    @Test
    public void testHandleCommitResultTimeoutException()
    {
        ClusterMetadata initial = new ClusterMetadata(Murmur3Partitioner.instance);
        LocalLog log = LocalLog.logSpec()
                               .sync()
                               .withInitialState(initial)
                               .createLog();

        /* Create a Processor that can returns a Success result with a far-future epoch
         * that the local log will never reach, causing awaitAtLeast to fail.
         */
        AtomicBoolean succeed = new AtomicBoolean(true);
        AtomicLongBackedProcessor realProcessor = new AtomicLongBackedProcessor(log);
        Processor testProcessor = new Processor()
        {
            @Override
            public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry retryPolicy)
            {
                if (succeed.get())
                {
                    return realProcessor.commit(entryId, transform, lastKnown, retryPolicy);
                }
                else
                {
                    Epoch farFuture = Epoch.create(999999);
                    return new Commit.Result.Success(farFuture, LogState.EMPTY);
                }
            }

            @Override
            public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
            {
                return ClusterMetadata.current();
            }
        };
        ClusterMetadataService service = new ClusterMetadataService(new UniformRangePlacement(),
                                                                     MetadataSnapshots.NO_OP,
                                                                     log,
                                                                     testProcessor,
                                                                     Commit.Replicator.NO_OP,
                                                                     true);
        ClusterMetadataService.setInstance(service);
        log.readyUnchecked();
        log.unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());
        service.commit(new Initialize(ClusterMetadata.current()) {
            public Result execute(ClusterMetadata prev)
            {
                ClusterMetadata next = baseState;
                DistributedSchema initialSchema = new DistributedSchema(prev.schema.getKeyspaces());
                ClusterMetadata.Transformer transformer = next.transformer().with(initialSchema);
                return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
            }
        });

        // Make the processor returns success with a far-future epoch the log can never reach
        succeed.set(false);

        Transformation noopTransform = new Transformation()
        {
            public Kind kind() { return Kind.CUSTOM; }
            public Result execute(ClusterMetadata metadata)
            {
                return Transformation.success(metadata.transformer(), MetaStrategy.affectedRanges(metadata));
            }
        };

        try
        {
            service.commit(noopTransform,
                           metadata -> "success", (code, message) -> { throw new RuntimeException("unexpected failure: " + message); });
            fail("Expected IllegalStateException wrapping TimeoutException");
        }
        catch (IllegalStateException e)
        {
            assertTrue("Should mention unreachable epoch or timeout: " + e.getMessage(),
                       e.getMessage().contains("Timed out") || e.getMessage().contains("Could not reach"));
            assertTrue("Should mention epoch: " + e.getMessage(),
                       e.getMessage().contains("999999"));
        }
    }
}
