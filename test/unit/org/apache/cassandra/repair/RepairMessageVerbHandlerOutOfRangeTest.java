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

package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.repair.state.ParticipateState;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.*;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.*;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class RepairMessageVerbHandlerOutOfRangeTest
{

    private static final String TEST_NAME = "repair_message_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";
    private static List<TableId> tableIds;

    static
    {
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
    }

    @BeforeClass
    public static void init() throws Exception
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.recreateCMS();
        SchemaLoader.schemaDefinition(TEST_NAME);
        ClusterMetadataTestHelper.register(broadcastAddress);
        ServerTestUtils.markCMS();
        StorageService.instance.unsafeSetInitialized();
        tableIds = Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata().id);
    }

    @Before
    public void setup() throws Exception
    {
        ServerTestUtils.resetCMS();
        // All tests suppose a 2 node ring, with the other peer having the tokens 0, 200, 300
        // Initially, the local node has no tokens so when indivividual test set owned tokens or
        // pending ranges for the local node, they're always in relation to this.
        // e.g. test calls setLocalTokens(100, 300) the ring now looks like
        // peer  -> (min, 0], (100, 200], (300, 400]
        // local -> (0, 100], (200, 300], (400, max]
        //
        // Pending ranges are set in test using start/end pairs.
        // Ring is initialised:
        // peer  -> (min, max]
        // local -> (,]
        // e.g. test calls setPendingRanges(0, 100, 200, 300)
        // the pending ranges for local would be calculated as:
        // local -> (0, 100], (200, 300]
        ClusterMetadataTestHelper.addEndpoint(node1, Lists.newArrayList(token(0), token(200), token(400)));
    }

    /*******************************************************************************
     *
     * PrepareMessage handling tests. A prepare request contains the entire set of ranges to be repaired.
     * Where subrange repairs are not being used (or potentially where the specified range intersects the
     * ranges of multiple instances) asserting the ranges at this point could cause the PREPARE message to
     * be rejected by some or all participants and so the parent repair fails.
     *
     ******************************************************************************/

    @Test
    public void testPrepareWithAllRequestedRangesWithinOwned() throws Exception
    {
        setLocalTokens(100);
        PrepareMessage prepare = prepareMsg(generateRanges(10, 20));
        tryPrepareExpectingSuccess(prepare);
    }

    @Test
    public void testPrepareWithAllRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        PrepareMessage prepare = prepareMsg(generateRanges(110, 120));
        tryPrepareExpectingSuccess(prepare);
    }

    @Test
    public void testPrepareWithSomeRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        PrepareMessage prepare = prepareMsg(generateRanges(10, 20, 110, 120));
        tryPrepareExpectingSuccess(prepare);
    }

    /*******************************************************************************
     *
     * ValidationRequest handling tests. The ranges in Validation requests _are_ verified
     * as the repair coordinator should tailor the ranges for these to the specific endpoint.
     *
     ******************************************************************************/

    @Test
    public void testValidationRequestWithRequestedRangeWithinOwned() throws Exception
    {
        setLocalTokens(100);
        ValidationRequest request = validationMsg(generateRange(10, 20));
        tryValidationExpectingSuccess(request, false);
    }

    @Test
    public void testValidationRequestWithRequestedRangeOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        ValidationRequest request = validationMsg(generateRange(110, 120));
        tryValidationExpectingFailure(request);
    }

    @Test
    public void testValidationRequestWithRequestedRangeOverlappingOwned() throws Exception
    {
        setLocalTokens(100);
        ValidationRequest request = validationMsg(generateRange(10, 120));
        tryValidationExpectingFailure(request);
    }

    private static void tryValidationExpectingFailure(ValidationRequest request) throws Exception
    {
        tryValidation(request, true, false);
    }

    private static void tryValidationExpectingSuccess(ValidationRequest request, boolean isOutOfRange) throws Exception
    {
        tryValidation(request, isOutOfRange, true);
    }

    private static void tryValidation(ValidationRequest request, boolean isOutOfRange, boolean expectSuccess) throws Exception
    {
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().inboundSink.clear();
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink(Verb.REPAIR_RSP);
        RepairMessageVerbHandler handler = new RepairMessageVerbHandler(SharedContext.Global.instance);
        int messageId = randomInt();
        // message must be prepared first as validate checks it is registered.
        PrepareMessage prepare = prepareMsg(request.desc.parentSessionId, request.desc.ranges);
        ActiveRepairService.instance().register(new ParticipateState(SharedContext.Global.instance.clock(), node1, prepare));
        Message<RepairMessage> message = Message.builder(Verb.VALIDATION_REQ, (RepairMessage)request).from(node1).withId(messageId).build();
        handler.doVerb(message);
        ClusterMetadataTestHelper.MessageDelivery response = messageSink.get(500, TimeUnit.MILLISECONDS);
        if (expectSuccess)
        {
            assertEquals(Verb.VALIDATION_RSP, response.message.verb());
            assertEquals(broadcastAddress, response.message.from());
            assertEquals(node1, response.to);
            assertTrue(response.message.payload instanceof ValidationResponse);
            ValidationResponse completion = (ValidationResponse) response.message.payload;
            assertTrue(completion.success());
            assertEquals(startMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
        }
        else
        {
            assertEquals(Verb.FAILURE_RSP, response.message.verb());
            assertEquals(broadcastAddress, response.message.from());
            assertEquals(node1, response.to);
            assertEquals(startMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
        }
    }

    private static void tryPrepareExpectingSuccess(PrepareMessage prepare) throws Exception
    {
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().inboundSink.clear();
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        RepairMessageVerbHandler handler = new RepairMessageVerbHandler(SharedContext.Global.instance);
        int messageId = randomInt();
        Message<RepairMessage> message = Message.builder(Verb.PREPARE_MSG, (RepairMessage)prepare).from(node1).withId(messageId).build();
        handler.doVerb(message);

        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(Verb.REPAIR_RSP, response.message.verb());
        assertEquals(broadcastAddress, response.message.from());
        assertEquals(messageId, response.message.id());
        assertEquals(node1, response.to);
        assertFalse(response.message.payload instanceof RequestFailureReason);
        assertEquals(startMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static PrepareMessage prepareMsg(Collection<Range<Token>> ranges)
    {
        return prepareMsg(uuid(), ranges);
    }
    private static PrepareMessage prepareMsg(TimeUUID parentRepairSession, Collection<Range<Token>> ranges)
    {
        return new PrepareMessage(parentRepairSession, tableIds, Murmur3Partitioner.instance, ranges, false, ActiveRepairService.UNREPAIRED_SSTABLE, true, PreviewKind.NONE);
    }

    private static ValidationRequest validationMsg(Range<Token> range)
    {
        TimeUUID parentId = uuid();
        List<ColumnFamilyStore> stores = tableIds.stream()
                                                 .map(Schema.instance::getColumnFamilyStoreInstance)
                                                 .collect(Collectors.toList());
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                 node1,
                                                                 stores,
                                                                 Collections.singleton(range),
                                                                 false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 true,
                                                                 PreviewKind.NONE);
        return new ValidationRequest(new RepairJobDesc(parentId, uuid(), KEYSPACE, TABLE, Collections.singleton(range)),
                                     randomInt());
    }

    public static TimeUUID uuid()
    {
        return nextTimeUUID();
    }
}

