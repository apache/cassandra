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
package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.paxos.CasForwarding.casForwardingFailure;
import static org.apache.cassandra.service.paxos.CasForwarding.readForwardingFailure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CasForwardingTest
{
    private static final String KEYSPACE1 = "CasForwardingTest";
    private static final String CF_STANDARD1 = "Standard1";

    private static final int VERSION = MessagingService.current_version;

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testCasForwardRequestWithRemoteClientState()
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD1);
        DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes("test"));
        ClientState localState = ClientState.forInternalCalls();
        localState.setKeyspace(KEYSPACE1);

        CQL3CasRequest casRequest = new CQL3CasRequest(metadata, key, RegularAndStaticColumns.NONE, true, false);

        CasForwardRequest request = new CasForwardRequest(KEYSPACE1,
                                                          CF_STANDARD1,
                                                          key,
                                                          ConsistencyLevel.QUORUM,
                                                          ConsistencyLevel.QUORUM,
                                                          System.currentTimeMillis(),
                                                          localState,
                                                          casRequest);

        assertNotNull("Client state should not be null", request.clientState);
        assertEquals("Keyspace name should match", KEYSPACE1, request.keyspaceName);
        assertEquals("CF name should match", CF_STANDARD1, request.cfName);
        assertEquals("Consistency for paxos should match", ConsistencyLevel.QUORUM, request.consistencyForPaxos);
        assertEquals("Consistency for commit should match", ConsistencyLevel.QUORUM, request.consistencyForCommit);
        assertNotNull("CAS request should not be null", request.casRequest);
    }

    @Test
    public void testExceptionResponse() throws IOException
    {
        UnavailableException exception = new UnavailableException("Test exception", ConsistencyLevel.QUORUM, 3, 1);
        CasForwardResponse response = new CasForwardResponse(exception, null);

        assertFalse("Response should not be successful", response.isSuccess());
        assertEquals("Exception should match", exception, response.exception);
        assertFalse("Result should be absent when exception is present", response.hasResult());

        CasForwardResponse deserialized = assertRoundTrips(response, null, Collections.emptyList());

        // UnavailableException rebuilds its message from the consistency level and the required and
        // alive counts, so compare those rather than the message.
        UnavailableException deserializedException = (UnavailableException) deserialized.exception;
        assertEquals("Exception type should match", exception.getClass(), deserializedException.getClass());
        assertEquals("Consistency level should match", exception.consistency, deserializedException.consistency);
        assertEquals("Required replicas should match", exception.required, deserializedException.required);
        assertEquals("Alive replicas should match", exception.alive, deserializedException.alive);
    }

    @Test
    public void testNoResultWithWarnings() throws IOException
    {
        List<String> warnings = Arrays.asList("Warning 1", "Warning 2", "Test warning message");

        // Both constructors, which is all that distinguishes the CAS verb from the read verb here.
        assertRoundTrips(new CasForwardResponse((RowIterator) null, warnings), null, warnings);
        assertRoundTrips(new CasForwardResponse((PartitionIterator) null, warnings), null, warnings);
    }

    @Test
    public void testResultRoundTrip() throws IOException
    {
        assertRoundTrips(new CasForwardResponse(twoRowResult(), null), twoRowValues(), Collections.emptyList());
    }

    @Test
    public void testConsensusReadResultRoundTrip() throws IOException
    {
        // The read forwarding verb reaches the same payload through the PartitionIterator constructor.
        assertRoundTrips(new CasForwardResponse(PartitionIterators.singletonIterator(twoRowResult()), null),
                         twoRowValues(), Collections.emptyList());
    }

    /**
     * {@link org.apache.cassandra.db.partitions.FilteredPartition} always stores rows in clustering order,
     * so a reversed multi-row result has to carry the direction it was read in.
     */
    @Test
    public void testReversedResultRoundTrip() throws IOException
    {
        // Guard the fixture: a reversed result that matched the ascending one would assert nothing
        List<String> ascending = twoRowValues(false);
        List<String> descending = twoRowValues(true);
        assertEquals("Reversed fixture should be the ascending rows in reverse",
                     Lists.reverse(ascending), descending);

        assertRoundTrips(new CasForwardResponse(twoRowResult(true), null), descending, true, Collections.emptyList());
    }

    @Test
    public void testReversedConsensusReadResultRoundTrip() throws IOException
    {
        // The read forwarding verb reaches the same payload through the PartitionIterator constructor
        assertRoundTrips(new CasForwardResponse(PartitionIterators.singletonIterator(twoRowResult(true)), null),
                         twoRowValues(true), true, Collections.emptyList());
    }

    /** A handler reads the result locally before messaging serializes it, so reading must not consume it. */
    @Test
    public void testReversedResultIsReadableRepeatedly() throws IOException
    {
        CasForwardResponse response = new CasForwardResponse(twoRowResult(true), null);

        List<String> expected = twoRowValues(true);
        for (int i = 0; i < 3; i++)
        {
            assertEquals("partitionIterator() read " + i, expected, rowValues(response.partitionIterator()));
            try (RowIterator rows = response.rowIterator())
            {
                assertTrue("rowIterator() read " + i + " should still be reversed", rows.isReverseOrder());
            }
        }

        assertRoundTrips(response, expected, true, Collections.emptyList());
    }

    @Test
    public void testResultAndWarningsRoundTrip() throws IOException
    {
        // Warnings follow the result on the wire, so they decode from the wrong offset unless
        // deserialize reads the result's bytes instead of leaving a lazy view over the stream.
        List<String> warnings = Arrays.asList("Warning 1", "Warning 2");
        assertRoundTrips(new CasForwardResponse(twoRowResult(), warnings), twoRowValues(), warnings);
    }

    @Test
    public void testEmptyResultIsDistinctFromNoResult() throws IOException
    {
        // A successful CAS reports "condition met" as an empty result, which has to stay
        // distinguishable from carrying no result at all.
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD1);
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("emptyResultKey"));

        assertRoundTrips(new CasForwardResponse(EmptyIterators.row(metadata, key, false), null),
                         Collections.emptyList(), Collections.emptyList());
    }

    /**
     * The result has to be readable an unbounded number of times, through either accessor, and reading
     * it must not consume it for the reads that follow — including the ones messaging performs.
     */
    @Test
    public void testResultIsReadableRepeatedly() throws IOException
    {
        CasForwardResponse response = new CasForwardResponse(twoRowResult(), null);

        List<String> expected = twoRowValues();
        assertEquals("Fixture should carry two rows", 2, expected.size());

        for (int i = 0; i < 3; i++)
        {
            assertEquals("partitionIterator() read " + i, expected, rowValues(response.partitionIterator()));
            assertEquals("rowIterator() read " + i,
                         expected, rowValues(PartitionIterators.singletonIterator(response.rowIterator())));
        }

        // Reading the result locally, as a handler does, must leave it intact for messaging.
        assertRoundTrips(response, expected, Collections.emptyList());
    }

    /**
     * A forwarded operation that timed out is indeterminate — it may have been applied — so it has to
     * surface as the same timeout type the non-forwarded path raises. Wrapping it in a bare
     * RuntimeException erases that, and callers (including the simulator's linearizability checker,
     * which tolerates RequestExecutionException) then treat it as an unexpected internal error.
     */
    @Test
    public void testCasForwardingTimeoutSurfacesAsCasWriteTimeout() throws UnknownHostException
    {
        RuntimeException translated = casForwardingFailure(forwardingTimeout(),
                                                                        ConsistencyLevel.SERIAL, 2);

        assertThat(translated).isInstanceOf(CasWriteTimeoutException.class)
                              .isInstanceOf(RequestExecutionException.class);
        CasWriteTimeoutException timeout = (CasWriteTimeoutException) translated;
        assertThat(timeout.consistency).isEqualTo(ConsistencyLevel.SERIAL);
        assertThat(timeout.received).isEqualTo(0);
        assertThat(timeout.blockFor).isEqualTo(2);
        assertThat(timeout.writeType).isEqualTo(WriteType.CAS);
    }

    @Test
    public void testConsensusReadForwardingTimeoutSurfacesAsReadTimeout() throws UnknownHostException
    {
        RuntimeException translated = readForwardingFailure(forwardingTimeout(), ConsistencyLevel.QUORUM, 2);

        assertThat(translated).isInstanceOf(ReadTimeoutException.class)
                              .isInstanceOf(RequestExecutionException.class);
        ReadTimeoutException timeout = (ReadTimeoutException) translated;
        assertThat(timeout.consistency).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(timeout.received).isEqualTo(0);
        assertThat(timeout.blockFor).isEqualTo(2);
    }

    /**
     * Reasons other than TIMEOUT are definite failures and must not be collapsed into a timeout, which
     * would tell the caller an operation might have applied when it did not.
     */
    @Test
    public void testCasForwardingNonTimeoutSurfacesAsWriteFailure() throws UnknownHostException
    {
        InetAddressAndPort from = InetAddressAndPort.getByName("127.0.0.1:7012");
        RuntimeException translated = casForwardingFailure(forwardingFailure(from, RequestFailure.INCOMPATIBLE_SCHEMA),
                                                                         ConsistencyLevel.SERIAL, 2);

        assertThat(translated).isInstanceOf(WriteFailureException.class)
                              .isNotInstanceOf(RequestTimeoutException.class);
        WriteFailureException failure = (WriteFailureException) translated;
        assertThat(failure.failureReasonByEndpoint)
        .containsExactly(entry(from, RequestFailureReason.INCOMPATIBLE_SCHEMA));
    }

    @Test
    public void testConsensusReadForwardingNonTimeoutSurfacesAsReadFailure() throws UnknownHostException
    {
        InetAddressAndPort from = InetAddressAndPort.getByName("127.0.0.1:7012");
        RuntimeException translated = readForwardingFailure(forwardingFailure(from, RequestFailure.UNKNOWN),
                                                                          ConsistencyLevel.QUORUM, 2);

        assertThat(translated).isInstanceOf(ReadFailureException.class)
                              .isNotInstanceOf(RequestTimeoutException.class);
        ReadFailureException failure = (ReadFailureException) translated;
        assertThat(failure.failureReasonByEndpoint)
        .containsExactly(entry(from, RequestFailureReason.UNKNOWN));
    }

    /**
     * Anything that isn't a failure response has nothing faithful to translate to, so it keeps the
     * existing wrapper rather than being reported as a timeout.
     */
    @Test
    public void testForwardingFailureWithoutFailureResponseKeepsGenericWrapper()
    {
        IllegalStateException cause = new IllegalStateException("something else went wrong");

        RuntimeException cas = casForwardingFailure(cause, ConsistencyLevel.SERIAL, 2);
        assertThat(cas).isExactlyInstanceOf(RuntimeException.class)
                       .hasMessage("Failed to forward CAS operation to replica coordinator")
                       .hasCause(cause);

        RuntimeException read = readForwardingFailure(cause, ConsistencyLevel.QUORUM, 2);
        assertThat(read).isExactlyInstanceOf(RuntimeException.class)
                        .hasMessage("Failed to forward consensus read operation to replica coordinator")
                        .hasCause(cause);
    }

    private static CasForwardResponse assertRoundTrips(CasForwardResponse response,
                                                       List<String> expectedRows,
                                                       List<String> expectedWarnings) throws IOException
    {
        return assertRoundTrips(response, expectedRows, false, expectedWarnings);
    }

    private static CasForwardResponse assertRoundTrips(CasForwardResponse response,
                                                       List<String> expectedRows,
                                                       boolean expectedReversed,
                                                       List<String> expectedWarnings) throws IOException
    {
        assertRows("Result", expectedRows, expectedReversed, response);

        byte[] bytes = serializeCheckingSize(response);
        assertArrayEquals("Repeated serialization should produce identical bytes",
                          bytes, serializeCheckingSize(response));

        CasForwardResponse deserialized;
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            deserialized = CasForwardResponse.serializer.deserialize(in, VERSION);
        }

        assertEquals("Success should survive the round trip", response.isSuccess(), deserialized.isSuccess());
        assertEquals("Warnings should survive the round trip", expectedWarnings, deserialized.warnings);
        assertRows("Deserialized result", expectedRows, expectedReversed, deserialized);

        byte[] reserialized = serializeCheckingSize(deserialized);
        // A deserialized exception picks up the stack frames of the deserialize call, so only the
        // result path is expected to re-serialize to the same bytes.
        if (response.exception == null)
            assertArrayEquals("Re-serializing the deserialized response should produce identical bytes",
                              bytes, reserialized);

        return deserialized;
    }

    private static byte[] serializeCheckingSize(CasForwardResponse response) throws IOException
    {
        long size = CasForwardResponse.serializer.serializedSize(response, VERSION);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            CasForwardResponse.serializer.serialize(response, out, VERSION);
            assertEquals("Calculated size should match actual serialized size", size, out.getLength());
            return out.toByteArray();
        }
    }

    private static void assertRows(String what, List<String> expectedRows, boolean expectedReversed, CasForwardResponse response)
    {
        if (expectedRows == null)
        {
            assertFalse(what + " should be absent", response.hasResult());
            assertNull(what + " should have no partition iterator", response.partitionIterator());
            assertNull(what + " should have no row iterator", response.rowIterator());
        }
        else
        {
            assertTrue(what + " should be present", response.hasResult());
            assertEquals(what + " rows should match", expectedRows, rowValues(response.partitionIterator()));
            assertEquals(what + " should report the direction it was read in",
                         expectedReversed, isReverseOrder(response.rowIterator()));
            assertEquals(what + " partition iterator should report the direction it was read in",
                         expectedReversed, isReverseOrder(response.partitionIterator()));
        }
    }

    private static boolean isReverseOrder(RowIterator rows)
    {
        try (RowIterator toClose = rows)
        {
            return toClose.isReverseOrder();
        }
    }

    private static boolean isReverseOrder(PartitionIterator partitions)
    {
        try (PartitionIterator toClose = partitions)
        {
            assertTrue("Result should contain a partition", toClose.hasNext());
            return isReverseOrder(toClose.next());
        }
    }

    private static RowIterator twoRowResult()
    {
        return twoRowResult(false);
    }

    private static RowIterator twoRowResult(boolean reversed)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD1);

        PartitionUpdate update = PartitionUpdate.merge(Arrays.asList(new RowUpdateBuilder(metadata, 1000L, "forwardedKey")
                                                                    .clustering("c1").add("val", "v1")
                                                                    .buildUpdate(),
                                                                    new RowUpdateBuilder(metadata, 1000L, "forwardedKey")
                                                                    .clustering("c2").add("val", "v2")
                                                                    .buildUpdate()));

        // all(update.columns()) rather than all(metadata) keeps the non-reversed fixture byte-identical to
        // the no-arg unfilteredIterator() this replaced, whose selection lands in the serialization header
        return UnfilteredRowIterators.filter(update.unfilteredIterator(ColumnFilter.all(update.columns()), Slices.ALL, reversed),
                                             FBUtilities.nowInSeconds());
    }

    private static List<String> twoRowValues()
    {
        return twoRowValues(false);
    }

    private static List<String> twoRowValues(boolean reversed)
    {
        return rowValues(PartitionIterators.singletonIterator(twoRowResult(reversed)));
    }

    private static List<String> rowValues(PartitionIterator partitions)
    {
        assertNotNull("Result should not be null", partitions);

        List<String> values = new ArrayList<>();
        try (PartitionIterator iter = partitions)
        {
            while (iter.hasNext())
            {
                try (RowIterator rows = iter.next())
                {
                    TableMetadata metadata = rows.metadata();
                    while (rows.hasNext())
                        values.add(rows.next().toString(metadata));
                }
            }
        }
        return values;
    }

    /**
     * The real shape off the wire: the failure response arrives wrapped in an ExecutionException from
     * the forwarding future, so the translation has to look through the cause chain.
     */
    private static Throwable forwardingTimeout() throws UnknownHostException
    {
        return forwardingFailure(InetAddressAndPort.getByName("127.0.0.1:7012"), RequestFailure.TIMEOUT);
    }

    private static Throwable forwardingFailure(InetAddressAndPort from, RequestFailure failure)
    {
        return new ExecutionException(new MessagingService.FailureResponseException(from, failure));
    }
}
