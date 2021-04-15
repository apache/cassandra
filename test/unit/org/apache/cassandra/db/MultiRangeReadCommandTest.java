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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiRangeReadCommandTest
{
    public static final String KEYSPACE1 = "MultiRangeReadCommandTest";
    public static final String CF_STANDARD1 = "Standard1";

    private static IPartitioner partitioner;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        partitioner = DatabaseDescriptor.getPartitioner();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
        cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
    }

    @Before
    public void setup()
    {
        cfs.clearUnsafe();
    }

    @Test
    public void verifyNotFetchingRemainingRangesOverLimit() throws InterruptedException
    {
        int rowCount = 1000;
        for (int i = 0; i < rowCount; ++i)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 10, String.valueOf(i));
            builder.clustering("c");
            builder.add("val", String.valueOf(i));
            builder.build().applyUnsafe();
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        int tokens = 100;
        DataLimits limits = DataLimits.cqlLimits(100);
        PartitionRangeReadCommand partitionRangeCommand = PartitionRangeReadCommand.allDataRead(cfs.metadata(), 10).withUpdatedLimit(limits);
        MultiRangeReadCommand command = MultiRangeReadCommand.create(partitionRangeCommand, ranges(tokens), true);

        assert cfs.metric != null;
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        long beforeMetricsRecorded = cfs.metric.liveScannedHistogram.cf.getCount();
        long beforeSSTableRead = sstable.getReadMeter().count();
        assertEquals(limits.count(), rows(command.executeLocally(command.executionController())).size());

        long metricsRecorded = cfs.metric.liveScannedHistogram.cf.getCount() - beforeMetricsRecorded;
        assertEquals(1, metricsRecorded);

        long subrangeScanned =  sstable.getReadMeter().count() - beforeSSTableRead;
        String errorMessage = String.format("Should only query enough ranges to satisfy limit, but queried %d ranges", subrangeScanned);
        assertTrue( errorMessage, subrangeScanned > 1 && subrangeScanned < tokens);
    }

    @Test
    public void testMultiRangeReadResponse()
    {
        int rowCount = 1000;
        for (int i = 0; i < rowCount; ++i)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 10, String.valueOf(i));
            builder.clustering("c");
            builder.add("val", String.valueOf(i));
            builder.build().applyUnsafe();
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        PartitionRangeReadCommand partitionRangeCommand = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        List<AbstractBounds<PartitionPosition>> ranges = ranges(100);
        MultiRangeReadCommand command = MultiRangeReadCommand.create(partitionRangeCommand, ranges, true);

        UnfilteredPartitionIterator data = command.executeLocally(command.executionController());
        MultiRangeReadResponse response = (MultiRangeReadResponse) command.createResponse(data, null);

        // verify subrange response from multi-range read responses contains all data in the subrange
        for (AbstractBounds<PartitionPosition> range : ranges)
        {
            PartitionRangeReadCommand subrange = partitionRangeCommand.forSubRange(range, false);
            ReadResponse subrangeResponse = response.subrangeResponse(command, range);

            UnfilteredPartitionIterator actual = subrangeResponse.makeIterator(subrange);
            UnfilteredPartitionIterator expected = subrange.executeLocally(subrange.executionController());
            assertData(expected, actual);
        }
    }

    @Test(expected = AssertionError.class)
    public void testEmptyRangesAssertionInCreation()
    {
        PartitionRangeReadCommand partitionRangeCommand = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        List<AbstractBounds<PartitionPosition>> ranges = Collections.EMPTY_LIST;
        MultiRangeReadCommand.create(partitionRangeCommand, ranges, true);
    }

    @Test(expected = AssertionError.class)
    public void testEmptyHandlersAssertionInCreation()
    {
        List<ReadCallback<?, ?>> subrangeHandlers = Collections.EMPTY_LIST;
        MultiRangeReadCommand.create(subrangeHandlers);
    }

    @Test
    public void testIsLimitedToOnePartition()
    {
        // Multiple ranges isn't limited
        assertFalse(command(ranges(2), true).isLimitedToOnePartition());

        // Single row bounds with different keys isn't limited
        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>();
        ranges.add(new Bounds<>(partitioner.decorateKey(UTF8Type.instance.decompose("B")),
                                partitioner.decorateKey(UTF8Type.instance.decompose("A"))));
        assertFalse(command(ranges, true).isLimitedToOnePartition());

        // Single row bounds with different keys is limited
        ranges = new ArrayList<>();
        ranges.add(new Bounds<>(partitioner.decorateKey(UTF8Type.instance.decompose("A")),
                                partitioner.decorateKey(UTF8Type.instance.decompose("A"))));
        assertTrue(command(ranges, true).isLimitedToOnePartition());
    }

    @Test
    public void testUpdatingLimitsIsReflectedInCommand()
    {
        ReadCommand command = command(ranges(2), true);
        assertTrue(command.limits() == DataLimits.NONE);
        command = command.withUpdatedLimit(DataLimits.cqlLimits(10));
        assertEquals(10, command.limits().count());
    }

    @Test
    public void testIsRangeRequestReturnsFalse()
    {
        assertFalse(command(ranges(2), true).isRangeRequest());
    }

    @Test
    public void testTimeoutReturnsRangeTimeout()
    {
        assertEquals(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.SECONDS), command(ranges(2), true).getTimeout(TimeUnit.SECONDS));
    }

    @Test
    public void testSelectsKey()
    {
        PartitionRangeReadCommand partitionRangeCommand = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>();
        ranges.add(new Bounds<>(partitioner.decorateKey(UTF8Type.instance.decompose("A")),
                                partitioner.decorateKey(UTF8Type.instance.decompose("A"))));
        MultiRangeReadCommand command = MultiRangeReadCommand.create(partitionRangeCommand, ranges, true);

        assertTrue(command.selectsKey(partitioner.decorateKey(UTF8Type.instance.decompose("A"))));
        assertFalse(command.selectsKey(partitioner.decorateKey(UTF8Type.instance.decompose("B"))));
    }

    @Test(expected = AssertionError.class)
    public void testCannotCreateResponseWithDigestQuery()
    {
        PartitionRangeReadCommand partitionRangeCommand = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        partitionRangeCommand = partitionRangeCommand.copyAsDigestQuery();
        MultiRangeReadCommand command = MultiRangeReadCommand.create(partitionRangeCommand, ranges(2), true);
        UnfilteredPartitionIterator data = command.executeLocally(command.executionController());
        command.createResponse(data, null);
    }

    @Test
    public void testResponseIsNotDigestResponse()
    {
        MultiRangeReadCommand command = command(ranges(2), true);
        MultiRangeReadResponse response = (MultiRangeReadResponse)command.createResponse(command.executeLocally(command.executionController()), null);
        assertFalse(response.isDigestResponse());
    }

    @Test
    public void testResponseIsRepairedDigestConclusiveForLocalResponse()
    {
        MultiRangeReadCommand command = command(ranges(2), true);
        MultiRangeReadResponse response = (MultiRangeReadResponse)command.createResponse(command.executeLocally(command.executionController()), null);
        assertTrue(response.isRepairedDigestConclusive());
    }

    @Test
    public void testRepairedDataDigestIsEmptyForLocalResponse()
    {
        MultiRangeReadCommand command = command(ranges(2), true);
        MultiRangeReadResponse response = (MultiRangeReadResponse)command.createResponse(command.executeLocally(command.executionController()), null);
        assertFalse(response.repairedDataDigest().hasRemaining());
    }

    @Test
    public void testMaybeIncludeRepairedDigestForLocalResponse()
    {
        MultiRangeReadCommand command = command(ranges(2), true);
        MultiRangeReadResponse response = (MultiRangeReadResponse)command.createResponse(command.executeLocally(command.executionController()), null);
        assertTrue(response.mayIncludeRepairedDigest());
    }

    @Test(expected = AssertionError.class)
    public void testCreateWithEmptyRanges()
    {
        command(Collections.EMPTY_LIST, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMultiRangeReadResponseDigest()
    {
        MultiRangeReadCommand command = command(ranges(10), true);
        UnfilteredPartitionIterator data = command.executeLocally(command.executionController());
        MultiRangeReadResponse response = (MultiRangeReadResponse) command.createResponse(data, null);
        response.digest(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMultiRangeReadResponseToDebugString()
    {
        MultiRangeReadCommand command = command(ranges(10), true);
        UnfilteredPartitionIterator data = command.executeLocally(command.executionController());
        MultiRangeReadResponse response = (MultiRangeReadResponse) command.createResponse(data, null);
        response.toDebugString(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateDigestCommand()
    {
        MultiRangeReadCommand command = command(ranges(10), true);
        command.copyAsDigestQuery();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetPager()
    {
        MultiRangeReadCommand command = command(ranges(10), true);
        command.getPager(null, ProtocolVersion.CURRENT);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExecute()
    {
        MultiRangeReadCommand command = command(ranges(10), true);
        command.execute(null, null, -1L);
    }

    @Test
    public void testSerializationRoundTrip() throws Exception
    {
        for (int tokens : Arrays.asList(2, 3, 5, 10, 63, 128))
        {
            List<AbstractBounds<PartitionPosition>> ranges = ranges(tokens);

            for (int i = 0; i < ranges.size() - 1; i++)
            {
                for (int j = i + 1; j < ranges.size(); j++)
                {
                    testSerializationRoundtrip(ranges.subList(i, j), true);
                    testSerializationRoundtrip(ranges.subList(i, j), false);
                }
            }
        }
    }

    private static MultiRangeReadCommand command(List<AbstractBounds<PartitionPosition>> subRanges, boolean isRangeContinuation)
    {
        PartitionRangeReadCommand partitionRangeCommand = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        return MultiRangeReadCommand.create(partitionRangeCommand, subRanges, isRangeContinuation);
    }

    private static void testSerializationRoundtrip(List<AbstractBounds<PartitionPosition>> subRanges, boolean isRangeContinuation) throws Exception
    {
        MultiRangeReadCommand command = command(subRanges, isRangeContinuation);
        testSerializationRoundtrip(command, command);
    }

    private static void testSerializationRoundtrip(MultiRangeReadCommand command, MultiRangeReadCommand expected) throws Exception
    {
        DataOutputBuffer output = new DataOutputBuffer();
        ReadCommand.serializer.serialize(command, output, MessagingService.current_version);
        assertEquals(ReadCommand.serializer.serializedSize(command, MessagingService.current_version), output.position());

        DataInputPlus input = new DataInputBuffer(output.buffer(), false);
        MultiRangeReadCommand deserialized = (MultiRangeReadCommand)ReadCommand.serializer.deserialize(input, MessagingService.current_version);

        assertEquals(expected.metadata().id, deserialized.metadata().id);
        assertEquals(expected.nowInSec(), deserialized.nowInSec());
        assertEquals(expected.limits(), deserialized.limits());
        assertEquals(expected.indexQueryPlan == null ? null : expected.indexQueryPlan.getFirst().getIndexMetadata(),
                     deserialized.indexQueryPlan == null ? null : deserialized.indexQueryPlan.getFirst().getIndexMetadata());
        assertEquals(expected.digestVersion(), deserialized.digestVersion());
        assertEquals(expected.ranges().size(), deserialized.ranges().size());
        Iterator<DataRange> expectedRangeIterator = expected.ranges().iterator();
        Iterator<DataRange> deserializedRangeIterator = expected.ranges().iterator();
        while (expectedRangeIterator.hasNext())
        {
            DataRange expectedRange = expectedRangeIterator.next();
            DataRange deserializedRange = deserializedRangeIterator.next();
            assertEquals(expectedRange.keyRange, deserializedRange.keyRange);
            assertEquals(expectedRange.clusteringIndexFilter, deserializedRange.clusteringIndexFilter);
        }
    }

    private List<AbstractBounds<PartitionPosition>> ranges(int numTokens)
    {
        assert numTokens >= 2;

        List<Token> tokens = new ArrayList<>(numTokens);
        tokens.add(partitioner.getMinimumToken());
        tokens.add(partitioner.getMaximumToken());

        while (tokens.size() < numTokens)
        {
            Token next = partitioner.getRandomToken();
            if (!tokens.contains(next))
                tokens.add(next);
        }
        Collections.sort(tokens);

        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>();
        for (int i = 0; i < tokens.size() - 1; i++)
        {
            Token.KeyBound left = tokens.get(i).maxKeyBound(); // exclusive
            Token.KeyBound right = tokens.get(i + 1).maxKeyBound(); // inclusive
            ranges.add(new Range<>(left, right));
        }

        return ranges;
    }

    private void assertData(UnfilteredPartitionIterator expectedResult, UnfilteredPartitionIterator actualResult)
    {
        List<Unfiltered> expected = rows(expectedResult);
        List<Unfiltered> actual = rows(actualResult);
        assertEquals(expected, actual);
    }

    private List<Unfiltered> rows(UnfilteredPartitionIterator iterator)
    {
        List<Unfiltered> unfiltered = new ArrayList<>();
        while (iterator.hasNext())
        {
            try (UnfilteredRowIterator rowIterator = iterator.next())
            {
                while (rowIterator.hasNext())
                {
                    unfiltered.add(rowIterator.next());
                }
            }
        }
        iterator.close();
        return unfiltered;
    }
}
