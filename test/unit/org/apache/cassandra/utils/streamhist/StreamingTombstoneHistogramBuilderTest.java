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
package org.apache.cassandra.utils.streamhist;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.psjava.util.AssertStatus;
import org.quicktheories.core.Gen;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;
import static org.quicktheories.generators.SourceDSL.lists;

public class StreamingTombstoneHistogramBuilderTest
{
    @Test
    public void testFunction() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);
        int[] samples = new int[]{ 23, 19, 10, 16, 36, 2, 9, 32, 30, 45 };

        // add 7 points to histogram of 5 bins
        for (int i = 0; i < 7; i++)
        {
            builder.update(samples[i]);
        }

        // should end up (2,1),(9.5,2),(17.5,2),(23,1),(36,1)
        Map<Double, Long> expected1 = new LinkedHashMap<Double, Long>(5);
        expected1.put(2.0, 1L);
        expected1.put(10.0, 2L);
        expected1.put(17.0, 2L);
        expected1.put(23.0, 1L);
        expected1.put(36.0, 1L);

        Iterator<Map.Entry<Double, Long>> expectedItr = expected1.entrySet().iterator();
        TombstoneHistogram hist = builder.build();
        hist.forEach((point, value) ->
                     {
                         Map.Entry<Double, Long> entry = expectedItr.next();
                         assertEquals(entry.getKey(), point, 0.01);
                         assertEquals(entry.getValue().longValue(), value);
                     });

        // sum test
        assertEquals(3.42, hist.sum(15), 0.01);
        // sum test (b > max(hist))
        assertEquals(7.0, hist.sum(50), 0.01);
    }

    @Test
    public void testSerDe() throws Exception
    {
        testSerDe(TombstoneHistogram.HistogramSerializer.instance);
        testSerDe(TombstoneHistogram.LegacyHistogramSerializer.instance);
    }

    private void testSerDe(TombstoneHistogram.HistogramSerializer serializer) throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);
        int[] samples = new int[]{ 23, 19, 10, 16, 36, 2, 9 };

        // add 7 points to histogram of 5 bins
        for (int i = 0; i < samples.length; i++)
        {
            builder.update(samples[i]);
        }
        TombstoneHistogram hist = builder.build();
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(hist, out);
        byte[] bytes = out.toByteArray();

        TombstoneHistogram deserialized = serializer.deserialize(new DataInputBuffer(bytes));

        // deserialized histogram should have following values
        Map<Double, Long> expected1 = new LinkedHashMap<Double, Long>(5);
        expected1.put(2.0, 1L);
        expected1.put(10.0, 2L);
        expected1.put(17.0, 2L);
        expected1.put(23.0, 1L);
        expected1.put(36.0, 1L);

        Iterator<Map.Entry<Double, Long>> expectedItr = expected1.entrySet().iterator();
        deserialized.forEach((point, value) ->
                             {
                                 Map.Entry<Double, Long> entry = expectedItr.next();
                                 assertEquals(entry.getKey(), point, 0.01);
                                 assertEquals(entry.getValue().longValue(), value);
                             });
    }

    @Test
    public void testNumericTypes() throws Exception
    {
        testNumericTypes(TombstoneHistogram.HistogramSerializer.instance);
        testNumericTypes(TombstoneHistogram.LegacyHistogramSerializer.instance);
    }

    private void testNumericTypes(TombstoneHistogram.HistogramSerializer serializer) throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);

        builder.update(2);
        builder.update(2);
        builder.update(2);
        builder.update(2, Integer.MAX_VALUE); // To check that value overflow is handled correctly
        TombstoneHistogram hist = builder.build();
        Map<Long, Integer> asMap = asMap(hist);
        assertEquals(Integer.MAX_VALUE, asMap.get(2L).intValue());

        //Make sure it's working with Serde
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(hist, out);
        byte[] bytes = out.toByteArray();

        TombstoneHistogram deserialized = serializer.deserialize(new DataInputBuffer(bytes));

        asMap = asMap(deserialized);
        assertEquals(1, deserialized.size());
        assertEquals(Integer.MAX_VALUE, asMap.get(2L).intValue());
    }

    @Test
    public void testOverflow() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 10, 1);
        int[] samples = new int[]{ 23, 19, 10, 16, 36, 2, 9, 32, 30, 45, 31,
                                   32, 32, 33, 34, 35, 70, 78, 80, 90, 100,
                                   32, 32, 33, 34, 35, 70, 78, 80, 90, 100
        };

        // Hit the spool cap, force it to make bins
        for (int i = 0; i < samples.length; i++)
        {
            builder.update(samples[i]);
        }

        assertEquals(5, builder.build().size());
    }

    @Test
    public void testRounding() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 10, 60);
        int[] samples = new int[]{ 59, 60, 119, 180, 181, 300 }; // 60, 60, 120, 180, 240, 300
        for (int i = 0; i < samples.length; i++)
            builder.update(samples[i]);
        TombstoneHistogram hist = builder.build();
        assertEquals(hist.size(), 5);
        assertEquals(asMap(hist).get(60L).intValue(), 2);
        assertEquals(asMap(hist).get(120L).intValue(), 1);
    }

    @Test
    public void testLargeValues() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);
        IntStream.range(Integer.MAX_VALUE - 30, Integer.MAX_VALUE).forEach(builder::update);
    }

    @Test
    public void testLargeDeletionTimesAndLargeValuesDontCauseOverflow()
    {
        qt().forAll(streamingTombstoneHistogramBuilderGen(1000, 300000, 60),
                    lists().of(longs().from(0).upTo(Cell.MAX_DELETION_TIME)).ofSize(300),
                    lists().of(integers().allPositive()).ofSize(300))
            .checkAssert(this::updateHistogramAndCheckAllBucketsArePositive);
    }

    static int iter = 0;
    private void updateHistogramAndCheckAllBucketsArePositive(StreamingTombstoneHistogramBuilder histogramBuilder, List<Long> keys, List<Integer> values)
    {
        for (int i = 0; i < keys.size(); i++)
        {
            histogramBuilder.update(keys.get(i), values.get(i));
        }

        TombstoneHistogram histogram = histogramBuilder.build();
        for (Map.Entry<Long, Integer> buckets : asMap(histogram).entrySet())
        {
            assertTrue("Invalid bucket key", buckets.getKey() >= 0);
            assertTrue("Invalid bucket value", buckets.getValue() >= 0);
        }
    }

    @Test
    public void testThatPointIsNotMissedBecauseOfRoundingToNoDeletionTime() throws Exception
    {
        long pointThatRoundedToNoDeletion = Cell.NO_DELETION_TIME - 2;
        assert pointThatRoundedToNoDeletion + pointThatRoundedToNoDeletion % 3 == Cell.NO_DELETION_TIME : "test data should be valid";

        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 10, 3);
        builder.update(pointThatRoundedToNoDeletion);

        TombstoneHistogram histogram = builder.build();

        Map<Long, Integer> integerIntegerMap = asMap(histogram);
        assertEquals(integerIntegerMap.size(), 1);
        assertEquals(integerIntegerMap.get(Cell.MAX_DELETION_TIME).intValue(), 1);
    }

    @Test
    public void testInvalidArguments()
    {
        assertThatThrownBy(() -> new StreamingTombstoneHistogramBuilder(5, 10, 0)).hasMessage("Invalid arguments: maxBinSize:5 maxSpoolSize:10 delta:0");
        assertThatThrownBy(() -> new StreamingTombstoneHistogramBuilder(5, 10, -1)).hasMessage("Invalid arguments: maxBinSize:5 maxSpoolSize:10 delta:-1");
        assertThatThrownBy(() -> new StreamingTombstoneHistogramBuilder(5, -1, 60)).hasMessage("Invalid arguments: maxBinSize:5 maxSpoolSize:-1 delta:60");
        assertThatThrownBy(() -> new StreamingTombstoneHistogramBuilder(-1, 10, 60)).hasMessage("Invalid arguments: maxBinSize:-1 maxSpoolSize:10 delta:60");
        assertThatThrownBy(() -> new StreamingTombstoneHistogramBuilder(0, 10, 60)).hasMessage("Invalid arguments: maxBinSize:0 maxSpoolSize:10 delta:60");
    }

    @Test
    public void testSpool()
    {
        StreamingTombstoneHistogramBuilder.Spool spool = new StreamingTombstoneHistogramBuilder.Spool(8);
        assertTrue(spool.tryAddOrAccumulate(5, 1));
        assertSpool(spool, 5, 1);
        assertTrue(spool.tryAddOrAccumulate(5, 3));
        assertSpool(spool, 5, 4);

        assertTrue(spool.tryAddOrAccumulate(10, 1));
        assertSpool(spool, 5, 4,
                    10, 1);

        assertTrue(spool.tryAddOrAccumulate(12, 1));
        assertTrue(spool.tryAddOrAccumulate(14, 1));
        assertTrue(spool.tryAddOrAccumulate(16, 1));
        assertSpool(spool, 5, 4,
                    10, 1,
                    12, 1,
                    14, 1,
                    16, 1);

        assertTrue(spool.tryAddOrAccumulate(18, 1));
        assertTrue(spool.tryAddOrAccumulate(20, 1));
        assertTrue(spool.tryAddOrAccumulate(30, 1));
        assertSpool(spool, 5, 4,
                    10, 1,
                    12, 1,
                    14, 1,
                    16, 1,
                    18, 1,
                    20, 1,
                    30, 1);

        assertTrue(spool.tryAddOrAccumulate(16, 5));
        assertTrue(spool.tryAddOrAccumulate(12, 4));
        assertTrue(spool.tryAddOrAccumulate(18, 9));
        assertSpool(spool,
                    5, 4,
                    10, 1,
                    12, 5,
                    14, 1,
                    16, 6,
                    18, 10,
                    20, 1,
                    30, 1);

        assertTrue(spool.tryAddOrAccumulate(99, 5));
    }

    @Test
    public void testDataHolder()
    {
        StreamingTombstoneHistogramBuilder.DataHolder dataHolder = new StreamingTombstoneHistogramBuilder.DataHolder(4, 1);
        assertFalse(dataHolder.isFull());
        assertEquals(0, dataHolder.size());

        assertTrue(dataHolder.addValue(4, 1));
        assertDataHolder(dataHolder,
                         4, 1);

        assertFalse(dataHolder.addValue(4, 1));
        assertDataHolder(dataHolder,
                         4, 2);

        assertTrue(dataHolder.addValue(7, 1));
        assertDataHolder(dataHolder,
                         4, 2,
                         7, 1);

        assertFalse(dataHolder.addValue(7, 1));
        assertDataHolder(dataHolder,
                         4, 2,
                         7, 2);

        assertTrue(dataHolder.addValue(5, 1));
        assertDataHolder(dataHolder,
                         4, 2,
                         5, 1,
                         7, 2);

        assertFalse(dataHolder.addValue(5, 1));
        assertDataHolder(dataHolder,
                         4, 2,
                         5, 2,
                         7, 2);

        assertTrue(dataHolder.addValue(2, 1));
        assertDataHolder(dataHolder,
                         2, 1,
                         4, 2,
                         5, 2,
                         7, 2);
        assertTrue(dataHolder.isFull());

        // expect to merge [4,2]+[5,2]
        dataHolder.mergeNearestPoints();
        assertDataHolder(dataHolder,
                         2, 1,
                         5, 4,
                         7, 2);

        assertFalse(dataHolder.addValue(2, 1));
        assertDataHolder(dataHolder,
                         2, 2,
                         5, 4,
                         7, 2);

        dataHolder.addValue(8, 1);
        assertDataHolder(dataHolder,
                         2, 2,
                         5, 4,
                         7, 2,
                         8, 1);
        assertTrue(dataHolder.isFull());

        // expect to merge [7,2]+[8,1]
        dataHolder.mergeNearestPoints();
        assertDataHolder(dataHolder,
                         2, 2,
                         5, 4,
                         8, 3);
    }

    private static void assertDataHolder(StreamingTombstoneHistogramBuilder.DataHolder dataHolder, int... pointValue)
    {
        assertEquals(pointValue.length / 2, dataHolder.size());

        for (int i = 0; i < pointValue.length; i += 2)
        {
            int point = pointValue[i];
            int expectedValue = pointValue[i + 1];
            assertEquals(expectedValue, dataHolder.getValue(point));
        }
    }

    /**
     * Compare the contents of {@code spool} with the given collection of key-value pairs in {@code pairs}.
     */
    private static void assertSpool(StreamingTombstoneHistogramBuilder.Spool spool, int... pairs)
    {
        assertEquals(pairs.length / 2, spool.size);
        Map<Long, Integer> tests = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2)
            tests.put((long) pairs[i], pairs[i + 1]);

        spool.forEach((k, v) -> {
            Integer x = tests.remove(k);
            assertNotNull("key " + k, x);
            assertEquals(x.intValue(), v);
        });
        AssertStatus.assertTrue(tests.isEmpty());
    }

    private Map<Long, Integer> asMap(TombstoneHistogram histogram)
    {
        Map<Long, Integer> result = new HashMap<>();
        histogram.forEach(result::put);
        return result;
    }

    private Gen<StreamingTombstoneHistogramBuilder> streamingTombstoneHistogramBuilderGen(int maxBinSize, int maxSpoolSize, int maxRoundSeconds)
    {
        return positiveIntegerUpTo(maxBinSize).zip(integers().between(0, maxSpoolSize),
                                                   positiveIntegerUpTo(maxRoundSeconds),
                                                   StreamingTombstoneHistogramBuilder::new);
    }

    private Gen<Integer> positiveIntegerUpTo(int upperBound)
    {
        return integers().between(1, upperBound);
    }
}
