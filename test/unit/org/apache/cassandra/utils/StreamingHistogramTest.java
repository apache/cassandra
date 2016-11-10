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
package org.apache.cassandra.utils;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.*;

import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertEquals;

public class StreamingHistogramTest
{
    @Test
    public void testFunction() throws Exception
    {
        StreamingHistogram hist = new StreamingHistogram(5);
        long[] samples = new long[]{23, 19, 10, 16, 36, 2, 9, 32, 30, 45};

        // add 7 points to histogram of 5 bins
        for (int i = 0; i < 7; i++)
        {
            hist.update(samples[i]);
        }

        // should end up (2,1),(9.5,2),(17.5,2),(23,1),(36,1)
        Map<Double, Long> expected1 = new LinkedHashMap<Double, Long>(5);
        expected1.put(2.0, 1L);
        expected1.put(9.5, 2L);
        expected1.put(17.5, 2L);
        expected1.put(23.0, 1L);
        expected1.put(36.0, 1L);

        Iterator<Map.Entry<Double, Long>> expectedItr = expected1.entrySet().iterator();
        for (Map.Entry<Double, Long> actual : hist.getAsMap().entrySet())
        {
            Map.Entry<Double, Long> entry = expectedItr.next();
            assertEquals(entry.getKey(), actual.getKey(), 0.01);
            assertEquals(entry.getValue(), actual.getValue());
        }

        // merge test
        StreamingHistogram hist2 = new StreamingHistogram(3);
        for (int i = 7; i < samples.length; i++)
        {
            hist2.update(samples[i]);
        }
        hist.merge(hist2);
        // should end up (2,1),(9.5,2),(19.33,3),(32.67,3),(45,1)
        Map<Double, Long> expected2 = new LinkedHashMap<Double, Long>(5);
        expected2.put(2.0, 1L);
        expected2.put(9.5, 2L);
        expected2.put(19.33, 3L);
        expected2.put(32.67, 3L);
        expected2.put(45.0, 1L);
        expectedItr = expected2.entrySet().iterator();
        for (Map.Entry<Double, Long> actual : hist.getAsMap().entrySet())
        {
            Map.Entry<Double, Long> entry = expectedItr.next();
            assertEquals(entry.getKey(), actual.getKey(), 0.01);
            assertEquals(entry.getValue(), actual.getValue());
        }

        // sum test
        assertEquals(3.28, hist.sum(15), 0.01);
        // sum test (b > max(hist))
        assertEquals(10.0, hist.sum(50), 0.01);
    }

    @Test
    public void testSerDe() throws Exception
    {
        StreamingHistogram hist = new StreamingHistogram(5);
        long[] samples = new long[]{23, 19, 10, 16, 36, 2, 9};

        // add 7 points to histogram of 5 bins
        for (int i = 0; i < samples.length; i++)
        {
            hist.update(samples[i]);
        }

        DataOutputBuffer out = new DataOutputBuffer();
        StreamingHistogram.serializer.serialize(hist, out);
        byte[] bytes = out.toByteArray();

        StreamingHistogram deserialized = StreamingHistogram.serializer.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));

        // deserialized histogram should have following values
        Map<Double, Long> expected1 = new LinkedHashMap<Double, Long>(5);
        expected1.put(2.0, 1L);
        expected1.put(9.5, 2L);
        expected1.put(17.5, 2L);
        expected1.put(23.0, 1L);
        expected1.put(36.0, 1L);

        Iterator<Map.Entry<Double, Long>> expectedItr = expected1.entrySet().iterator();
        for (Map.Entry<Double, Long> actual : deserialized.getAsMap().entrySet())
        {
            Map.Entry<Double, Long> entry = expectedItr.next();
            assertEquals(entry.getKey(), actual.getKey(), 0.01);
            assertEquals(entry.getValue(), actual.getValue());
        }
    }
}
