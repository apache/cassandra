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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterFactory
{
    private static final Logger logger = LoggerFactory.getLogger(FilterFactory.class);
    private static final TypeSizes TYPE_SIZES = TypeSizes.NATIVE;

    public enum Type
    {
        SHA, MURMUR2, MURMUR3
    }

    public static void serialize(Filter bf, DataOutput output) throws IOException
    {
        serialize(bf, output, Type.MURMUR3);
    }

    public static void serialize(Filter bf, DataOutput output, Type type) throws IOException
    {
        switch (type)
        {
            case SHA:
                LegacyBloomFilter.serializer.serialize((LegacyBloomFilter) bf, output);
                break;
            case MURMUR2:
                Murmur2BloomFilter.serializer.serialize((Murmur2BloomFilter) bf, output);
                break;
            default:
                Murmur3BloomFilter.serializer.serialize((Murmur3BloomFilter) bf, output);
                break;
        }
    }

    public static Filter deserialize(DataInput input, Type type) throws IOException
    {
        switch (type)
        {
            case SHA:
                return LegacyBloomFilter.serializer.deserialize(input);
            case MURMUR2:
                return Murmur2BloomFilter.serializer.deserialize(input);
            default:
                return Murmur3BloomFilter.serializer.deserialize(input);
        }
    }

    public static long serializedSize(Filter bf)
    {
        return serializedSize(bf, Type.MURMUR3);
    }

    public static long serializedSize(Filter bf, Type type)
    {
        switch (type)
        {
            case SHA:
                return LegacyBloomFilter.serializer.serializedSize((LegacyBloomFilter) bf);
            case MURMUR2:
                return Murmur2BloomFilter.serializer.serializedSize((Murmur2BloomFilter) bf, TYPE_SIZES);
            default:
                return Murmur3BloomFilter.serializer.serializedSize((Murmur3BloomFilter) bf, TYPE_SIZES);
        }
    }

    /**
     * @return A BloomFilter with the lowest practical false positive
     *         probability for the given number of elements.
     */
    public static Filter getFilter(long numElements, int targetBucketsPerElem)
    {
        return getFilter(numElements, targetBucketsPerElem, Type.MURMUR3);
    }

    // helper method for test.
    static Filter getFilter(long numElements, int targetBucketsPerElem, Type type)
    {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn(String.format("Cannot provide an optimal BloomFilter for %d elements (%d/%d buckets per element).", numElements, bucketsPerElement, targetBucketsPerElem));
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return createFilter(spec.K, numElements, spec.bucketsPerElement, type);
    }

    /**
     * @return The smallest BloomFilter that can provide the given false
     *         positive probability rate for the given number of elements.
     *
     *         Asserts that the given probability can be satisfied using this
     *         filter.
     */
    public static Filter getFilter(long numElements, double maxFalsePosProbability)
    {
        return getFilter(numElements, maxFalsePosProbability, Type.MURMUR3);
    }

    // helper method for test.
    static Filter getFilter(long numElements, double maxFalsePosProbability, Type type)
    {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        return createFilter(spec.K, numElements, spec.bucketsPerElement, type);
    }

    private static Filter createFilter(int hash, long numElements, int bucketsPer, Type type)
    {
        switch (type)
        {
            case MURMUR2:
              return new Murmur2BloomFilter(hash, numElements, bucketsPer);
            default:
              return new Murmur3BloomFilter(hash, numElements, bucketsPer);
        }
    }

    public static BloomFilter emptyFilter()
    {
        return new Murmur3BloomFilter(0, 0, 0);
    }
}
