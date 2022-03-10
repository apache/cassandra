/*
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
 */

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

import org.github.jamm.MemoryLayoutSpecification;
import org.github.jamm.MemoryMeter;

/**
 * A convenience class for wrapping access to MemoryMeter
 */
public class ObjectSizes
{
    private static final MemoryMeter meter = new MemoryMeter().withGuessing(MemoryMeter.Guess.FALLBACK_UNSAFE)
                                                              .ignoreKnownSingletons();

    private static final long EMPTY_HEAP_BUFFER_SIZE = measure(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    private static final long EMPTY_BYTE_ARRAY_SIZE = measure(new byte[0]);
    private static final long EMPTY_STRING_SIZE = measure("");

    private static final long DIRECT_BUFFER_HEAP_SIZE = measure(ByteBuffer.allocateDirect(0));

    /**
     * Memory a byte array consumes
     *
     * @param bytes byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(byte[] bytes)
    {
        if (bytes == null)
            return 0;

        return sizeOfArray(bytes.length, 1);
    }

    /**
     * Memory a long array consumes
     *
     * @param longs byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(long[] longs)
    {
        if (longs == null)
            return 0;

        return sizeOfArray(longs.length, 8);
    }

    /**
     * Memory an int array consumes
     *
     * @param ints byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(int[] ints)
    {
        if (ints == null)
            return 0;

        return sizeOfArray(ints.length, 4);
    }

    /**
     * Memory a reference array consumes
     *
     * @param length the length of the reference array
     * @return heap-size of the array
     */
    public static long sizeOfReferenceArray(int length)
    {
        return sizeOfArray(length, MemoryLayoutSpecification.SPEC.getReferenceSize());
    }

    /**
     * Memory a reference array consumes itself only
     *
     * @param objects the array to size
     * @return heap-size of the array (excluding memory retained by referenced objects)
     */
    public static long sizeOfArray(Object[] objects)
    {
        if (objects == null)
            return 0;

        return sizeOfReferenceArray(objects.length);
    }

    private static long sizeOfArray(int length, long elementSize)
    {
        return MemoryLayoutSpecification.sizeOfArray(length, elementSize);
    }

    /**
     * Amount of heap memory consumed by the array of byte buffers. It sums memory consumed by the array itself
     * and for each included byte buffer using {@link #sizeOnHeapOf(ByteBuffer)}.
     */
    public static long sizeOnHeapOf(ByteBuffer[] array)
    {
        if (array == null)
            return 0;

        long sum = sizeOfArray(array);
        for (ByteBuffer buffer : array)
            sum += sizeOnHeapOf(buffer);

        return sum;
    }

    /**
     * Amount of non-data heap memory consumed by the array of byte buffers. It sums memory consumed
     * by the array itself and for each included byte buffer using {@link #sizeOnHeapExcludingData(ByteBuffer)}.
     */
    public static long sizeOnHeapExcludingData(ByteBuffer[] array)
    {
        if (array == null)
            return 0;

        long sum = sizeOfArray(array);
        for (ByteBuffer b : array)
            sum += sizeOnHeapExcludingData(b);

        return sum;
    }

    /**
     * @return heap memory consumed by the byte buffer. If it is a slice, it counts the data size, but it does not
     * include the internal array overhead.
     */
    public static long sizeOnHeapOf(ByteBuffer buffer)
    {
        if (buffer == null)
            return 0;

        if (buffer.isDirect())
            return DIRECT_BUFFER_HEAP_SIZE;

        int arrayLen = buffer.array().length;
        int bufLen = buffer.remaining();

        // if we're only referencing a sub-portion of the ByteBuffer, don't count the array overhead (assume it is SLAB
        // allocated - the overhead amortized over all the allocations is negligible and better to undercount than over)
        if (arrayLen > bufLen)
            return EMPTY_HEAP_BUFFER_SIZE + bufLen;

        return EMPTY_HEAP_BUFFER_SIZE + (arrayLen == 0 ? EMPTY_BYTE_ARRAY_SIZE : sizeOfArray(arrayLen, 1));
    }

    /**
     * @return non-data heap memory consumed by the byte buffer. If it is a slice, it does not include the internal
     * array overhead.
     */
    public static long sizeOnHeapExcludingData(ByteBuffer buffer)
    {
        if (buffer == null)
            return 0;

        if (buffer.isDirect())
            return DIRECT_BUFFER_HEAP_SIZE;

        int arrayLen = buffer.array().length;
        int bufLen = buffer.remaining();

        // if we're only referencing a sub-portion of the ByteBuffer, don't count the array overhead (assume it is SLAB
        // allocated - the overhead amortized over all the allocations is negligible and better to undercount than over)
        if (arrayLen > bufLen)
            return EMPTY_HEAP_BUFFER_SIZE;

        // If buffers are dedicated, account for byte array size and any padding overhead
        return EMPTY_HEAP_BUFFER_SIZE + (arrayLen == 0 ? EMPTY_BYTE_ARRAY_SIZE : (sizeOfArray(arrayLen, 1) - arrayLen));
    }

    /**
     * Memory a String consumes
     *
     * @param str String to calculate memory size of
     * @return Total in-memory size of the String
     */
    // TODO hard coding this to 2 isn't necessarily correct in Java 11
    public static long sizeOf(String str)
    {
        if (str == null)
            return 0;

        return EMPTY_STRING_SIZE + sizeOfArray(str.length(), Character.BYTES);
    }

    /**
     * @param pojo the object to measure
     * @return the size on the heap of the instance and all retained heap referenced by it, excluding portions of
     * ByteBuffer that are not directly referenced by it but including any other referenced that may also be retained
     * by other objects.
     */
    public static long measureDeep(Object pojo)
    {
        return meter.measureDeep(pojo);
    }

    /**
     * @param pojo the object to measure
     * @return the size on the heap of the instance only, excluding any referenced objects
     */
    public static long measure(Object pojo)
    {
        return meter.measure(pojo);
    }
}
