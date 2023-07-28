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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.github.jamm.MemoryMeter;
import org.github.jamm.MemoryMeter.ByteBufferMode;
import org.github.jamm.MemoryMeter.Guess;

import static org.github.jamm.MemoryMeterStrategy.MEMORY_LAYOUT;
import static org.github.jamm.utils.ArrayMeasurementUtils.computeArraySize;

/**
 * A convenience class for wrapping access to MemoryMeter. Should be used instead of using a {@code MemoryMeter} directly.
 * {@code MemoryMeter} can be used directly for testing as it allow a more fine tuned configuration for comparison.
 */
public class ObjectSizes
{
    private static final MemoryMeter meter = MemoryMeter.builder().withGuessing(Guess.INSTRUMENTATION_AND_SPECIFICATION,
                                                                                Guess.UNSAFE)
                                                                  .build();

    private static final long HEAP_BUFFER_SHALLOW_SIZE = measure(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    private static final long DIRECT_BUFFER_SHALLOW_SIZE = measure(ByteBuffer.allocateDirect(0));
    private static final long DIRECT_BUFFER_DEEP_SIZE = measureDeep(ByteBuffer.allocateDirect(0));

    public static final long IPV6_SOCKET_ADDRESS_SIZE = ObjectSizes.measureDeep(new InetSocketAddress(getIpvAddress(16), 42));

    /**
     * Memory a byte array consumes
     *
     * @param bytes byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(byte[] bytes)
    {
        return meter.measureArray(bytes);
    }

    /**
     * Memory a long array consumes
     *
     * @param longs byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(long[] longs)
    {
        return meter.measureArray(longs);
    }

    /**
     * Memory an int array consumes
     *
     * @param ints byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(int[] ints)
    {
        return meter.measureArray(ints);
    }

    /**
     * Memory a reference array consumes
     *
     * @param length the length of the reference array
     * @return heap-size of the array
     */
    public static long sizeOfReferenceArray(int length)
    {
        return sizeOfArray(length, MEMORY_LAYOUT.getReferenceSize());
    }

    /**
     * Memory a reference array consumes itself only
     *
     * @param objects the array to size
     * @return heap-size of the array (excluding memory retained by referenced objects)
     */
    public static long sizeOfArray(Object[] objects)
    {
        return meter.measureArray(objects);
    }

    private static long sizeOfArray(int length, int elementSize)
    {
        return computeArraySize(MEMORY_LAYOUT.getArrayHeaderSize(), length, elementSize, MEMORY_LAYOUT.getObjectAlignment());
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
     * by the array itself and for each included byte buffer using {@link #sizeOnHeapExcludingDataOf(ByteBuffer)}.
     */
    public static long sizeOnHeapExcludingDataOf(ByteBuffer[] array)
    {
        if (array == null)
            return 0;

        long sum = sizeOfArray(array);
        for (ByteBuffer b : array)
            sum += sizeOnHeapExcludingDataOf(b);

        return sum;
    }

    /**
     * Measures the heap memory used by the specified byte buffer. If the buffer is a slab only the data size will be
     * counted but not the internal overhead. A SLAB is assumed to be created by: {@code buffer.duplicate().position(start).limit(end)} without the use of {@code slice()}.
     * <p>This method makes a certain amount of assumptions:
     *   <ul>
     *       <li>That slabs are always created using: {@code buffer.duplicate().position(start).limit(end)} and not through slice</li>
     *       <li>That the input buffers are not read-only buffers</li>
     *       <li>That the direct buffers that are not slab are not duplicates</li>  
     *   </ul>
     *  Non-respect of those assumptions can lead to an invalid value being returned.
     * @param buffer the buffer to measure
     * @return the heap memory used by the specified byte buffer.
     */
    public static long sizeOnHeapOf(ByteBuffer buffer)
    {
        if (buffer == null)
            return 0;

        assert !buffer.isReadOnly();

        // We assume here that slabs are always created using: buffer.duplicate().position(start).limit(end) and not through slice
        if (ByteBufferMode.SLAB_ALLOCATION_NO_SLICE.isSlab(buffer))
        {
            if (buffer.isDirect())
                return DIRECT_BUFFER_SHALLOW_SIZE; // We ignore the underlying buffer

            return HEAP_BUFFER_SHALLOW_SIZE + buffer.remaining(); // We ignore the array overhead
        }

        if (buffer.isDirect())
            return DIRECT_BUFFER_DEEP_SIZE; // That might not be true if the buffer is a view of another buffer so we could undercount

        return HEAP_BUFFER_SHALLOW_SIZE + meter.measureArray(buffer.array());
    }

    /**
     * Measures the heap memory used by the specified byte buffer excluding the data. If the buffer shallow size will be counted.
     * A SLAB is assumed to be created by: {@code buffer.duplicate().position(start).limit(end)} without the use of {@code slice()}.
     * <p>This method makes a certain amount of assumptions:
     *   <ul>
     *       <li>That slabs are always created using: {@code buffer.duplicate().position(start).limit(end)} and not through slice</li>
     *       <li>That the input buffers are not read-only buffers</li>
     *       <li>That the direct buffers that are not slab are not duplicates</li>  
     *   </ul>
     *  Non-respect of those assumptions can lead to an invalid value being returned. T 
     * @param buffer the buffer to measure
     * @return the heap memory used by the specified byte buffer excluding the data..
     */
    public static long sizeOnHeapExcludingDataOf(ByteBuffer buffer)
    {
        if (buffer == null)
            return 0;

        assert !buffer.isReadOnly();

        // We assume here that slabs are always created using: buffer.duplicate().position(start).limit(end) and not through slice
        if (ByteBufferMode.SLAB_ALLOCATION_NO_SLICE.isSlab(buffer))
        {
            if (buffer.isDirect())
                return DIRECT_BUFFER_SHALLOW_SIZE; // We ignore the underlying buffer

            return HEAP_BUFFER_SHALLOW_SIZE; // We ignore the array overhead
        }

        if (buffer.isDirect())
            return DIRECT_BUFFER_DEEP_SIZE; // That might not be true if the buffer is a view of another buffer so we could undercount

        byte[] bytes = buffer.array();
        return HEAP_BUFFER_SHALLOW_SIZE + meter.measureArray(bytes) - bytes.length;
    }

    /**
     * Memory a String consumes
     *
     * @param str String to calculate memory size of
     * @return Total in-memory size of the String
     */
    public static long sizeOf(String str)
    {
        return meter.measureStringDeep(str);
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
     * @return The size on the heap of the instance and all retained heap referenced by it, excluding portions of
     * ByteBuffer that are not directly referenced by it but including any other referenced that may also be retained
     * by other objects. This also includes bytes referenced in direct byte buffers, and may double-count memory if
     * it is referenced by multiple ByteBuffer copies.
     */
    public static long measureDeepOmitShared(Object pojo)
    {
        return meter.measureDeep(pojo, ByteBufferMode.SLAB_ALLOCATION_NO_SLICE);
    }

    /**
     * @param pojo the object to measure
     * @return the size on the heap of the instance only, excluding any referenced objects
     */
    public static long measure(Object pojo)
    {
        return meter.measure(pojo);
    }

    private static InetAddress getIpvAddress(int size)
    {
        if (size == 16 || size ==4)
        {
            try
            {
                return InetAddress.getByAddress(new byte[size]);
            }
            catch (UnknownHostException e)
            {
                throw new IllegalArgumentException("Invalid size of a byte array when getting and ipv address: " + size);
            }
        }
        else throw new IllegalArgumentException("Excpected a byte array size of 4 or 16 for an ipv address but got: " + size);
    }
}
