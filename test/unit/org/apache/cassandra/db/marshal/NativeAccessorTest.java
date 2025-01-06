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

package org.apache.cassandra.db.marshal;

import com.google.common.primitives.UnsignedBytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class NativeAccessorTest extends ValueAccessorTester
{
    private static final TestNativeDataAllocator allocator = new TestNativeDataAllocator();
    @BeforeClass
    public static void setSetMemoryAllocator()
    {
        NativeAccessor.setNativeMemoryAllocator(allocator);
    }

    @AfterClass
    public static void releaseMemory()
    {
        allocator.close();
    }

    @Test
    public void testCompare()
    {
        qt().forAll(accessors(),
                    byteArrays(integers().between(0, 200)),
                    byteArrays(integers().between(0, 200))
            ).checkAssert(this::testCompare);
    }

    private <V> void testCompare(ValueAccessor<V> rightAccessor, byte[] leftArray, byte[] rightArray)
    {
        NativeData left = NativeAccessor.instance.valueOf(leftArray);
        V right = rightAccessor.valueOf(rightArray);
        int expectedResult = Integer.signum(UnsignedBytes.lexicographicalComparator().compare(leftArray, rightArray));
        int actualResult = Integer.signum(NativeAccessor.instance.compare(left, right, rightAccessor));
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testCopy()
    {
        qt().forAll(accessors(),
                    byteArrays(integers().between(10, 100)),
                    integers().between(0, 9),
                    integers().between(0, 9)
        ).checkAssert(this::testCopy);
    }

    private <V> void testCopy(ValueAccessor<V> dstAccessor, byte[] dataToCopy, int srcOffset, int dstOffset)
    {
        ValueAccessor<NativeData> srcAcccessor = NativeAccessor.instance;
        NativeData src = srcAcccessor.valueOf(dataToCopy);
        V dst = dstAccessor.valueOf(new byte[dataToCopy.length + dstOffset - srcOffset]);
        NativeAccessor.instance.copyTo(src, srcOffset, dst, dstAccessor, dstOffset,  dataToCopy.length - srcOffset);
        V dstSlice = dstAccessor.slice(dst, dstOffset, dataToCopy.length - srcOffset);
        NativeData expectedData = srcAcccessor.slice(src, srcOffset, dataToCopy.length - srcOffset);

        Assert.assertArrayEquals(srcAcccessor.toArray(src, srcOffset, dataToCopy.length - srcOffset), dstAccessor.toArray(dstSlice));
        Assert.assertArrayEquals(srcAcccessor.toArray(expectedData), dstAccessor.toArray(dstSlice));

    }
}
