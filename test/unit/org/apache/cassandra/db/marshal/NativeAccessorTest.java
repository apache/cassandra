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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.primitives.UnsignedBytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.memory.BigEndianMemoryUtil;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
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

    private final ValueAccessor<NativeData> nativeAccessor = NativeAccessor.instance;
    private final ValueAccessor<ByteBuffer> bufferAccessor = ByteBufferAccessor.instance;

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

    @Test
    public void testPutMethods()
    {
        testNativePut((byte) 42, nativeAccessor::putByte, bufferAccessor::getByte);

        testNativePut((short )(Short.MAX_VALUE - 3), nativeAccessor::putShort, bufferAccessor::getShort);

        testNativePut(Integer.MAX_VALUE - 5, nativeAccessor::putInt, bufferAccessor::getInt);

        testNativePut((float) Math.PI, nativeAccessor::putFloat, bufferAccessor::getFloat);

        testNativePut(Long.MAX_VALUE - 2, nativeAccessor::putLong, bufferAccessor::getLong);

        testNativePut(0L, nativeAccessor::putVInt, bufferAccessor::getVInt);
        testNativePut(42L, nativeAccessor::putVInt, bufferAccessor::getVInt);
        testNativePut(0xFFFFFFL, nativeAccessor::putVInt, bufferAccessor::getVInt);
        testNativePut(Long.MAX_VALUE - 1, nativeAccessor::putVInt, bufferAccessor::getVInt);

        testNativePut(42, nativeAccessor::putVInt32, bufferAccessor::getVInt32);
        testNativePut(0xFFFFF, nativeAccessor::putVInt32, bufferAccessor::getVInt32);
        testNativePut(Integer.MAX_VALUE - 1, nativeAccessor::putVInt32, bufferAccessor::getVInt32);

        testNativePut(42L, nativeAccessor::putUnsignedVInt, bufferAccessor::getUnsignedVInt);
        testNativePut(0xFFFFFL, nativeAccessor::putUnsignedVInt, bufferAccessor::getUnsignedVInt);
        testNativePut(0xFFFFFFFL, nativeAccessor::putUnsignedVInt, bufferAccessor::getUnsignedVInt);
        testNativePut(Long.MAX_VALUE - 1, nativeAccessor::putUnsignedVInt, bufferAccessor::getUnsignedVInt);
    }

    @Test
    public void testPutDouble() // there is no putDouble method to test it like others
    {
        Double originalValue = Math.PI; // Double conversion is used to compare values as bit values
        NativeData nativeData = nativeAccessor.allocate(25);
        ByteBuffer bufferData = bufferAccessor.allocate(25);
        int offset = 7;
        bufferData.putDouble(offset, originalValue);
        nativeAccessor.copyByteBufferTo(bufferData, 0, nativeData, 0, bufferAccessor.size(bufferData));
        Double getValue = nativeAccessor.getDouble(nativeData, offset);
        Assert.assertEquals(originalValue, getValue);

        NativeData nativeDataSlice = nativeAccessor.slice(nativeData, offset, nativeData.nativeDataSize() - offset);
        Double toValue = nativeAccessor.toDouble(nativeDataSlice);
        Assert.assertEquals(originalValue, toValue);

    }

    private <V> void testNativePut(V originalValue, TriFunction<NativeData, Integer, V, Integer> putMethod,
                                   BiFunction<ByteBuffer, Integer, V> getMethod)
    {
        NativeData nativeData = nativeAccessor.allocate(25);
        int offset = 2;
        putMethod.apply(nativeData, offset, originalValue);
        ByteBuffer buffer = nativeAccessor.toBuffer(nativeData);
        V getValue = getMethod.apply(buffer, offset);
        Assert.assertEquals(originalValue, getValue);
    }

    @Test
    public void testGetMethods()
    {
        testNativeGet((byte) 42, bufferAccessor::putByte, nativeAccessor::getByte, nativeAccessor::toByte);

        testNativeGet((short )(Short.MAX_VALUE - 3), bufferAccessor::putShort, nativeAccessor::getShort, nativeAccessor::toShort);

        // nativeAccessor::getUnsignedShort is already tested by org.apache.cassandra.db.marshal.ValueAccessorTest.testUnsignedShort()

        testNativeGet(Integer.MAX_VALUE - 5, bufferAccessor::putInt, nativeAccessor::getInt, nativeAccessor::toInt);

        testNativeGet((float) Math.PI, bufferAccessor::putFloat, nativeAccessor::getFloat, nativeAccessor::toFloat);

        testNativeGet(Long.MAX_VALUE - 2, bufferAccessor::putLong, nativeAccessor::getLong, nativeAccessor::toLong);

        testNativeGet(0L, bufferAccessor::putVInt, nativeAccessor::getVInt, null);
        testNativeGet(42L, bufferAccessor::putVInt, nativeAccessor::getVInt, null);
        testNativeGet(0xFFFFFFL, bufferAccessor::putVInt, nativeAccessor::getVInt, null);
        testNativeGet(Long.MAX_VALUE - 1, bufferAccessor::putVInt, nativeAccessor::getVInt, null);

        testNativeGet(42, bufferAccessor::putVInt32, nativeAccessor::getVInt32, null);
        testNativeGet(0xFFFFF, bufferAccessor::putVInt32, nativeAccessor::getVInt32, null);
        testNativeGet(Integer.MAX_VALUE - 1, bufferAccessor::putVInt32, nativeAccessor::getVInt32, null);

        testNativeGet(42L, bufferAccessor::putUnsignedVInt, nativeAccessor::getUnsignedVInt, null);
        testNativeGet(0xFFFFFL, bufferAccessor::putUnsignedVInt, nativeAccessor::getUnsignedVInt, null);
        testNativeGet(0xFFFFFFFL, bufferAccessor::putUnsignedVInt, nativeAccessor::getUnsignedVInt, null);
        testNativeGet(Long.MAX_VALUE - 1, bufferAccessor::putUnsignedVInt, nativeAccessor::getUnsignedVInt, null);
    }

    private <V> void testNativeGet(V originalValue, TriFunction<ByteBuffer, Integer, V, Integer> putMethod,
                                   BiFunction<NativeData, Integer, V> getMethod, Function<NativeData, V> toMethod)
    {
        ByteBuffer bufferData = bufferAccessor.allocate(25);
        NativeData nativeData = nativeAccessor.allocate(25);
        int offset = 2;
        putMethod.apply(bufferData, offset, originalValue);
        nativeAccessor.copyByteBufferTo(bufferData, 0, nativeData, 0, bufferAccessor.size(bufferData));
        V getValue = getMethod.apply(nativeData, offset);
        Assert.assertEquals(originalValue, getValue);

        if (toMethod != null)
        {
            NativeData nativeDataSlice = nativeAccessor.slice(nativeData, offset, nativeData.nativeDataSize() - offset);
            V toValue = toMethod.apply(nativeDataSlice);
            Assert.assertEquals(originalValue, toValue);
        }
    }

    @Test
    public void testToUUID() {
        UUID originalValue = UUID.randomUUID();
        ByteBuffer encodedOriginalValue = UUIDGen.toByteBuffer(originalValue);
        int size = encodedOriginalValue.remaining();
        NativeData nativeData = nativeAccessor.allocate(size);
        nativeAccessor.copyByteBufferTo(encodedOriginalValue, 0, nativeData, 0, size);

        UUID nativeUUID = nativeAccessor.toUUID(nativeData);
        Assert.assertEquals(originalValue, nativeUUID);
    }

    @Test
    public void testToTimeUUID() {
        TimeUUID originalValue = nextTimeUUID();
        ByteBuffer encodedOriginalValue = originalValue.toBytes();
        int size = encodedOriginalValue.remaining();
        NativeData nativeData = nativeAccessor.allocate(size);
        nativeAccessor.copyByteBufferTo(encodedOriginalValue, 0, nativeData, 0, size);

        TimeUUID nativeUUID = nativeAccessor.toTimeUUID(nativeData);
        Assert.assertEquals(originalValue, nativeUUID);
    }

    @Test
    public void testToBullot() {
        Ballot originalValue = Ballot.fromUuid(nextTimeUUID().asUUID());
        ByteBuffer encodedOriginalValue = originalValue.toBytes();
        int size = encodedOriginalValue.remaining();
        NativeData nativeData = nativeAccessor.allocate(size);
        nativeAccessor.copyByteBufferTo(encodedOriginalValue, 0, nativeData, 0, size);

        Ballot nativeBallot = nativeAccessor.toBallot(nativeData);
        Assert.assertEquals(originalValue, nativeBallot);
    }

    @Test
    public void testToHex() {
        int valueSize = 42;
        byte[] originalData = new byte[valueSize];
        for (int i = 0; i < valueSize; i++)
            originalData[i] = (byte) i;

        ByteBuffer bufferData = bufferAccessor.valueOf(originalData);
        String bufferHex = bufferAccessor.toHex(bufferData);

        NativeData nativeData = nativeAccessor.valueOf(originalData);
        String nativeHex = nativeAccessor.toHex(nativeData);
        Assert.assertEquals(bufferHex, nativeHex);
    }

    @Test
    public void test() {
        NativeData nativeData = nativeAccessor.allocate(4);
        BigEndianMemoryUtil.setInt(nativeData.getAddress(), 0x00FF);

        String nativeHex = nativeAccessor.toHex(nativeData);
        System.out.println(nativeHex);
    }

    @Test
    public void testToString() throws CharacterCodingException
    {
        String originalData = "test string value";
        NativeData nativeData = nativeAccessor.valueOf(originalData, StandardCharsets.UTF_8);
        String nativeToString = nativeAccessor.toString(nativeData, StandardCharsets.UTF_8);
        Assert.assertEquals(originalData, nativeToString);
    }

    @Test
    public void testToFloatArray() {
        int valueSize = 42;
        ByteBuffer buffer = ByteBuffer.allocate(valueSize * Float.BYTES);
        FloatBuffer floatBuffer = buffer.asFloatBuffer();
        for (int i = 0; i < valueSize; i++)
            floatBuffer.put((float) i);

        NativeData nativeData = nativeAccessor.valueOf(buffer);
        float[] decodedFloatArray = nativeAccessor.toFloatArray(nativeData, valueSize);

        for (int i = 0; i < valueSize; i++)
            Assert.assertEquals((Float) floatBuffer.get(i), (Float) decodedFloatArray[i]);
        // Float conversion is used to compare values as bit values
    }

    @Test
    public void testDataOutputPlusWrite() throws IOException
    {
        int valueSize = 25;
        NativeData nativeData = nativeAccessor.allocate(valueSize);
        byte[] originalData = new byte[valueSize];
        for (int i = 0; i < valueSize; i++)
            originalData[i] = (byte) i;
        nativeAccessor.putBytes(nativeData, 0, originalData);

        try(DataOutputBuffer dataOutput = new DataOutputBuffer())
        {
            nativeAccessor.write(nativeData, dataOutput);
            byte[] writenData = dataOutput.toByteArray();
            Assert.assertArrayEquals(originalData, writenData);
        }
    }

    @Test
    public void testHeapByteBufferWrite()
    {
        testHeapByteBufferWrite(ByteBuffer.allocate(25), 23);
    }

    @Test
    public void testDirectByteBufferWrite()
    {
        testHeapByteBufferWrite(ByteBuffer.allocateDirect(25), 23);
    }

    private void testHeapByteBufferWrite(ByteBuffer buffer, int valueSize)
    {
        NativeData nativeData = nativeAccessor.allocate(valueSize);
        byte[] originalData = new byte[valueSize];
        for (int i = 0; i < valueSize; i++)
            originalData[i] = (byte) i;
        nativeAccessor.putBytes(nativeData, 0, originalData);

        int initialPosition = buffer.position();
        nativeAccessor.write(nativeData, buffer);
        Assert.assertEquals(valueSize, buffer.position() - initialPosition);
        buffer.flip();
        Assert.assertArrayEquals(originalData, ByteBufferUtil.getArray(buffer));
    }

    @Test
    public void testDigest()
    {
        int valueSize = 25;
        NativeData nativeData = nativeAccessor.allocate(valueSize);
        byte[] originalData = new byte[valueSize];
        for (int i = 0; i < valueSize; i++)
            originalData[i] = (byte) i;
        nativeAccessor.putBytes(nativeData, 0, originalData);

        Digest byteArrayDigest = Digest.forReadResponse();
        byteArrayDigest.update(originalData, 0, originalData.length);

        Digest nativeDigest = Digest.forReadResponse();
        nativeAccessor.digest(nativeData, nativeDigest);

        Assert.assertArrayEquals(byteArrayDigest.digest(), nativeDigest.digest());
    }

    @FunctionalInterface
    interface TriFunction<A,B,C,R>
    {
        R apply(A a, B b, C c);
    }
}
