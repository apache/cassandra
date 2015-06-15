/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.vint;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.junit.Test;

import junit.framework.Assert;

public class VIntCodingTest
{

    @Test
    public void testComputeSize() throws Exception
    {
        assertEncodedAtExpectedSize(0L, 1);

        for (int size = 1 ; size < 8 ; size++)
        {
            assertEncodedAtExpectedSize((1L << 7 * size) - 1, size);
            assertEncodedAtExpectedSize(1L << 7 * size, size + 1);
        }
        Assert.assertEquals(9, VIntCoding.computeUnsignedVIntSize(Long.MAX_VALUE));
    }

    private void assertEncodedAtExpectedSize(long value, int expectedSize) throws Exception
    {
        Assert.assertEquals(expectedSize, VIntCoding.computeUnsignedVIntSize(value));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        VIntCoding.writeUnsignedVInt(value, dos);
        dos.flush();
        Assert.assertEquals( expectedSize, baos.toByteArray().length);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt(value);
        Assert.assertEquals( expectedSize, dob.buffer().remaining());
        dob.close();
    }

    @Test
    public void testReadExtraBytesCount()
    {
        for (int i = 1 ; i < 8 ; i++)
            Assert.assertEquals(i, VIntCoding.numberOfExtraBytesToRead((byte) ((0xFF << (8 - i)) & 0xFF)));
    }

    /*
     * Quick sanity check that 1 byte encodes up to 127 as expected
     */
    @Test
    public void testOneByteCapacity() throws Exception {
        int biggestOneByte = 127;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        VIntCoding.writeUnsignedVInt(biggestOneByte, dos);
        dos.flush();
        Assert.assertEquals( 1, baos.toByteArray().length);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt(biggestOneByte);
        Assert.assertEquals( 1, dob.buffer().remaining());
        dob.close();
    }
}
