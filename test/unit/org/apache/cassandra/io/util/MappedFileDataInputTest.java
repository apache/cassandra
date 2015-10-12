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
package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.junit.Test;

import junit.framework.Assert;

public class MappedFileDataInputTest
{

    @Test
    public void testPositionAndSeek() throws IOException
    {
        MappedFileDataInput bbdi = new MappedFileDataInput((MappedByteBuffer) ByteBuffer.allocateDirect(100), "", 15, 1);
        Assert.assertEquals(99, bbdi.bytesRemaining());
//        Assert.assertEquals(16, bbdi.getPosition());
        Assert.assertEquals(16, bbdi.getFilePointer());
//        Assert.assertTrue(bbdi.markSupported());
        FileMark mark = bbdi.mark();
        bbdi.seek(115);
        Assert.assertEquals(115, bbdi.getFilePointer());
//        Assert.assertEquals(115, bbdi.getPosition());
        Assert.assertEquals(99, bbdi.bytesPastMark(mark));
        Assert.assertTrue(bbdi.isEOF());
        bbdi.seek(15);
        Assert.assertEquals(15, bbdi.getFilePointer());
//        Assert.assertEquals(15, bbdi.getPosition());
        try
        {
            bbdi.seek(14);
            Assert.assertTrue(false);
        }
        catch (IOException t)
        {
        }
        try
        {
            bbdi.seek(116);
            Assert.assertTrue(false);
        }
        catch (IOException t)
        {
        }
    }

}
