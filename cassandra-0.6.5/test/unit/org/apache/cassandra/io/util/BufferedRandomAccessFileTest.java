package org.apache.cassandra.io.util;
/*
 * 
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
 * 
 */


import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class BufferedRandomAccessFileTest
{

    @Test
    public void testLength() throws IOException
    {
        File tmpFile = File.createTempFile("lengthtest", "bin");
        BufferedRandomAccessFile rw = new BufferedRandomAccessFile(tmpFile, "rw");
        assertEquals(0, rw.length());

        // write a chunk smaller then our buffer, so will not be flushed
        // to disk
        byte[] lessThenBuffer = new byte[BufferedRandomAccessFile.BuffSz_ / 2];
        rw.write(lessThenBuffer);
        assertEquals(lessThenBuffer.length, rw.length());

        // sync the data and check length
        rw.sync();
        assertEquals(lessThenBuffer.length, rw.length());

        // write more then the buffer can hold and check length
        byte[] biggerThenBuffer = new byte[BufferedRandomAccessFile.BuffSz_ * 2];
        rw.write(biggerThenBuffer);
        assertEquals(biggerThenBuffer.length + lessThenBuffer.length, rw.length());

        // checking that reading doesn't interfere
        rw.seek(0);
        rw.read();
        assertEquals(biggerThenBuffer.length + lessThenBuffer.length, rw.length());

        rw.close();

        // will use cachedlength
        BufferedRandomAccessFile r = new BufferedRandomAccessFile(tmpFile, "r");
        assertEquals(lessThenBuffer.length + biggerThenBuffer.length, r.length());
        r.close();
    }

}
