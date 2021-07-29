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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;

import org.junit.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;

public class ChecksummedSequentialWriterTest extends SequentialWriterTest
{

    private final List<TestableCSW> writers = new ArrayList<>();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void cleanup()
    {
        for (TestableSW sw : writers)
            sw.file.tryDelete();
        writers.clear();
    }

    protected TestableTransaction newTest() throws IOException
    {
        TestableCSW sw = new TestableCSW();
        writers.add(sw);
        return sw;
    }

    private static class TestableCSW extends TestableSW
    {
        final File crcFile;

        private TestableCSW() throws IOException
        {
            this(tempFile("compressedsequentialwriter"),
                 tempFile("compressedsequentialwriter.checksum"));
        }

        private TestableCSW(File file, File crcFile) throws IOException
        {
            this(file, crcFile, new ChecksummedSequentialWriter(file, crcFile, null, SequentialWriterOption.newBuilder()
                                                                                                           .bufferSize(BUFFER_SIZE)
                                                                                                           .build()));
        }

        private TestableCSW(File file, File crcFile, SequentialWriter sw) throws IOException
        {
            super(file, sw);
            this.crcFile = crcFile;
        }

        protected void assertInProgress() throws Exception
        {
            super.assertInProgress();
            Assert.assertTrue(crcFile.exists());
            Assert.assertEquals(0, crcFile.length());
        }

        protected void assertPrepared() throws Exception
        {
            super.assertPrepared();
            Assert.assertTrue(crcFile.exists());
            Assert.assertFalse(0 == crcFile.length());
        }

        protected void assertAborted() throws Exception
        {
            super.assertAborted();
        }
    }

}
