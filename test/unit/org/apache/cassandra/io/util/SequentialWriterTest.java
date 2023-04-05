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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.concurrent.AbstractTransactionalTest;

import static org.apache.commons.io.FileUtils.*;
import static org.junit.Assert.assertEquals;

public class SequentialWriterTest extends AbstractTransactionalTest
{

    private final List<TestableSW> writers = new ArrayList<>();

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
        TestableSW sw = new TestableSW();
        writers.add(sw);
        return sw;
    }

    protected static class TestableSW extends TestableTransaction
    {
        protected static final int BUFFER_SIZE = 8 << 10;
        protected final File file;
        protected final SequentialWriter writer;
        protected final byte[] fullContents, partialContents;

        protected TestableSW() throws IOException
        {
            this(tempFile("sequentialwriter"));
        }

        protected TestableSW(File file) throws IOException
        {
            this(file, new SequentialWriter(file, SequentialWriterOption.newBuilder()
                                                                        .bufferSize(8 << 10)
                                                                        .bufferType(BufferType.OFF_HEAP)
                                                                        .build()));
        }

        protected TestableSW(File file, SequentialWriter sw) throws IOException
        {
            super(sw);
            this.file = file;
            this.writer = sw;
            fullContents = new byte[BUFFER_SIZE + BUFFER_SIZE / 2];
            ThreadLocalRandom.current().nextBytes(fullContents);
            partialContents = Arrays.copyOf(fullContents, BUFFER_SIZE);
            sw.write(fullContents);
        }

        protected void assertInProgress() throws Exception
        {
            Assert.assertTrue(file.exists());
            byte[] bytes = readFileToByteArray(file.toJavaIOFile());
            Assert.assertTrue(Arrays.equals(partialContents, bytes));
        }

        protected void assertPrepared() throws Exception
        {
            Assert.assertTrue(file.exists());
            byte[] bytes = readFileToByteArray(file.toJavaIOFile());
            Assert.assertTrue(Arrays.equals(fullContents, bytes));
        }

        protected void assertAborted() throws Exception
        {
            Assert.assertFalse(writer.isOpen());
        }

        protected void assertCommitted() throws Exception
        {
            assertPrepared();
            Assert.assertFalse(writer.isOpen());
        }

        protected static File tempFile(String prefix)
        {
            File file = FileUtils.createTempFile(prefix, "test");
            file.tryDelete();
            return file;
        }
    }

    @Test
    public void resetAndTruncateTest()
    {
        File tempFile = new File(Files.createTempDir().toPath(), "reset.txt");
        final int bufferSize = 48;
        final int writeSize = 64;
        byte[] toWrite = new byte[writeSize];
        SequentialWriterOption option = SequentialWriterOption.newBuilder().bufferSize(bufferSize).build();
        try (SequentialWriter writer = new SequentialWriter(tempFile, option))
        {
            // write bytes greather than buffer
            writer.write(toWrite);
            assertEquals(bufferSize, writer.getLastFlushOffset());
            assertEquals(writeSize, writer.position());
            // mark thi position
            DataPosition pos = writer.mark();
            // write another
            writer.write(toWrite);
            // another buffer should be flushed
            assertEquals(bufferSize * 2, writer.getLastFlushOffset());
            assertEquals(writeSize * 2, writer.position());
            // reset writer
            writer.resetAndTruncate(pos);
            // current position and flushed size should be changed
            assertEquals(writeSize, writer.position());
            assertEquals(writeSize, writer.getLastFlushOffset());
            // write another byte less than buffer
            writer.write(new byte[]{0});
            assertEquals(writeSize + 1, writer.position());
            // flush off set should not be increase
            assertEquals(writeSize, writer.getLastFlushOffset());
            writer.finish();
        }
        catch (IOException e)
        {
            Assert.fail();
        }
        // final file size check
        assertEquals(writeSize + 1, tempFile.length());
    }

    /**
     * Tests that the output stream exposed by SequentialWriter behaves as expected
     */
    @Test
    public void outputStream()
    {
        File tempFile = new File(Files.createTempDir().toPath(), "test.txt");
        Assert.assertFalse("temp file shouldn't exist yet", tempFile.exists());

        SequentialWriterOption option = SequentialWriterOption.newBuilder().finishOnClose(true).build();
        try (DataOutputStream os = new DataOutputStream(new SequentialWriter(tempFile, option)))
        {
            os.writeUTF("123");
        }
        catch (IOException e)
        {
            Assert.fail();
        }

        Assert.assertTrue("temp file should exist", tempFile.exists());
    }

}
