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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import static org.apache.cassandra.io.util.PageAware.PAGE_SIZE;
import static org.junit.Assert.assertEquals;

public class PageAwareTest
{
    @Test
    public void pageLimit()
    {
        assertEquals(PAGE_SIZE, PageAware.pageLimit(0));
        assertEquals(2 * PAGE_SIZE, PageAware.pageLimit(PAGE_SIZE));
        assertEquals(3 * PAGE_SIZE, PageAware.pageLimit(2 * PAGE_SIZE));

        assertEquals(PAGE_SIZE, PageAware.pageLimit(PAGE_SIZE / 3));
        assertEquals(PAGE_SIZE, PageAware.pageLimit(PAGE_SIZE - 1));
        assertEquals(PAGE_SIZE, PageAware.pageLimit(1));

        assertEquals(2 * PAGE_SIZE, PageAware.pageLimit(PAGE_SIZE + PAGE_SIZE / 3));
        assertEquals(2 * PAGE_SIZE, PageAware.pageLimit(PAGE_SIZE + PAGE_SIZE - 1));
        assertEquals(2 * PAGE_SIZE, PageAware.pageLimit(PAGE_SIZE + 1));
    }

    @Test
    public void bytesLeftInPage()
    {
        assertEquals(PAGE_SIZE, PageAware.bytesLeftInPage(0));
        assertEquals(PAGE_SIZE, PageAware.bytesLeftInPage(PAGE_SIZE));
        assertEquals(PAGE_SIZE, PageAware.bytesLeftInPage(2 * PAGE_SIZE));

        assertEquals(PAGE_SIZE - (PAGE_SIZE / 3), PageAware.bytesLeftInPage(PAGE_SIZE / 3));
        assertEquals(1, PageAware.bytesLeftInPage(PAGE_SIZE - 1));
        assertEquals(PAGE_SIZE - 1, PageAware.bytesLeftInPage(1));

        assertEquals(PAGE_SIZE - (PAGE_SIZE / 3), PageAware.bytesLeftInPage(PAGE_SIZE + PAGE_SIZE / 3));
        assertEquals(1, PageAware.bytesLeftInPage(PAGE_SIZE + PAGE_SIZE - 1));
        assertEquals(PAGE_SIZE - 1, PageAware.bytesLeftInPage(PAGE_SIZE + 1));
    }

    @Test
    public void pageStart()
    {
        assertEquals(0, PageAware.pageStart(0));
        assertEquals(PAGE_SIZE, PageAware.pageStart(PAGE_SIZE));
        assertEquals(2 * PAGE_SIZE, PageAware.pageStart(2 * PAGE_SIZE));

        assertEquals(0, PageAware.pageStart(PAGE_SIZE / 3));
        assertEquals(0, PageAware.pageStart(PAGE_SIZE - 1));
        assertEquals(0, PageAware.pageStart(1));

        assertEquals(PAGE_SIZE, PageAware.pageStart(PAGE_SIZE + PAGE_SIZE / 3));
        assertEquals(PAGE_SIZE, PageAware.pageStart(PAGE_SIZE + PAGE_SIZE - 1));
        assertEquals(PAGE_SIZE, PageAware.pageStart(PAGE_SIZE + 1));
    }

    @Test
    public void padded()
    {
        assertEquals(0, PageAware.padded(0));
        assertEquals(PAGE_SIZE, PageAware.padded(PAGE_SIZE));
        assertEquals(2 * PAGE_SIZE, PageAware.padded(2 * PAGE_SIZE));

        assertEquals(PAGE_SIZE, PageAware.padded(PAGE_SIZE / 3));
        assertEquals(PAGE_SIZE, PageAware.padded(PAGE_SIZE - 1));
        assertEquals(PAGE_SIZE, PageAware.padded(1));

        assertEquals(2 * PAGE_SIZE, PageAware.padded(PAGE_SIZE + PAGE_SIZE / 3));
        assertEquals(2 * PAGE_SIZE, PageAware.padded(PAGE_SIZE + PAGE_SIZE - 1));
        assertEquals(2 * PAGE_SIZE, PageAware.padded(PAGE_SIZE + 1));
    }

    @Test
    public void numPages()
    {
        assertEquals(0, PageAware.numPages(0));
        assertEquals(1, PageAware.numPages(PAGE_SIZE));
        assertEquals(2, PageAware.numPages(2 * PAGE_SIZE));

        assertEquals(1, PageAware.numPages(PAGE_SIZE / 3));
        assertEquals(1, PageAware.numPages(PAGE_SIZE - 1));
        assertEquals(1, PageAware.numPages(1));

        assertEquals(2, PageAware.numPages(PAGE_SIZE + PAGE_SIZE / 3));
        assertEquals(2, PageAware.numPages(PAGE_SIZE + PAGE_SIZE - 1));
        assertEquals(2, PageAware.numPages(PAGE_SIZE + 1));
    }

    @Test
    public void pageNum()
    {
        assertEquals(0, PageAware.pageNum(0));
        assertEquals(1, PageAware.pageNum(PAGE_SIZE));
        assertEquals(2, PageAware.pageNum(2 * PAGE_SIZE));

        assertEquals(0, PageAware.pageNum(PAGE_SIZE / 3));
        assertEquals(0, PageAware.pageNum(PAGE_SIZE - 1));
        assertEquals(0, PageAware.pageNum(1));

        assertEquals(1, PageAware.pageNum(PAGE_SIZE + PAGE_SIZE / 3));
        assertEquals(1, PageAware.pageNum(PAGE_SIZE + PAGE_SIZE - 1));
        assertEquals(1, PageAware.pageNum(PAGE_SIZE + 1));
    }

    @Test
    public void pad() throws IOException
    {
        testPad(0, 0);
        testPad(PAGE_SIZE, PAGE_SIZE);
        testPad(2 * PAGE_SIZE, 2 * PAGE_SIZE);

        testPad(PAGE_SIZE, PAGE_SIZE / 3);
        testPad(PAGE_SIZE, PAGE_SIZE - 1);
        testPad(PAGE_SIZE, 1);

        testPad(2 * PAGE_SIZE, PAGE_SIZE + PAGE_SIZE / 3);
        testPad(2 * PAGE_SIZE, PAGE_SIZE + PAGE_SIZE - 1);
        testPad(2 * PAGE_SIZE, PAGE_SIZE + 1);
    }

    private void testPad(int expectedSize, int currentSize) throws IOException
    {
        ByteBuffer expectedBuf = ByteBuffer.allocate(expectedSize);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            for (int i = 0; i < currentSize; i++)
            {
                expectedBuf.put((byte) 1);
                out.write(1);
            }
            for (int i = currentSize; i < expectedSize; i++)
            {
                expectedBuf.put((byte) 0);
            }

            PageAware.pad(out);
            out.flush();

            assertEquals(expectedBuf.rewind(), out.asNewBuffer());
        }
    }

    @Test
    public void randomizedTest()
    {
        Random rand = new Random();
        for (int i = 0; i < 100000; ++i)
        {
            long pos = rand.nextLong() & ((1L << rand.nextInt(64)) - 1);    // positive long with random length
            long pageStart = (pos / PAGE_SIZE) * PAGE_SIZE;
            long pageLimit = pageStart + PAGE_SIZE;
            assertEquals(pageLimit, PageAware.pageLimit(pos));
            assertEquals(pageStart, PageAware.pageStart(pos));
            assertEquals(pageLimit - pos, PageAware.bytesLeftInPage(pos));
            assertEquals(pos == pageStart ? pageStart : pageLimit, PageAware.padded(pos));
        }
    }
}