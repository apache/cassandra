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
package org.apache.cassandra.index.sasi.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class LongIteratorTest
{
    @Test
    public void testEmptyIterator() throws IOException {
        LongIterator it = new LongIterator(new long[] { });

        Assert.assertEquals(0, it.getCount());
        Assert.assertEquals(null, it.getCurrent());
        Assert.assertEquals(null, it.getMaximum());
        Assert.assertEquals(null, it.getMinimum());
        Assert.assertFalse(it.hasNext());

        it.close();
    }

    @Test
    public void testBasicITerator() throws IOException {
        LongIterator it = new LongIterator(new long[] { 2L, 3L, 5L, 6L });

        Assert.assertEquals(4L, (long) it.getCount());
        Assert.assertEquals(2L, (long) it.getCurrent());
        Assert.assertEquals(6L, (long) it.getMaximum());
        Assert.assertEquals(2L, (long) it.getMinimum());

        Assert.assertEquals(2L, (long) it.next().get());
        Assert.assertEquals(3L, (long) it.next().get());

        it.close();
    }
}
