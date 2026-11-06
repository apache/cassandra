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

package org.apache.cassandra.io.sstable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.Iterables;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

@RunWith(BMUnitRunner.class)
@BMRule(name = "Record page cache drops",
        targetClass = "NativeLibrary",
        targetMethod = "trySkipCache(int, long, long, String)",
        targetLocation = "AT ENTRY",
        action = "org.apache.cassandra.io.sstable.SSTableReaderSkipFileCacheTest.recordDrop($4, $2, $3)")
public class SSTableReaderSkipFileCacheTest extends CQLTester
{
    private static final List<List<Long>> drops = new CopyOnWriteArrayList<>();
    private static volatile String trackedPath;

    public static void recordDrop(String path, long offset, long len)
    {
        if (path.equals(trackedPath))
            drops.add(List.of(offset, len));
    }

    @Test
    public void testTrySkipFileCacheBefore() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        for (int i = 0; i < 1000; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "x".repeat(100));
        flush();

        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        trackedPath = sstable.descriptor.fileFor(Components.DATA).absolutePath();

        drops.clear();
        sstable.trySkipFileCacheBefore(sstable.getFirst());
        assertEquals(List.of(), drops);

        long position = sstable.getPosition(sstable.getLast(), SSTableReader.Operator.EQ);
        drops.clear();
        sstable.trySkipFileCacheBefore(sstable.getLast());
        assertEquals(List.of(List.of(0L, sstable.getCompressionMetadata().chunkFor(position).offset)), drops);

        DecoratedKey pastEnd = new BufferDecoratedKey(sstable.getLast().getToken().nextValidToken(), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        drops.clear();
        sstable.trySkipFileCacheBefore(pastEnd);
        assertEquals(List.of(List.of(0L, sstable.getCompressionMetadata().compressedFileLength)), drops);
    }
}
