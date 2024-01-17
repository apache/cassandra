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

package org.apache.cassandra.db.commitlog;


import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.File;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MemoryMappedSegmentTest
{
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void shouldNotSkipFileAdviseToFreeSystemCache()
    {
        //given
        MemoryMappedSegment.skipFileAdviseToFreePageCache = false;
        MemoryMappedSegment memoryMappedSegment = memoryMappedSegment();
        int startMarker = 0;
        int nextMarker = 1024;

        //when
        memoryMappedSegment.flush(startMarker, nextMarker);

        //then
        verify(memoryMappedSegment)
                .adviceOnFileToFreePageCache(eq(memoryMappedSegment.fd), eq(startMarker), eq(nextMarker), eq(memoryMappedSegment.logFile));
    }

    @Test
    public void shouldSkipFileAdviseToFreeSystemCache()
    {
        //given
        MemoryMappedSegment.skipFileAdviseToFreePageCache = true;
        MemoryMappedSegment memoryMappedSegment = memoryMappedSegment();

        //when
        memoryMappedSegment.flush(0, 1024);

        //then
        verify(memoryMappedSegment, never())
                .adviceOnFileToFreePageCache(anyInt(), anyInt(), anyInt(), any(File.class));
    }

    private MemoryMappedSegment memoryMappedSegment()
    {
        return Mockito.spy(new MemoryMappedSegment(CommitLog.instance, CommitLog.instance.getSegmentManager()));
    }
}
