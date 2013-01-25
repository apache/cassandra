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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class PoolingSegmentedFile extends SegmentedFile
{
    public final Queue<RandomAccessReader> pool = new ConcurrentLinkedQueue<RandomAccessReader>();

    protected PoolingSegmentedFile(String path, long length)
    {
        super(path, length);
    }

    protected PoolingSegmentedFile(String path, long length, long onDiskLength)
    {
        super(path, length, onDiskLength);
    }

    public FileDataInput getSegment(long position)
    {
        RandomAccessReader reader = pool.poll();
        if (reader == null)
            reader = createReader(path);
        reader.seek(position);
        return reader;
    }

    protected abstract RandomAccessReader createReader(String path);

    public void recycle(RandomAccessReader reader)
    {
        pool.add(reader);
    }

    public void cleanup()
    {
        for (RandomAccessReader reader : pool)
            reader.deallocate();
    }
}
