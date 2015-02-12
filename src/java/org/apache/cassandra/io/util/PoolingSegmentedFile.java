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

import java.io.File;

import org.apache.cassandra.service.FileCacheService;

public abstract class PoolingSegmentedFile extends SegmentedFile
{
    final FileCacheService.CacheKey cacheKey;
    protected PoolingSegmentedFile(Cleanup cleanup, String path, long length)
    {
        this(cleanup, path, length, length);
    }

    protected PoolingSegmentedFile(Cleanup cleanup, String path, long length, long onDiskLength)
    {
        super(cleanup, path, length, onDiskLength);
        cacheKey = cleanup.cacheKey;
    }

    public PoolingSegmentedFile(PoolingSegmentedFile copy)
    {
        super(copy);
        cacheKey = copy.cacheKey;
    }

    protected static class Cleanup extends SegmentedFile.Cleanup
    {
        final FileCacheService.CacheKey cacheKey = new FileCacheService.CacheKey();
        protected Cleanup(String path)
        {
            super(path);
        }
        public void tidy() throws Exception
        {
            FileCacheService.instance.invalidate(cacheKey, path);
        }
    }

    public FileDataInput getSegment(long position)
    {
        RandomAccessReader reader = FileCacheService.instance.get(cacheKey);

        if (reader == null)
            reader = createPooledReader();

        reader.seek(position);
        return reader;
    }

    protected RandomAccessReader createPooledReader()
    {
        return RandomAccessReader.open(new File(path), length, this);
    }

    public void recycle(RandomAccessReader reader)
    {
        FileCacheService.instance.put(cacheKey, reader);
    }
}
