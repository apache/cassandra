package org.apache.cassandra.io.sstable;
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


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.JMXInstrumentedCache;
import org.apache.cassandra.io.ICompactionInfo;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CacheWriter<K, V> implements ICompactionInfo
{
    private static final Logger logger = LoggerFactory.getLogger(CacheWriter.class);

    private final File path;
    private final Function<K, ByteBuffer> converter;
    private final Set<K> keys;
    private final String columnFamily;
    private final long estimatedTotalBytes;
    private long bytesWritten;

    public CacheWriter(String columnFamily, JMXInstrumentedCache<K, V> cache, File path, Function<K, ByteBuffer> converter)
    {
        this.columnFamily = columnFamily;
        this.path = path;
        this.converter = converter;
        keys = cache.getKeySet();

        long bytes = 0;
        for (K key : keys)
            bytes += converter.apply(key).remaining();

        // an approximation -- the keyset can change while saving
        estimatedTotalBytes = bytes;
    }

    public void saveCache() throws IOException
    {
        long start = System.currentTimeMillis();

        if (keys.size() == 0 || estimatedTotalBytes == 0)
        {
            logger.debug("Deleting {} (cache is empty)");
            path.delete();
            return;
        }

        logger.debug("Saving {}", path);
        File tmpFile = File.createTempFile(path.getName(), null, path.getParentFile());

        BufferedRandomAccessFile out = new BufferedRandomAccessFile(tmpFile, "rw", BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE, true);
        try
        {
            for (K key : keys)
            {
                ByteBuffer bytes = converter.apply(key);
                ByteBufferUtil.writeWithLength(bytes, out);
                bytesWritten += bytes.remaining();
            }
        }
        finally
        {
            out.close();
        }
        if (!tmpFile.renameTo(path))
            throw new IOException("Unable to rename cache to " + path);
        logger.info(String.format("Saved %s (%d items) in %d ms",
                                  path.getName(), keys.size(), (System.currentTimeMillis() - start)));
    }

    public long getTotalBytes()
    {
        // keyset can change in size, thus totalBytes can too
        return Math.max(estimatedTotalBytes, getBytesComplete());
    }

    public long getBytesComplete()
    {
        return bytesWritten;
    }

    public String getTaskType()
    {
        return "Save " + path.getName();
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }
}
