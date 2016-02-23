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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.*;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;

/**
 * A single threaded executor that exclusively writes all the hints and otherwise manipulate the writers.
 *
 * Flushing demultiplexes the provided {@link HintsBuffer} and sequentially writes to each {@link HintsWriter},
 * using the same shared write buffer. In the near future, when CASSANDRA-9428 (compression) is implemented,
 * will also share a compression buffer.
 */
final class HintsWriteExecutor
{
    static final int WRITE_BUFFER_SIZE = 256 << 10;

    private final HintsCatalog catalog;
    private final ByteBuffer writeBuffer;
    private final ExecutorService executor;

    HintsWriteExecutor(HintsCatalog catalog)
    {
        this.catalog = catalog;

        writeBuffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
        executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("HintsWriteExecutor", 1);
    }

    /*
     * Should be very fast (worst case scenario - write a few 10s of megabytes to disk).
     */
    void shutdownBlocking()
    {
        executor.shutdown();
        try
        {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Flush the provided buffer, recycle it and offer it back to the pool.
     */
    Future<?> flushBuffer(HintsBuffer buffer, HintsBufferPool bufferPool)
    {
        return executor.submit(new FlushBufferTask(buffer, bufferPool));
    }

    /**
     * Flush the current buffer, but without clearing/recycling it.
     */
    Future<?> flushBufferPool(HintsBufferPool bufferPool)
    {
        return executor.submit(new FlushBufferPoolTask(bufferPool));
    }

    /**
     * Flush the current buffer just for the specified hints stores. Without clearing/recycling it.
     */
    Future<?> flushBufferPool(HintsBufferPool bufferPool, Iterable<HintsStore> stores)
    {
        return executor.submit(new PartiallyFlushBufferPoolTask(bufferPool, stores));
    }

    void fsyncWritersBlockingly(Iterable<HintsStore> stores)
    {
        try
        {
            executor.submit(new FsyncWritersTask(stores)).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    Future<?> closeWriter(HintsStore store)
    {
        return executor.submit(store::closeWriter);
    }

    Future<?> closeAllWriters()
    {
        return executor.submit(() -> catalog.stores().forEach(HintsStore::closeWriter));
    }

    private final class FlushBufferTask implements Runnable
    {
        private final HintsBuffer buffer;
        private final HintsBufferPool bufferPool;

        FlushBufferTask(HintsBuffer buffer, HintsBufferPool bufferPool)
        {
            this.buffer = buffer;
            this.bufferPool = bufferPool;
        }

        public void run()
        {
            buffer.waitForModifications();

            try
            {
                flush(buffer);
            }
            finally
            {
                HintsBuffer recycledBuffer = buffer.recycle();
                bufferPool.offer(recycledBuffer);
            }
        }
    }

    private final class FlushBufferPoolTask implements Runnable
    {
        private final HintsBufferPool bufferPool;

        FlushBufferPoolTask(HintsBufferPool bufferPool)
        {
            this.bufferPool = bufferPool;
        }

        public void run()
        {
            HintsBuffer buffer = bufferPool.currentBuffer();
            buffer.waitForModifications();
            flush(buffer);
        }
    }

    private final class PartiallyFlushBufferPoolTask implements Runnable
    {
        private final HintsBufferPool bufferPool;
        private final Iterable<HintsStore> stores;

        PartiallyFlushBufferPoolTask(HintsBufferPool bufferPool, Iterable<HintsStore> stores)
        {
            this.bufferPool = bufferPool;
            this.stores = stores;
        }

        public void run()
        {
            HintsBuffer buffer = bufferPool.currentBuffer();
            buffer.waitForModifications();
            stores.forEach(store -> flush(buffer.consumingHintsIterator(store.hostId), store));
        }
    }

    private final class FsyncWritersTask implements Runnable
    {
        private final Iterable<HintsStore> stores;

        FsyncWritersTask(Iterable<HintsStore> stores)
        {
            this.stores = stores;
        }

        public void run()
        {
            stores.forEach(HintsStore::fsyncWriter);
            catalog.fsyncDirectory();
        }
    }

    private void flush(HintsBuffer buffer)
    {
        buffer.hostIds().forEach(hostId -> flush(buffer.consumingHintsIterator(hostId), catalog.get(hostId)));
    }

    private void flush(Iterator<ByteBuffer> iterator, HintsStore store)
    {
        while (true)
        {
            if (iterator.hasNext())
                flushInternal(iterator, store);

            if (!iterator.hasNext())
                break;

            // exceeded the size limit for an individual file, but still have more to write
            // close the current writer and continue flushing to a new one in the next iteration
            store.closeWriter();
        }
    }

    @SuppressWarnings("resource")   // writer not closed here
    private void flushInternal(Iterator<ByteBuffer> iterator, HintsStore store)
    {
        long maxHintsFileSize = DatabaseDescriptor.getMaxHintsFileSize();

        HintsWriter writer = store.getOrOpenWriter();

        try (HintsWriter.Session session = writer.newSession(writeBuffer))
        {
            while (iterator.hasNext())
            {
                session.append(iterator.next());
                if (session.position() >= maxHintsFileSize)
                    break;
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, writer.descriptor().fileName());
        }
    }
}
