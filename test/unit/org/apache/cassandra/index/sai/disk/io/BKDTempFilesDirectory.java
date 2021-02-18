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
package org.apache.cassandra.index.sai.disk.io;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import static org.apache.cassandra.index.sai.disk.io.IndexComponents.PER_COLUMN_FILE_NAME_FORMAT;

/**
 * Limited Lucene Directory for use with writing the KD-Tree only
 */
public class BKDTempFilesDirectory extends Directory
{
    private static final Logger logger = LoggerFactory.getLogger(BKDTempFilesDirectory.class);

    private final AtomicLong nextTempFileCounter;
    private final IndexComponents delegate;

    public BKDTempFilesDirectory(IndexComponents delegate, long seed)
    {
        this.delegate = delegate;
        // SequentialWriter#openChannel doesn't fail when we try to create a file that already exist.
        // If tests were running concurrently, it's possible that we could get a tmp file name collision,
        // hence each directory has to have a separate seed.
        this.nextTempFileCounter = new AtomicLong(seed);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
    {
        final String name = prefix + "_" + Long.toString(nextTempFileCounter.getAndIncrement(), Character.MAX_RADIX) + "_" + suffix;
        final File file = delegate.descriptor.tmpFileFor(new Component(Component.Type.CUSTOM,
                                                                       String.format(PER_COLUMN_FILE_NAME_FORMAT,
                                                                                     delegate.indexName,
                                                                                     name)));
        return delegate.createOutput(file);
    }

    @Override
    public IndexInput openInput(String name, IOContext context)
    {
        final File indexInput = getTmpFileByName(name);
        
        try (FileHandle.Builder builder = new FileHandle.Builder(indexInput.getPath()))
        {
            final FileHandle handle = builder.complete();
            final RandomAccessReader reader = handle.createReader();

            return IndexInputReader.create(reader, handle::close);
        }
    }

    @Override
    public long fileLength(String name)
    {
        return getTmpFileByName(name).length();
    }

    @Override
    public void deleteFile(String name)
    {
        final File file = getTmpFileByName(name);
        if (!file.delete())
        {
            logger.warn(delegate.logMessage("Unable to delete file {}"), file.getAbsolutePath());
        }
    }

    @Override
    public void close()
    {
        // noop
    }

    @Override
    public void syncMetaData()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] listAll()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> collection)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock obtainLock(String s)
    {
        throw new UnsupportedOperationException();
    }

    private File getTmpFileByName(String name)
    {
        assert name.endsWith(Descriptor.TMP_EXT);
        final File file = new File(name);
//        final File file = new File(delegate.descriptor.directory, name);
        if (file.exists())
        {
            return file;
        }
        throw new IllegalStateException(delegate.logMessage("unrecognised file: " + name));
    }
}
