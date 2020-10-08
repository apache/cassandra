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

package org.apache.cassandra.distributed.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Predicate;

import com.google.common.io.Closeables;

import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.api.LineIterator;

public class FileLogAction implements LogAction
{
    private final File file;

    public FileLogAction(File file)
    {
        this.file = Objects.requireNonNull(file);
    }

    @Override
    public long mark()
    {
        return file.length();
    }

    @Override
    public LineIterator match(long startPosition, Predicate<String> fn)
    {
        RandomAccessFile reader;
        try
        {
            reader = new RandomAccessFile(file, "r");
        }
        catch (FileNotFoundException e)
        {
            // if file isn't present, don't return an empty stream as it looks the same as no log lines matched
            throw new UncheckedIOException(e);
        }
        if (startPosition > 0) // -1 used to disable, so ignore any negative values or 0 (default offset)
        {
            try
            {
                reader.seek(startPosition);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Unable to seek to " + startPosition, e);
            }
        }
        return new FileLineIterator(reader, fn);
    }

    private static final class FileLineIterator extends AbstractIterator<String> implements LineIterator
    {
        private final RandomAccessFile reader;
        private final Predicate<String> fn;

        private FileLineIterator(RandomAccessFile reader, Predicate<String> fn)
        {
            this.reader = reader;
            this.fn = fn;
        }

        @Override
        public long mark()
        {
            try
            {
                return reader.getFilePointer();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected String computeNext()
        {
            try
            {
                String s;
                while ((s = reader.readLine()) != null)
                {
                    if (fn.test(s))
                        return s;
                }
                return endOfData();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close()
        {
            try
            {
                Closeables.close(reader, true);
            }
            catch (IOException impossible)
            {
                throw new AssertionError(impossible);
            }
        }
    }
}
