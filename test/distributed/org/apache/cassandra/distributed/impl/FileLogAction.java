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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Predicate;

import com.google.common.io.Closeables;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
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
        RandomAccessReader reader;
        reader = RandomAccessReader.open(file);
        if (startPosition > 0) // -1 used to disable, so ignore any negative values or 0 (default offset)
        {
            reader.seek(startPosition);
        }
        return new FileLineIterator(reader, fn);
    }

    private static final class FileLineIterator extends AbstractIterator<String> implements LineIterator
    {
        private final RandomAccessReader reader;
        private final Predicate<String> fn;

        private FileLineIterator(RandomAccessReader reader, Predicate<String> fn)
        {
            this.reader = reader;
            this.fn = fn;
        }

        @Override
        public long mark()
        {
            return reader.getFilePointer();
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
