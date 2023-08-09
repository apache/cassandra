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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertNotNull;

public class TrackingIndexFileUtils extends IndexFileUtils
{
    private final Map<TrackedForwardingIndexInput, String> openInputs = Collections.synchronizedMap(new HashMap<>());

    public TrackingIndexFileUtils(SequentialWriterOption writerOption)
    {
        super(writerOption);
    }

    @Override
    public IndexInput openInput(FileHandle handle)
    {
        TrackedForwardingIndexInput input = new TrackedForwardingIndexInput(super.openInput(handle));
        openInputs.put(input, Throwables.getStackTraceAsString(new RuntimeException("Input created")));
        return input;
    }

    public Map<IndexInput, String> getOpenInputs()
    {
        return new HashMap<>(openInputs);
    }

    public class TrackedForwardingIndexInput extends IndexInput
    {
        private final IndexInput delegate;

        protected TrackedForwardingIndexInput(IndexInput delegate)
        {
            super(delegate.toString());
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException
        {
            delegate.close();
            final String creationStackTrace = openInputs.remove(this);
            assertNotNull("Closed unregistered input: " + this, creationStackTrace);
        }

        @Override
        public long getFilePointer()
        {
            return delegate.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException
        {
            delegate.seek(pos);
        }

        @Override
        public long length()
        {
            return delegate.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException
        {
            return delegate.slice(sliceDescription, offset, length);
        }

        @Override
        public byte readByte() throws IOException
        {
            return delegate.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException
        {
            delegate.readBytes(b, offset, len);
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }
    }
}
