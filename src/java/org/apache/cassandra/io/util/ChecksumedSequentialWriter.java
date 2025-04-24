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

import java.io.IOException;
import java.util.function.Supplier;
import java.util.zip.Checksum;

import org.apache.cassandra.utils.concurrent.Transactional;

public class ChecksumedSequentialWriter extends ChecksumedDataOutputPlus implements Transactional
{
    private static final SequentialWriterOption DEFAULT_WRITER_OPTIONS = SequentialWriterOption.newBuilder().finishOnClose(true).build();

    public ChecksumedSequentialWriter(SequentialWriter delegate, Checksum checksum)
    {
        super(delegate, checksum);
    }

    public ChecksumedSequentialWriter(SequentialWriter delegate, Supplier<Checksum> fn)
    {
        super(delegate, fn);
    }

    public static ChecksumedSequentialWriter open(File file, boolean append, Supplier<Checksum> fn) throws IOException
    {
        SequentialWriter writer = new SequentialWriter(file, DEFAULT_WRITER_OPTIONS);
        if (append)
            writer.skipBytes(file.length());
        return new ChecksumedSequentialWriter(writer, fn);
    }

    @Override
    public SequentialWriter delegate()
    {
        return (SequentialWriter) super.delegate();
    }

    public long getFilePointer()
    {
        return delegate().position();
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        return delegate().commit(accumulate);
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        return delegate().abort(accumulate);
    }

    @Override
    public void prepareToCommit()
    {
        delegate().prepareToCommit();
    }

    @Override
    public void close()
    {
        delegate().close();
    }
}
