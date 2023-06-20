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
import java.io.OutputStream;

import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.Owning;

/**
 * When possible use {@link WrappedDataOutputStreamPlus} instead of this class, as it will
 * be more efficient when using Plus methods. This class is only for situations where it cannot be used.
 *
 * The channel provided by this class is just a wrapper around the output stream.
 */
public class WrappedDataOutputStreamPlus extends UnbufferedDataOutputStreamPlus
{
    protected final @Owning OutputStream out;

    public WrappedDataOutputStreamPlus(@Owning OutputStream out)
    {
        super();
        this.out = out;
    }

    @Override
    public void write(byte[] buffer, int offset, int count) throws IOException
    {
        out.write(buffer, offset, count);
    }

    @Override
    public void write(int oneByte) throws IOException
    {
        out.write(oneByte);
    }

    @EnsuresCalledMethods(value = {"this.channel", "this.out"}, methods = "close")
    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(channel);
        out.close();
    }

    @Override
    public void flush() throws IOException
    {
        out.flush();
    }
}
