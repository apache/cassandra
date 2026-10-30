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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.io.IOException;

import org.apache.cassandra.io.util.SequentialWriter;

import io.github.jbellis.jvector.disk.IndexWriter;

/**
 * Exposes a {@link SequentialWriter} as a jvector {@link IndexWriter}. Closing the adapter does not close the
 * underlying writer, which remains owned by the caller.
 */
public class SequentialWriterAdapter implements IndexWriter
{
    private final SequentialWriter writer;

    public SequentialWriterAdapter(SequentialWriter writer)
    {
        this.writer = writer;
    }

    @Override
    public long position()
    {
        return writer.position();
    }

    @Override
    public void close()
    {
    }

    @Override
    public void write(int b) throws IOException
    {
        writer.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        writer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        writer.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        writer.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        writer.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        writer.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        writer.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        writer.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        writer.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        writer.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        writer.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        writer.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        writer.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        writer.writeUTF(s);
    }
}
