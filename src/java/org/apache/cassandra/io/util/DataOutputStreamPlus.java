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
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * When possible use {@link DataOutputStreamAndChannel} instead of this class, as it will
 * be more efficient. This class is only for situations where it cannot be used
 */
public class DataOutputStreamPlus extends AbstractDataOutput implements DataOutputPlus
{
    protected final OutputStream out;
    public DataOutputStreamPlus(OutputStream out)
    {
        this.out = out;
    }

    public void write(byte[] buffer, int offset, int count) throws IOException
    {
        out.write(buffer, offset, count);
    }

    public void write(int oneByte) throws IOException
    {
        out.write(oneByte);
    }

    public void close() throws IOException
    {
        out.close();
    }

    public void flush() throws IOException
    {
        out.flush();
    }
}
