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

package org.apache.cassandra.net.async;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.cassandra.io.util.DataInputPlus;

import java.io.IOException;

public class ByteBufDataInputPlus extends ByteBufInputStream implements DataInputPlus
{
    /**
     * The parent class does not expose the buffer to derived classes, so we need
     * to stash a reference here so it can be exposed via {@link #buffer()}.
     */
    private final ByteBuf buf;

    public ByteBufDataInputPlus(ByteBuf buffer)
    {
        super(buffer);
        this.buf = buffer;
    }

    public ByteBuf buffer()
    {
        return buf;
    }

    @Override
    public String readUTF() throws IOException
    {
        return DataInputStreamPlus.readUTF(this);
    }
}
