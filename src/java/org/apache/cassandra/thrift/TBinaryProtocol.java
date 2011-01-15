/**
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
package org.apache.cassandra.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;

/**
 * TODO
 * This was added to support writeBinary on direct buffers for CASSANDRA-1714;
 * we can remove it once we upgrade to Thrift 0.7, which incorporates the patch (THRIFT-883).
 */

public class TBinaryProtocol extends org.apache.thrift.protocol.TBinaryProtocol
{

    public TBinaryProtocol(TTransport trans)
    {
        this(trans, false, true);
    }

    public TBinaryProtocol(TTransport trans, boolean strictRead, boolean strictWrite)
    {
        super(trans);
        strictRead_ = strictRead;
        strictWrite_ = strictWrite;
    }

    public static class Factory extends org.apache.thrift.protocol.TBinaryProtocol.Factory
    {
        public Factory()
        {
            super(false, true);
        }

        public Factory(boolean strictRead, boolean strictWrite)
        {
            super(strictRead, strictWrite, 0);
        }

        public Factory(boolean strictRead, boolean strictWrite, int readLength)
        {
            super(strictRead, strictWrite, readLength);
        }

        public TProtocol getProtocol(TTransport trans)
        {
            TBinaryProtocol protocol = new TBinaryProtocol(trans, strictRead_, strictWrite_);

            if (readLength_ != 0)
            {
                protocol.setReadLength(readLength_);
            }

            return protocol;
        }
    }

    @Override
    public void writeBinary(ByteBuffer buffer) throws TException
    {
        writeI32(buffer.remaining());

        if (buffer.hasArray())
        {
            trans_.write(buffer.array(), buffer.position() + buffer.arrayOffset(), buffer.remaining());
        }
        else
        {
            byte[] bytes = new byte[buffer.remaining()];

            int j = 0;
            for (int i = buffer.position(); i < buffer.limit(); i++)
            {
                bytes[j++] = buffer.get(i);
            }

            trans_.write(bytes);
        }
    }
}