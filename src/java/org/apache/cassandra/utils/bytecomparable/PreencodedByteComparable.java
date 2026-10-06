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

package org.apache.cassandra.utils.bytecomparable;

import java.nio.ByteBuffer;

abstract class PreencodedByteComparable implements ByteComparable.Preencoded
{
    private final Version version;

    PreencodedByteComparable(Version version)
    {
        this.version = version;
    }

    @Override
    public Version encodingVersion()
    {
        return version;
    }

    static class Array extends PreencodedByteComparable
    {
        private final byte[] bytes;
        private final int offset;
        private final int length;

        Array(Version version, byte[] bytes)
        {
            this(version, bytes, 0, bytes.length);
        }

        Array(Version version, byte[] bytes, int offset, int length)
        {
            super(version);
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public ByteSource.Duplicatable getPreencodedBytes()
        {
            return ByteSource.preencoded(bytes, offset, length);
        }
    }

    static class Buffer extends PreencodedByteComparable
    {
        private final ByteBuffer bytes;

        Buffer(Version version, ByteBuffer bytes)
        {
            super(version);
            this.bytes = bytes;
        }

        @Override
        public ByteSource.Duplicatable getPreencodedBytes()
        {
            return ByteSource.preencoded(bytes);
        }
    }
}
