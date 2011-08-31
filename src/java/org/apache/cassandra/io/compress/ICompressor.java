/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.util.Map;

public interface ICompressor
{
    public int initialCompressedBufferLength(int chunkLength);

    public int compress(byte[] input, int inputOffset, int inputLength, WrappedArray output, int outputOffset) throws IOException;

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException;

    /**
     * A simple wrapper of a byte array.
     * Not all implementation allows to know what is the maximum size after
     * compression. This make it hard to size the ouput buffer for compress
     * (and we want to reuse the buffer).  Instead we use this wrapped buffer
     * so that compress can have the liberty to resize underlying array if
     * need be.
     */
    public static class WrappedArray
    {
        public byte[] buffer;

        public WrappedArray(byte[] buffer)
        {
            this.buffer = buffer;
        }
    }
}
