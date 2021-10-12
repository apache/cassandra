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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.io.util.DataOutputBufferFixed;

public class StreamingDataOutputPlusFixed extends DataOutputBufferFixed implements StreamingDataOutputPlus
{
    public StreamingDataOutputPlusFixed(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public int writeToChannel(Write write, RateLimiter limiter) throws IOException
    {
        int position = buffer.position();
        write.write(size -> buffer);
        return buffer.position() - position;
    }

    @Override
    public long writeFileToChannel(FileChannel file, RateLimiter limiter) throws IOException
    {
        long count = 0;
        long tmp;
        while (0 <= (tmp = file.read(buffer))) count += tmp;
        return count;
    }
}
