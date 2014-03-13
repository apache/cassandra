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

package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class CollectionSerializer<T> implements TypeSerializer<T>
{
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        // The collection is not currently being properly validated.
    }

    // Utilitary method
    protected static ByteBuffer pack(List<ByteBuffer> buffers, int elements, int size)
    {
        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short)elements);
        for (ByteBuffer bb : buffers)
        {
            result.putShort((short)bb.remaining());
            result.put(bb.duplicate());
        }
        return (ByteBuffer)result.flip();
    }

    public static ByteBuffer pack(List<ByteBuffer> buffers, int elements)
    {
        int size = 0;
        for (ByteBuffer bb : buffers)
            size += 2 + bb.remaining();
        return pack(buffers, elements, size);
    }
}
