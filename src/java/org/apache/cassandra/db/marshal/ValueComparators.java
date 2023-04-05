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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ValueComparators
{
    public final Comparator<byte[]> array;
    public final Comparator<ByteBuffer> buffer;

    public ValueComparators(Comparator<byte[]> array, Comparator<ByteBuffer> buffer)
    {
        this.array = array;
        this.buffer = buffer;
    }

    public Comparator getForAccessor(ValueAccessor accessor)
    {
        if (accessor == ByteArrayAccessor.instance)
            return array;
        if (accessor == ByteBufferAccessor.instance)
            return buffer;
        throw new UnsupportedOperationException("Unsupported accessor: " + accessor.getClass().getName());
    }
}
