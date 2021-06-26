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

import java.nio.charset.StandardCharsets;

import org.apache.cassandra.db.marshal.ValueAccessor;

public class AsciiSerializer extends AbstractTextSerializer
{
    public static final AsciiSerializer instance = new AsciiSerializer();

    private AsciiSerializer()
    {
        super(StandardCharsets.US_ASCII);
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        // 0-127
        for (int i=0, size=accessor.size(value); i < size; i++)
        {
            byte b = accessor.getByte(value, i);
            if (b < 0)
                throw new MarshalException("Invalid byte for ascii: " + Byte.toString(b));
        }
    }
}
