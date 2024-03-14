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

package org.apache.cassandra.index.accord;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.serializers.AccordRoutingKeyByteSource;

public class OrderedRouteSerializer
{
    private static final AccordRoutingKeyByteSource.FixedLength SERIALIZER = AccordRoutingKeyByteSource.fixedLength(DatabaseDescriptor.getPartitioner());

    public static ByteBuffer serializeRoutingKey(AccordRoutingKey key)
    {
        return ByteBuffer.wrap(SERIALIZER.serialize(key));
    }

    public static byte[] serializeRoutingKeyNoTable(AccordRoutingKey key)
    {
        return SERIALIZER.serializeNoTable(key);
    }

    public static byte[] unwrap(AccordRoutingKey key)
    {
        return SERIALIZER.serialize(key);
    }

    public static AccordRoutingKey deserializeRoutingKey(ByteBuffer bb)
    {
        try
        {
            return SERIALIZER.fromComparableBytes(ByteBufferAccessor.instance, bb);
        }
        catch (IOException e)
        {
            throw new UnsupportedOperationException(e);
        }
    }
}
