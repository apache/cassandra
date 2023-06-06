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
import java.util.UUID;

import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.TimeUUID;

// Fully compatible with UUID, and indeed is interpreted as UUID for UDF
public class LegacyTimeUUIDType extends AbstractTimeUUIDType<UUID>
{
    public static final LegacyTimeUUIDType instance = new LegacyTimeUUIDType();

    private static final ByteBuffer MASKED_VALUE = instance.decompose(TimeUUID.minAtUnixMillis(0).asUUID());

    public TypeSerializer<UUID> getSerializer()
    {
        return UUIDSerializer.instance;
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
