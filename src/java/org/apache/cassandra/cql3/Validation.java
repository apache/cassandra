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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A collection of static validation functions reused across statements.
 *
 * Note: this hosts functions that were historically in ThriftValidation, but
 * it's not necessary clear that this is the best place to have this (this is
 * certainly not horrible either though).
 */
public abstract class Validation
{

    /**
     * Validates a (full serialized) partition key.
     *
     * @param metadata the metadata for the table of which to check the key.
     * @param key the serialized partition key to check.
     *
     * @throws InvalidRequestException if the provided {@code key} is invalid.
     */
    public static void validateKey(TableMetadata metadata, ByteBuffer key)
    {
        if (key == null || key.remaining() == 0)
            throw new InvalidRequestException("Key may not be empty");

        // check that key can be handled by ByteArrayUtil.writeWithShortLength and ByteBufferUtil.writeWithShortLength
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " +
                                              FBUtilities.MAX_UNSIGNED_SHORT);
        }

        try
        {
            metadata.partitionKeyType.validate(key);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }
}
