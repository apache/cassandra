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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.exceptions.InvalidRequestException;
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
     * Retrieves the metadata for the provided keyspace and table name, throwing
     * a meaningful user exception if those doen't exist.
     *
     * @param keyspaceName the keyspace name.
     * @param tableName the table name.
     * @return the metadata for table {@code keyspaceName.tableName} if it
     * exists (otherwise an {@code InvalidRequestException} is thrown).
     *
     * @throws InvalidRequestException if the table requested doesn't exist.
     */
    public static CFMetaData validateColumnFamily(String keyspaceName, String tableName)
    throws InvalidRequestException
    {
        validateKeyspace(keyspaceName);
        if (tableName.isEmpty())
            throw new InvalidRequestException("non-empty table is required");

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, tableName);
        if (metadata == null)
            throw new InvalidRequestException("unconfigured table " + tableName);

        return metadata;
    }

    private static void validateKeyspace(String keyspaceName)
    throws KeyspaceNotDefinedException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
            throw new KeyspaceNotDefinedException("Keyspace " + keyspaceName + " does not exist");
    }

    /**
     * Validates a (full serialized) partition key.
     *
     * @param metadata the metadata for the table of which to check the key.
     * @param key the serialized partition key to check.
     *
     * @throws InvalidRequestException if the provided {@code key} is invalid.
     */
    public static void validateKey(CFMetaData metadata, ByteBuffer key)
    throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
            throw new InvalidRequestException("Key may not be empty");

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " +
                                              FBUtilities.MAX_UNSIGNED_SHORT);
        }

        try
        {
            metadata.getKeyValidator().validate(key);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    /**
     * Validates that the provided keyspace is not one of the system keyspace.
     *
     * @param keyspace the keyspace name to validate.
     *
     * @throws InvalidRequestException if {@code keyspace} is the name of a
     * system keyspace.
     */
    public static void validateKeyspaceNotSystem(String keyspace)
    throws InvalidRequestException
    {
        if (SchemaConstants.isSystemKeyspace(keyspace))
            throw new InvalidRequestException(String.format("%s keyspace is not user-modifiable", keyspace));
    }
}
