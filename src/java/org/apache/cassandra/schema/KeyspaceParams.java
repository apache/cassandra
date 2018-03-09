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
package org.apache.cassandra.schema;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * An immutable class representing keyspace parameters (durability and replication).
 */
public final class KeyspaceParams
{
    public static final boolean DEFAULT_DURABLE_WRITES = true;

    /**
     * This determines durable writes for the {@link org.apache.cassandra.config.SchemaConstants#SCHEMA_KEYSPACE_NAME}
     * and {@link org.apache.cassandra.config.SchemaConstants#SYSTEM_KEYSPACE_NAME} keyspaces,
     * the only reason it is not final is for commitlog unit tests. It should only be changed for testing purposes.
     */
    @VisibleForTesting
    public static boolean DEFAULT_LOCAL_DURABLE_WRITES = true;

    public enum Option
    {
        DURABLE_WRITES,
        REPLICATION;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public final boolean durableWrites;
    public final ReplicationParams replication;

    public KeyspaceParams(boolean durableWrites, ReplicationParams replication)
    {
        this.durableWrites = durableWrites;
        this.replication = replication;
    }

    public static KeyspaceParams create(boolean durableWrites, Map<String, String> replication)
    {
        return new KeyspaceParams(durableWrites, ReplicationParams.fromMap(replication));
    }

    public static KeyspaceParams local()
    {
        return new KeyspaceParams(DEFAULT_LOCAL_DURABLE_WRITES, ReplicationParams.local());
    }

    public static KeyspaceParams simple(int replicationFactor)
    {
        return new KeyspaceParams(true, ReplicationParams.simple(replicationFactor));
    }

    public static KeyspaceParams simpleTransient(int replicationFactor)
    {
        return new KeyspaceParams(false, ReplicationParams.simple(replicationFactor));
    }

    public static KeyspaceParams nts(Object... args)
    {
        return new KeyspaceParams(true, ReplicationParams.nts(args));
    }

    public void validate(String name)
    {
        replication.validate(name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof KeyspaceParams))
            return false;

        KeyspaceParams p = (KeyspaceParams) o;

        return durableWrites == p.durableWrites && replication.equals(p.replication);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(durableWrites, replication);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add(Option.DURABLE_WRITES.toString(), durableWrites)
                          .add(Option.REPLICATION.toString(), replication)
                          .toString();
    }
}
