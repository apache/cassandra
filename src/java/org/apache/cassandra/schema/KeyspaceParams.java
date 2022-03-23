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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.NetworkTopologyStrategy;

import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY;

/**
 * An immutable class representing keyspace parameters (durability and replication).
 */
public final class KeyspaceParams
{
    private static final Logger logger = LoggerFactory.getLogger(KeyspaceParams.class);

    public static final boolean DEFAULT_DURABLE_WRITES = true;

    /**
     * This determines durable writes for the {@link org.apache.cassandra.schema.SchemaConstants#SCHEMA_KEYSPACE_NAME}
     * and {@link org.apache.cassandra.schema.SchemaConstants#SYSTEM_KEYSPACE_NAME} keyspaces,
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

    private static final Map<String, String> SYSTEM_DISTRIBUTED_NTS_OVERRIDE = getSystemDistributedNtsOverride();

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

    public static KeyspaceParams simple(String replicationFactor)
    {
        return new KeyspaceParams(true, ReplicationParams.simple(replicationFactor));
    }

    public static KeyspaceParams simpleTransient(int replicationFactor)
    {
        return new KeyspaceParams(false, ReplicationParams.simple(replicationFactor));
    }

    public static KeyspaceParams everywhere()
    {
        return new KeyspaceParams(true, ReplicationParams.everywhere());
    }

    public static KeyspaceParams nts(Object... args)
    {
        return new KeyspaceParams(true, ReplicationParams.nts(args));
    }

    public void validate(String name)
    {
        replication.validate(name);
    }

    /**
     * Used to pick the default replication strategy for all distributed system keyspaces.
     * The default will be SimpleStrategy and a hard coded RF factor.
     * <p>
     * One can change this default to NTS by passing in system properties:
     * -Dcassandra.system_distributed_replication_per_dc=3
     * -Dcassandra.system_distributed_replication_dc_names=cloud-east,cloud-west
     */
    public static KeyspaceParams systemDistributed(int rf)
    {
        if (!SYSTEM_DISTRIBUTED_NTS_OVERRIDE.isEmpty())
        {
            logger.info("Using override for distributed system keyspaces: {}", SYSTEM_DISTRIBUTED_NTS_OVERRIDE);
            return create(true, SYSTEM_DISTRIBUTED_NTS_OVERRIDE);
        }

        return simple(rf);
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

    @VisibleForTesting
    static Map<String, String> getSystemDistributedNtsOverride()
    {
        int rfOverride = -1;
        List<String> dcOverride = Collections.emptyList();
        ImmutableMap.Builder<String, String> ntsOverride = ImmutableMap.builder();

        try
        {
            rfOverride = SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY.getInt(-1);
            dcOverride = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY.getString(","));
        }
        catch (RuntimeException ex)
        {
            logger.error("Error parsing system distributed replication override properties", ex);
        }

        if (rfOverride != -1 && !dcOverride.isEmpty())
        {
            // Validate reasonable defaults
            if (rfOverride <= 0 || rfOverride > 5)
            {
                logger.error("Invalid value for {}", SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY.getKey());
            }
            else
            {
                for (String dc : dcOverride)
                    ntsOverride.put(dc, String.valueOf(rfOverride));

                ntsOverride.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getCanonicalName());
                return ntsOverride.build();
            }
        }

        return Collections.emptyMap();
    }
}
