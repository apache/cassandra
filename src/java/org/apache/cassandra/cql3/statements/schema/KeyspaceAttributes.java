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
package org.apache.cassandra.cql3.statements.schema;

import java.util.*;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.KeyspaceParams.Option;
import org.apache.cassandra.schema.ReplicationParams;

public final class KeyspaceAttributes extends PropertyDefinitions
{
    private static final Set<String> validKeywords;
    private static final Set<String> obsoleteKeywords;

    static
    {
        ImmutableSet.Builder<String> validBuilder = ImmutableSet.builder();
        for (Option option : Option.values())
            validBuilder.add(option.toString());
        validKeywords = validBuilder.build();
        obsoleteKeywords = ImmutableSet.of();
    }

    public void validate()
    {
        validate(validKeywords, obsoleteKeywords);

        Map<String, String> replicationOptions = getAllReplicationOptions();
        if (!replicationOptions.isEmpty() && !replicationOptions.containsKey(ReplicationParams.CLASS))
            throw new ConfigurationException("Missing replication strategy class");
    }

    public String getReplicationStrategyClass()
    {
        return getAllReplicationOptions().get(ReplicationParams.CLASS);
    }

    private Map<String, String> getAllReplicationOptions()
    {
        Map<String, String> replication = getMap(Option.REPLICATION.toString());
        return replication == null
             ? Collections.emptyMap()
             : replication;
    }

    KeyspaceParams asNewKeyspaceParams()
    {
        boolean durableWrites = getBoolean(Option.DURABLE_WRITES.toString(), KeyspaceParams.DEFAULT_DURABLE_WRITES);
        return KeyspaceParams.create(durableWrites, getAllReplicationOptions());
    }

    KeyspaceParams asAlteredKeyspaceParams(KeyspaceParams previous)
    {
        boolean durableWrites = getBoolean(Option.DURABLE_WRITES.toString(), previous.durableWrites);
        Map<String, String> previousOptions = previous.replication.options;
        ReplicationParams replication = getReplicationStrategyClass() == null
                                      ? previous.replication
                                      : ReplicationParams.fromMapWithDefaults(getAllReplicationOptions(), previousOptions);
        return new KeyspaceParams(durableWrites, replication);
    }

    public boolean hasOption(Option option)
    {
        return hasProperty(option.toString());
    }
}
