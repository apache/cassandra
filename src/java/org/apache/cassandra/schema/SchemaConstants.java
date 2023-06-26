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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;

/**
 * When adding new String keyspace names here, double check if it needs to be added to PartitionDenylist.canDenylistKeyspace
 */
public final class SchemaConstants
{
    public static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");

    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String SCHEMA_KEYSPACE_NAME = "system_schema";

    public static final String TRACE_KEYSPACE_NAME = "system_traces";
    public static final String AUTH_KEYSPACE_NAME = "system_auth";
    public static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";

    public static final String VIRTUAL_SCHEMA = "system_virtual_schema";

    public static final String VIRTUAL_VIEWS = "system_views";

    public static final String DUMMY_KEYSPACE_OR_TABLE_NAME = "--dummy--";

    /* system keyspace names (the ones with LocalStrategy replication strategy) */
    public static final Set<String> LOCAL_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(SYSTEM_KEYSPACE_NAME, SCHEMA_KEYSPACE_NAME);

    /* virtual table system keyspace names */
    public static final Set<String> VIRTUAL_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(VIRTUAL_VIEWS, VIRTUAL_SCHEMA);

    /* replicate system keyspace names (the ones with a "true" replication strategy) */
    public static final Set<String> REPLICATED_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(TRACE_KEYSPACE_NAME, AUTH_KEYSPACE_NAME, DISTRIBUTED_KEYSPACE_NAME);
    /**
     * The longest permissible KS or CF name.
     *
     * Before CASSANDRA-16956, we used to care about not having the entire path longer than 255 characters because of
     * Windows support but this limit is by implementing CASSANDRA-16956 not in effect anymore.
     */
    public static final int NAME_LENGTH = 48;

    // 59adb24e-f3cd-3e02-97f0-5b395827453f
    public static final UUID emptyVersion;

    public static final List<String> LEGACY_AUTH_TABLES = Arrays.asList("credentials", "users", "permissions");

    public static boolean isValidName(String name)
    {
        return name != null && !name.isEmpty() && name.length() <= NAME_LENGTH && PATTERN_WORD_CHARS.matcher(name).matches();
    }

    static
    {
        emptyVersion = UUID.nameUUIDFromBytes(Digest.forSchema().digest());
    }

    /**
     * @return whether or not the keyspace is a really system one (w/ LocalStrategy, unmodifiable, hardcoded)
     */
    public static boolean isLocalSystemKeyspace(String keyspaceName)
    {
        return LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase()) || isVirtualSystemKeyspace(keyspaceName);
    }

    /**
     * @return whether or not the keyspace is a replicated system ks (system_auth, system_traces, system_distributed)
     */
    public static boolean isReplicatedSystemKeyspace(String keyspaceName)
    {
        return REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }

    /**
     * Checks if the keyspace is a virtual system keyspace.
     * @return {@code true} if the keyspace is a virtual system keyspace, {@code false} otherwise.
     */
    public static boolean isVirtualSystemKeyspace(String keyspaceName)
    {
        return VIRTUAL_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }

    /**
     * Checks if the keyspace is a system keyspace (local replicated or virtual).
     * @return {@code true} if the keyspace is a system keyspace, {@code false} otherwise.
     */
    public static boolean isSystemKeyspace(String keyspaceName)
    {
        return isLocalSystemKeyspace(keyspaceName) // this includes vtables
                || isReplicatedSystemKeyspace(keyspaceName);
    }

    /**
     * Returns the set of all system keyspaces
     * @return all system keyspaces
     */
    public static Set<String> getSystemKeyspaces()
    {
        return Sets.union(Sets.union(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES), VIRTUAL_SYSTEM_KEYSPACE_NAMES);
    }

    /**
     * Returns the set of local and replicated system keyspace names
     * @return all local and replicated system keyspace names
     */
    public static Set<String> getLocalAndReplicatedSystemKeyspaceNames()
    {
        return Sets.union(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES);
    }
    
    /**
     * Returns the set of all local and replicated system table names
     * @return all local and replicated system table names
     */
    public static Set<String> getLocalAndReplicatedSystemTableNames()
    {
        return ImmutableSet.<String>builder()
                           .addAll(SystemKeyspace.TABLE_NAMES)
                           .addAll(SchemaKeyspaceTables.ALL)
                           .addAll(TraceKeyspace.TABLE_NAMES)
                           .addAll(AuthKeyspace.TABLE_NAMES)
                           .addAll(SystemDistributedKeyspace.TABLE_NAMES)
                           .build();
    }
}
