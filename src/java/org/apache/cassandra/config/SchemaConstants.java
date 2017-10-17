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

package org.apache.cassandra.config;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

public final class SchemaConstants
{
    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String SCHEMA_KEYSPACE_NAME = "system_schema";

    public static final String TRACE_KEYSPACE_NAME = "system_traces";
    public static final String AUTH_KEYSPACE_NAME = "system_auth";
    public static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";

    /* system keyspace names (the ones with LocalStrategy replication strategy) */
    public static final Set<String> LOCAL_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(SYSTEM_KEYSPACE_NAME, SCHEMA_KEYSPACE_NAME);

    /* replicate system keyspace names (the ones with a "true" replication strategy) */
    public static final Set<String> REPLICATED_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(TRACE_KEYSPACE_NAME, AUTH_KEYSPACE_NAME, DISTRIBUTED_KEYSPACE_NAME);
    /**
     * longest permissible KS or CF name.  Our main concern is that filename not be more than 255 characters;
     * the filename will contain both the KS and CF names. Since non-schema-name components only take up
     * ~64 characters, we could allow longer names than this, but on Windows, the entire path should be not greater than
     * 255 characters, so a lower limit here helps avoid problems.  See CASSANDRA-4110.
     */
    public static final int NAME_LENGTH = 48;

    // 59adb24e-f3cd-3e02-97f0-5b395827453f
    public static final UUID emptyVersion;

    static
    {
        try
        {
            emptyVersion = UUID.nameUUIDFromBytes(MessageDigest.getInstance("MD5").digest());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError();
        }
    }

    /**
     * @return whether or not the keyspace is a really system one (w/ LocalStrategy, unmodifiable, hardcoded)
     */
    public static boolean isLocalSystemKeyspace(String keyspaceName)
    {
        return LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }

    /**
     * @return whether or not the keyspace is a replicated system ks (system_auth, system_traces, system_distributed)
     */
    public static boolean isReplicatedSystemKeyspace(String keyspaceName)
    {
        return REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }
}
