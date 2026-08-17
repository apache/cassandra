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

package org.apache.cassandra.auth;

import org.apache.commons.lang3.StringUtils;
import org.mindrot.jbcrypt.BCrypt;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;

import static org.apache.cassandra.auth.PasswordDefaultRoleInitializer.DEFAULT_SUPERUSER_NAME;

public class AuthUtils
{
    static final ConsistencyLevel DEFAULT_SUPERUSER_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    private AuthUtils() {}

    public static String hashpw(String password)
    {
        return BCrypt.hashpw(password, PasswordSaltSupplier.get());
    }

    /**
     * Escapes a value for safe interpolation into a single-quoted CQL string literal by doubling any
     * single quotes it contains (e.g. {@code o'brien} becomes {@code o''brien}). Used when building auth
     * DDL/DML from role, identity and resource names via {@link String#format}.
     *
     * @param name the raw value to escape; {@code null} is returned unchanged
     * @return the value with every {@code '} doubled
     */
    public static String escapeCqlLiteral(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    /** Allows selective overriding of the consistency level for specific roles. */
    public static ConsistencyLevel consistencyForRoleWrite(String role)
    {
        return defaultRoleName().equals(role) ? DEFAULT_SUPERUSER_CONSISTENCY_LEVEL : CassandraAuthorizer.authWriteConsistencyLevel();
    }

    public static ConsistencyLevel consistencyForRoleRead(String role)
    {
        return defaultRoleName().equals(role) ? DEFAULT_SUPERUSER_CONSISTENCY_LEVEL : CassandraAuthorizer.authReadConsistencyLevel();
    }

    private static String defaultRoleName()
    {
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        IDefaultRoleInitializer initializer = roleManager != null
                                              ? roleManager.defaultRoleInitializer()
                                              : DatabaseDescriptor.getDefaultRoleInitializer();
        return initializer != null ? initializer.defaultRoleName() : DEFAULT_SUPERUSER_NAME;
    }
}
