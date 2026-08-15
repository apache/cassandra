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

public class AuthUtils
{
    static final ConsistencyLevel DEFAULT_SUPERUSER_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    private AuthUtils() {}

    public static String hashpw(String password)
    {
        return BCrypt.hashpw(password, PasswordSaltSupplier.get());
    }

    public static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    /** Allows selective overriding of the consistency level for specific roles. */
    public static ConsistencyLevel consistencyForRoleWrite(String role)
    {
        return role.equals(DatabaseDescriptor.getRoleManager().defaultRoleInitializer().defaultRoleName()) ? DEFAULT_SUPERUSER_CONSISTENCY_LEVEL : CassandraAuthorizer.authWriteConsistencyLevel();
    }

    public static ConsistencyLevel consistencyForRoleRead(String role)
    {
        return role.equals(DatabaseDescriptor.getRoleManager().defaultRoleInitializer().defaultRoleName()) ? DEFAULT_SUPERUSER_CONSISTENCY_LEVEL : CassandraAuthorizer.authReadConsistencyLevel();
    }
}
