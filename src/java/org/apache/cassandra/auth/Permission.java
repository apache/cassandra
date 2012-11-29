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

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * An enum encapsulating the set of possible permissions that an authenticated user can have on a resource.
 *
 * IAuthorizer implementations may encode permissions using ordinals, so the Enum order must never change order.
 * Adding new values is ok.
 */
public enum Permission
{
    @Deprecated
    READ,
    @Deprecated
    WRITE,

    // schema management
    CREATE, // required for CREATE KEYSPACE and CREATE TABLE.
    ALTER,  // required for ALTER KEYSPACE, ALTER TABLE, CREATE INDEX, DROP INDEX.
    DROP,   // required for DROP KEYSPACE and DROP TABLE.

    // data access
    SELECT, // required for SELECT.
    MODIFY, // required for INSERT, UPDATE, DELETE, TRUNCATE.

    // permission management
    AUTHORIZE; // required for GRANT and REVOKE.


    public static final Set<Permission> ALL_DATA =
            ImmutableSet.copyOf(EnumSet.range(Permission.CREATE, Permission.AUTHORIZE));

    public static final Set<Permission> ALL =
            ImmutableSet.copyOf(EnumSet.range(Permission.CREATE, Permission.AUTHORIZE));
    public static final Set<Permission> NONE = ImmutableSet.of();
}
