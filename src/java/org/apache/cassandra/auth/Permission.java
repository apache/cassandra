/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.auth;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * An enum encapsulating the set of possible permissions that an authenticated user can have for a resource.
 *
 * IAuthority implementations may encode permissions using ordinals, so the Enum order must never change.
 */
public enum Permission
{
    READ,  // for backward compatibility
    WRITE, // for backward compatibility

    FULL_ACCESS,
    NO_ACCESS,

    // schema management
    USE,
    CREATE,
    ALTER,
    DROP,

    // data access
    UPDATE,
    DELETE,
    SELECT;

    public static final EnumSet<Permission> ALL = EnumSet.allOf(Permission.class);
    public static final EnumSet<Permission> NONE = EnumSet.noneOf(Permission.class);
    public static final EnumSet<Permission> GRANULAR_PERMISSIONS = EnumSet.range(FULL_ACCESS, SELECT);

    /**
     * Maps old permissions to the new ones as we want to support old client IAuthority implementations
     * and new style of granular permission checking at the same time.
     */
    public static final Map<Permission, EnumSet<Permission>> oldToNew = new HashMap<Permission, EnumSet<Permission>>(2)
    {{
        put(READ,  EnumSet.of(USE, SELECT));
        put(WRITE, EnumSet.range(USE, DELETE));
    }};
}
