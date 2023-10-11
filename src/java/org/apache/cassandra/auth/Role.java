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

import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class Role
{
    /**
     * Represents a user or group in the auth subsystem.
     * Roles may be members of other roles, but circular graphs of roles are not permitted.
     * The reason that memberOf is a {@code Set<String>} and not {@code Set<Role>} is to simplify loading
     * for IRoleManager implementations (in particular, CassandraRoleManager)
     */
    public final RoleResource resource;
    public final boolean isSuper;
    public final boolean canLogin;
    public final Set<String> memberOf;
    public final Map<String, String> options;

    public Role(String name, boolean isSuperUser, boolean canLogin, Map<String, String> options, Set<String> memberOf)
    {
        this.resource = RoleResource.role(name);
        this.isSuper = isSuperUser;
        this.canLogin = canLogin;
        this.memberOf = ImmutableSet.copyOf(memberOf);
        this.options = ImmutableMap.copyOf(options);
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof Role))
            return false;

        Role r = (Role)o;
        return Objects.equal(resource, r.resource)
               && Objects.equal(isSuper, r.isSuper)
               && Objects.equal(canLogin, r.canLogin)
               && Objects.equal(memberOf, r.memberOf)
               && Objects.equal(options, r.options);
    }

    public int hashCode()
    {
        return Objects.hashCode(resource, isSuper, canLogin, memberOf, options);
    }
}
