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

import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.DatabaseDescriptor;

public class RolesCache extends AuthCache<RoleResource, Set<RoleResource>> implements RolesCacheMBean
{
    public RolesCache(IRoleManager roleManager)
    {
        super("RolesCache",
              DatabaseDescriptor::setRolesValidity,
              DatabaseDescriptor::getRolesValidity,
              DatabaseDescriptor::setRolesUpdateInterval,
              DatabaseDescriptor::getRolesUpdateInterval,
              DatabaseDescriptor::setRolesCacheMaxEntries,
              DatabaseDescriptor::getRolesCacheMaxEntries,
              (r) -> roleManager.getRoles(r, true),
              () -> DatabaseDescriptor.getAuthenticator().requireAuthentication());
    }

    public Set<RoleResource> getRoles(RoleResource role)
    {
        try
        {
            return get(role);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
