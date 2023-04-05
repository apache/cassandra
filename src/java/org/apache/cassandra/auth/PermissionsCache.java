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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Pair;

public class PermissionsCache extends AuthCache<Pair<AuthenticatedUser, IResource>, Set<Permission>>
        implements PermissionsCacheMBean
{
    public PermissionsCache(IAuthorizer authorizer)
    {
        super(CACHE_NAME,
              DatabaseDescriptor::setPermissionsValidity,
              DatabaseDescriptor::getPermissionsValidity,
              DatabaseDescriptor::setPermissionsUpdateInterval,
              DatabaseDescriptor::getPermissionsUpdateInterval,
              DatabaseDescriptor::setPermissionsCacheMaxEntries,
              DatabaseDescriptor::getPermissionsCacheMaxEntries,
              DatabaseDescriptor::setPermissionsCacheActiveUpdate,
              DatabaseDescriptor::getPermissionsCacheActiveUpdate,
              (p) -> authorizer.authorize(p.left, p.right),
              authorizer.bulkLoader(),
              authorizer::requireAuthorization);
    }

    public Set<Permission> getPermissions(AuthenticatedUser user, IResource resource)
    {
        return get(Pair.create(user, resource));
    }

    public void invalidatePermissions(String roleName, String resourceName)
    {
        invalidate(Pair.create(new AuthenticatedUser(roleName), Resources.fromName(resourceName)));
    }
}
