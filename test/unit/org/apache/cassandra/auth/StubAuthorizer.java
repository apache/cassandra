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

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.utils.Pair;

public class StubAuthorizer implements IAuthorizer
{
    private final Map<Pair<String, IResource>, PermissionSets> userPermissions = new HashMap<>();

    public void clear()
    {
        userPermissions.clear();
    }

    public PermissionSets allPermissionSets(AuthenticatedUser user, IResource resource)
    {
        Pair<String, IResource> key = Pair.create(user.getName(), resource);
        PermissionSets perms = userPermissions.get(key);
        return perms != null ? perms : PermissionSets.EMPTY;
    }

    public Set<Permission> grant(AuthenticatedUser performer,
                                 Set<Permission> permissions,
                                 IResource resource,
                                 RoleResource grantee,
                                 GrantMode grantMode) throws RequestValidationException, RequestExecutionException
    {
        Pair<String, IResource> key = Pair.create(grantee.getRoleName(), resource);
        switch (grantMode)
        {
            case GRANT:
                userPermissions.compute(key, (k, old) ->
                                             (old != null ? old.unbuild() : PermissionSets.builder()).addGranted(permissions).build());

                return userPermissions.get(key).granted;

            case RESTRICT:
                userPermissions.compute(key, (k, old) ->
                                             (old != null ? old.unbuild() : PermissionSets.builder()).addRestricted(permissions).build());

                return userPermissions.get(key).restricted;

            case GRANTABLE:
                userPermissions.compute(key, (k, old) ->
                                             (old != null ? old.unbuild() : PermissionSets.builder()).addGrantables(permissions).build());

                return userPermissions.get(key).grantables;
        }
        
        return Collections.EMPTY_SET;
    }

    public Set<Permission> revoke(AuthenticatedUser performer,
                                  Set<Permission> permissions,
                                  IResource resource,
                                  RoleResource revokee,
                                  GrantMode grantMode) throws RequestValidationException, RequestExecutionException
    {
        Pair<String, IResource> key = Pair.create(revokee.getRoleName(), resource);
        PermissionSets perms = null;
        switch (grantMode)
        {
            case GRANT:
                perms = userPermissions.compute(key, (k, old) ->
                                             (old != null ? old.unbuild() : PermissionSets.builder()).removeGranted(permissions).build());

                return userPermissions.get(key).granted;

            case RESTRICT:
                perms = userPermissions.compute(key, (k, old) ->
                                             (old != null ? old.unbuild() : PermissionSets.builder()).removeRestricted(permissions).build());

                return userPermissions.get(key).restricted;

            case GRANTABLE:
                perms = userPermissions.compute(key, (k, old) ->
                                             (old != null ? old.unbuild() : PermissionSets.builder()).removeGrantables(permissions).build());

                return userPermissions.get(key).grantables;
        }

        if (perms.granted.isEmpty() && perms.restricted.isEmpty() && perms.grantables.isEmpty())
            userPermissions.remove(key);

        return Collections.EMPTY_SET;
    }

    public Set<PermissionDetails> list(Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee) throws RequestValidationException, RequestExecutionException
    {
        return userPermissions.entrySet()
                .stream()
                .filter(entry -> entry.getKey().left.equals(grantee.getRoleName())
                                 && (resource == null || entry.getKey().right.equals(resource))
                                 && containsAny(entry.getValue(), permissions))
                .flatMap(entry -> permissions.stream()
                                             .map(p -> new PermissionDetails(entry.getKey().left,
                                                                             entry.getKey().right,
                                                                             p,
                                                                             entry.getValue().grantModesFor(p))))
                .collect(Collectors.toSet());

    }

    private static boolean containsAny(PermissionSets value, Set<Permission> permissions)
    {
        return permissions.stream()
                          .anyMatch(p -> value.granted.contains(p) || value.restricted.contains(p) || value.grantables.contains(p));
    }

    public void revokeAllFrom(RoleResource revokee)
    {
        userPermissions.keySet()
        .removeAll(userPermissions.keySet()
                                  .stream()
                                  .filter(key -> key.left.equals(revokee.getRoleName())).collect(Collectors.toList()));
    }

    public void revokeAllOn(IResource droppedResource)
    {
        userPermissions.keySet()
        .removeAll(userPermissions.keySet()
                                  .stream()
                                  .filter(key -> key.right.equals(droppedResource)).collect(Collectors.toList()));
    }

    public Set<? extends IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
    }
}
