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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.utils.Pair;

public class StubAuthorizer implements IAuthorizer
{
    Map<Pair<String, IResource>, Set<Permission>> userPermissions = new HashMap<>();

    public void clear()
    {
        userPermissions.clear();
    }

    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        Pair<String, IResource> key = Pair.create(user.getName(), resource);
        Set<Permission> perms = userPermissions.get(key);
        return perms != null ? perms : Collections.emptySet();
    }

    public void grant(AuthenticatedUser performer,
                      Set<Permission> permissions,
                      IResource resource,
                      RoleResource grantee) throws RequestValidationException, RequestExecutionException
    {
        Pair<String, IResource> key = Pair.create(grantee.getRoleName(), resource);
        Set<Permission> perms = userPermissions.get(key);
        if (null == perms)
        {
            perms = new HashSet<>();
            userPermissions.put(key, perms);
        }
        perms.addAll(permissions);
    }

    public void revoke(AuthenticatedUser performer,
                       Set<Permission> permissions,
                       IResource resource,
                       RoleResource revokee) throws RequestValidationException, RequestExecutionException
    {
        Pair<String, IResource> key = Pair.create(revokee.getRoleName(), resource);
        Set<Permission> perms = userPermissions.get(key);
        if (null != perms)
        {
            perms.removeAll(permissions);
            if (perms.isEmpty())
                userPermissions.remove(key);
        }
    }

    public Set<PermissionDetails> list(AuthenticatedUser performer,
                                       Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee) throws RequestValidationException, RequestExecutionException
    {
        return userPermissions.entrySet()
                              .stream()
                              .filter(entry -> entry.getKey().left.equals(grantee.getRoleName())
                                               && (resource == null || entry.getKey().right.equals(resource)))
                              .flatMap(entry -> entry.getValue()
                                                     .stream()
                                                     .filter(permissions::contains)
                                                     .map(p -> new PermissionDetails(entry.getKey().left,
                                                                                     entry.getKey().right,
                                                                                     p)))
                              .collect(Collectors.toSet());

    }

    public void revokeAllFrom(RoleResource revokee)
    {
        for (Pair<String, IResource> key : userPermissions.keySet())
            if (key.left.equals(revokee.getRoleName()))
                userPermissions.remove(key);
    }

    public void revokeAllOn(IResource droppedResource)
    {
        for (Pair<String, IResource> key : userPermissions.keySet())
            if (key.right.equals(droppedResource))
                userPermissions.remove(key);
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
