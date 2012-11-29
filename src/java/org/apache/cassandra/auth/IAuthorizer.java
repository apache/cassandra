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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;

/**
 * Primary Cassandra authorization interface.
 */
public interface IAuthorizer
{
    /**
     * The primary IAuthorizer method. Returns a set of permissions of a user on a resource.
     *
     * @param user Authenticated user requesting authorization.
     * @param resource Resource for which the authorization is being requested. @see DataResource.
     * @return Set of permissions of the user on the resource. Should never return null. Use Permission.NONE instead.
     */
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource);

    /**
     * Grants a set of permissions on a resource to a user.
     * The opposite of revoke().
     *
     * @param performer User who grants the permissions.
     * @param permissions Set of permissions to grant.
     * @param to Grantee of the permissions.
     * @param resource Resource on which to grant the permissions.
     *
     * @throws UnauthorizedException if the granting user isn't allowed to grant (and revoke) the permissions on the resource.
     * @throws InvalidRequestException upon parameter misconfiguration or internal error.
     */
    public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String to)
    throws UnauthorizedException, InvalidRequestException;

    /**
     * Revokes a set of permissions on a resource from a user.
     * The opposite of grant().
     *
     * @param performer User who revokes the permissions.
     * @param permissions Set of permissions to revoke.
     * @param from Revokee of the permissions.
     * @param resource Resource on which to revoke the permissions.
     *
     * @throws UnauthorizedException if the revoking user isn't allowed to revoke the permissions on the resource.
     * @throws InvalidRequestException upon parameter misconfiguration or internal error.
     */
    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String from)
    throws UnauthorizedException, InvalidRequestException;

    /**
     * Returns a list of permissions on a resource of a user.
     *
     * @param performer User who wants to see the permissions.
     * @param permissions Set of Permission values the user is interested in. The result should only include the matching ones.
     * @param resource The resource on which permissions are requested. Can be null, in which case permissions on all resources
     *                 should be returned.
     * @param of The user whose permissions are requested. Can be null, in which case permissions of every user should be returned.
     *
     * @return All of the matching permission that the requesting user is authorized to know about.
     *
     * @throws UnauthorizedException if the user isn't allowed to view the requested permissions.
     * @throws InvalidRequestException upon parameter misconfiguration or internal error.
     */
    public Set<PermissionDetails> listPermissions(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String of)
    throws UnauthorizedException, InvalidRequestException;

    /**
     * This method is called before deleting a user with DROP USER query so that a new user with the same
     * name wouldn't inherit permissions of the deleted user in the future.
     *
     * @param droppedUser The user to revoke all permissions from.
     */
    public void revokeAll(String droppedUser);

    /**
     * This method is called after a resource is removed (i.e. keyspace or a table is dropped).
     *
     * @param droppedResource The resource to revoke all permissions on.
     */
    public void revokeAll(IResource droppedResource);

    /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces, column families that will be unreadable and unmodifiable by users; other resources.
     */
    public Set<? extends IResource> protectedResources();

    /**
     * Validates configuration of IAuthorizer implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    public void validateConfiguration() throws ConfigurationException;

    /**
     * Setup is called once upon system startup to initialize the IAuthorizer.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    public void setup();
}
