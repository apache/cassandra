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
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

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
    Set<Permission> authorize(AuthenticatedUser user, IResource resource);

    /**
     * Grants a set of permissions on a resource to a grantee that can be a role or a user.
     * The opposite of revoke().
     *
     * @param performer User who grants the permissions.
     * @param permissions Set of permissions to grant.
     * @param grantee Grantee of the permissions.
     * @param resource Resource on which to grant the permissions.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee grantee)
            throws RequestValidationException, RequestExecutionException;

    /**
     * Revokes a set of permissions on a resource from a revokee that can be a role or a user.
     * The opposite of grant().
     *
     * @param performer User who revokes the permissions.
     * @param permissions Set of permissions to revoke.
     * @param revokee Revokee of the permissions.
     * @param resource Resource on which to revoke the permissions.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee revokee)
            throws RequestValidationException, RequestExecutionException;

    /**
     * Returns a list of permissions on a resource of a grantee that can be a role or a user.
     *
     * @param performer User who wants to see the permissions.
     * @param permissions Set of Permission values the user is interested in. The result should only include the matching ones.
     * @param resource The resource on which permissions are requested. Can be null, in which case permissions on all resources
     *                 should be returned.
     * @param grantee Grantee whose permissions are requested. Can be null, in which case permissions of every user and role
     *                should be returned.
     *
     * @return All of the matching permission that the requesting user is authorized to know about.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee grantee)
            throws RequestValidationException, RequestExecutionException;

    /**
     * This method is called before deleting a user or role with a DROP USER or DROP ROLE query so that a new user or role
     * with the same name wouldn't inherit permissions of the deleted user or role in the future.
     *
     * @param revokee The user or role to revoke all permissions from.
     */
    void revokeAll(IGrantee revokee);

    /**
     * This method is called after a resource is removed (i.e. keyspace or a table is dropped).
     *
     * @param droppedResource The resource to revoke all permissions on.
     */
    void revokeAll(IResource droppedResource);

    /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces, column families that will be unmodifiable by users; other resources.
     */
    Set<? extends IResource> protectedResources();

    /**
     * Validates configuration of IAuthorizer implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Setup is called once upon system startup to initialize the IAuthorizer.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();
}
