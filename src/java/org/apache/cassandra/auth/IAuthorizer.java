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
     * Returns a set of permissions of a user on a resource.
     * Since Roles were introduced in version 3.0, Cassandra does not distinguish in any
     * meaningful way between users and roles. A role may or may not have login privileges
     * and roles may be granted to other roles. In fact, Cassandra does not really have the
     * concept of a user, except to link a client session to role. AuthenticatedUser can be
     * thought of as a manifestation of a role, linked to a specific client connection.
     *
     * @param user Authenticated user requesting authorization.
     * @param resource Resource for which the authorization is being requested. @see DataResource.
     * @return Set of permissions of the user on the resource. Should never return null. Use Permission.NONE instead.
     */
    Set<Permission> authorize(AuthenticatedUser user, IResource resource);

    /**
     * Grants a set of permissions on a resource to a role.
     * The opposite of revoke().
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param performer User who grants the permissions.
     * @param permissions Set of permissions to grant.
     * @param resource Resource on which to grant the permissions.
     * @param grantee Role to which the permissions are to be granted.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     * @throws java.lang.UnsupportedOperationException
     */
    void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Revokes a set of permissions on a resource from a user.
     * The opposite of grant().
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param performer User who revokes the permissions.
     * @param permissions Set of permissions to revoke.
     * @param revokee Role from which to the permissions are to be revoked.
     * @param resource Resource on which to revoke the permissions.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     * @throws java.lang.UnsupportedOperationException
     */
    void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Returns a list of permissions on a resource granted to a role.
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param performer User who wants to see the permissions.
     * @param permissions Set of Permission values the user is interested in. The result should only include the
     *                    matching ones.
     * @param resource The resource on which permissions are requested. Can be null, in which case permissions on all
     *                 resources should be returned.
     * @param grantee The role whose permissions are requested. Can be null, in which case permissions of every
     *           role should be returned.
     *
     * @return All of the matching permission that the requesting user is authorized to know about.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     * @throws java.lang.UnsupportedOperationException
     */
    Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Called before deleting a role with DROP ROLE statement (or the alias provided for compatibility,
     * DROP USER) so that a new role with the same name wouldn't inherit permissions of the deleted one in the future.
     * This removes all permissions granted to the Role in question.
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param revokee The role to revoke all permissions from.
     * @throws java.lang.UnsupportedOperationException
     */
    void revokeAllFrom(RoleResource revokee);

    /**
     * This method is called after a resource is removed (i.e. keyspace, table or role is dropped) and revokes all
     * permissions granted on the IResource in question.
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param droppedResource The resource to revoke all permissions on.
     * @throws java.lang.UnsupportedOperationException
     */
    void revokeAllOn(IResource droppedResource);

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
