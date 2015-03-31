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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * Responsible for managing roles (which also includes what
 * used to be known as users), including creation, deletion,
 * alteration and the granting & revoking of roles to other
 * roles.
 */
public interface IRoleManager
{

    /**
     * Supported options for CREATE ROLE/ALTER ROLE (and
     * CREATE USER/ALTER USER, which are aliases provided
     * for backwards compatibility).
     */
    public enum Option
    {
        SUPERUSER, PASSWORD, LOGIN, OPTIONS
    }

    /**
     * Set of options supported by CREATE ROLE and ALTER ROLE queries.
     * Should never return null - always return an empty set instead.
     */
    Set<Option> supportedOptions();

    /**
     * Subset of supportedOptions that users are allowed to alter when performing ALTER ROLE [themselves].
     * Should never return null - always return an empty set instead.
     */
    Set<Option> alterableOptions();

    /**
     * Called during execution of a CREATE ROLE statement.
     * options are guaranteed to be a subset of supportedOptions().
     *
     * @param performer User issuing the create role statement.
     * @param role Rolei being created
     * @param options Options the role will be created with
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Called during execution of DROP ROLE statement, as well we removing any main record of the role from the system
     * this implies that we want to revoke this role from all other roles that it has been granted to.
     *
     * @param performer User issuing the drop role statement.
     * @param role Role to be dropped.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException;

    /**
     * Called during execution of ALTER ROLE statement.
     * options are always guaranteed to be a subset of supportedOptions(). Furthermore, if the actor performing the query
     * is not a superuser and is altering themself, then options are guaranteed to be a subset of alterableOptions().
     * Keep the body of the method blank if your implementation doesn't support modification of any options.
     *
     * @param performer User issuing the alter role statement.
     * @param role Role that will be altered.
     * @param options Options to alter.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Called during execution of GRANT ROLE query.
     * Grant an role to another existing role. A grantee that has a role granted to it will inherit any
     * permissions of the granted role.
     *
     * @param performer User issuing the grant statement.
     * @param role Role to be granted to the grantee.
     * @param grantee Role acting as the grantee.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Called during the execution of a REVOKE ROLE query.
     * Revoke an granted role from an existing role. The revokee will lose any permissions inherited from the role being
     * revoked.
     *
     * @param performer User issuing the revoke statement.
     * @param role Role to be revoked.
     * @param revokee Role from which the granted role is to be revoked.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Called during execution of a LIST ROLES query.
     * Returns a set of roles that have been granted to the grantee using GRANT ROLE.
     *
     * @param grantee Role whose granted roles will be listed.
     * @param includeInherited if True will list inherited roles as well as those directly granted to the grantee.
     * @return A list containing the granted roles for the user.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException;

    /**
     * Called during the execution of an unqualified LIST ROLES query.
     * Returns the total set of distinct roles in the system.
     *
     * @return the set of all roles in the system.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException;

    /**
     * Return true if there exists a Role with the given name that also has
     * superuser status. Superuser status may be inherited from another
     * granted role, so this method should return true if either the named
     * Role, or any other Role it is transitively granted has superuser
     * status.
     *
     * @param role Role whose superuser status to verify
     * @return true if the role exists and has superuser status, either
     * directly or transitively, otherwise false.
     */
    boolean isSuper(RoleResource role);

    /**
     * Return true if there exists a Role with the given name which has login
     * privileges. Such privileges is not inherited from other granted Roles
     * and so must be directly granted to the named Role with the LOGIN option
     * of CREATE ROLE or ALTER ROLE
     *
     * @param role Role whose login privileges to verify
     * @return true if the role exists and is permitted to login, otherwise false
     */
    boolean canLogin(RoleResource role);

    /**
     * Where an implementation supports OPTIONS in CREATE and ALTER operations
     * this method should return the Map<String, String> representing the custom
     * options associated with the role, as supplied to CREATE or ALTER.
     * It should never return null; if the implementation does not support
     * OPTIONS or if none were supplied then it should return an empty map.
     * @param role Role whose custom options are required
     * @return Key/Value pairs representing the custom options for the Role
     */
    Map<String, String> getCustomOptions(RoleResource role);

    /**
     * Return true is a Role with the given name exists in the system.
     *
     * @param role Role whose existence to verify
     * @return true if the name identifies an extant Role in the system,
     * otherwise false
     */
    boolean isExistingRole(RoleResource role);

    /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces and column families that will be unmodifiable by users; other resources.
     */
    Set<? extends IResource> protectedResources();

    /**
     * Hook to perform validation of an implementation's configuration (if supported).
     *
     * @throws ConfigurationException
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Hook to perform implementation specific initialization, called once upon system startup.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();
}
