/**
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

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;

public interface IAuthority2 extends IAuthority
{
    /**
     * Setup is called each time upon system startup
     */
    public void setup();

    /**
     * GRANT <permission> ON <resource> TO <user> [WITH GRANT OPTION];
     *
     * @param granter The user who grants the permission
     * @param permission The specific permission
     * @param to Grantee of the permission
     * @param resource The resource which is affect by permission change
     * @param grantOption Does grantee has a permission to grant the same kind of permission on this particular resource?
     *
     * @throws InvalidRequestException upon parameter misconfiguration or internal error.
     */
    public void grant(AuthenticatedUser granter, Permission permission, String to, CFName resource, boolean grantOption) throws InvalidRequestException;

    /**
     * REVOKE <permission> ON <resource> FROM <user_name>;
     *
     * @param revoker The user know requests permission revoke
     * @param permission The permission to revoke
     * @param from The user to revoke permission from.
     * @param resource The resource affected by permission change.
     *
     * @throws InvalidRequestException upon parameter misconfiguration or internal error.
     */
    public void revoke(AuthenticatedUser revoker, Permission permission, String from, CFName resource) throws InvalidRequestException;

    /**
     * LIST GRANTS FOR <user>;
     * Not 'SHOW' because it's reserved for CQLsh for commands like 'show cluster'
     *
     * @param username The username to look for permissions.
     *
     * @return All of the permission of this particular user.
     *
     * @throws InvalidRequestException upon parameter misconfiguration or internal error.
     */
    public CqlResult listPermissions(String username) throws InvalidRequestException;
}
