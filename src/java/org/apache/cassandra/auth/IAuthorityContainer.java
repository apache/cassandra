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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * 1.1.x : Temporary measure to unable dynamic operations without changing IAuthority interface.
 */
public class IAuthorityContainer
{
    private final IAuthority authority;
    private final IAuthority2 dynamicAuthority;

    public IAuthorityContainer(IAuthority authority)
    {
        this.authority = authority;
        dynamicAuthority = (authority instanceof IAuthority2) ? ((IAuthority2) authority) : null;
    }

    public void setup()
    {
        if (dynamicAuthority != null)
            dynamicAuthority.setup();
    }

    public boolean isDynamic()
    {
        return dynamicAuthority != null;
    }

    public IAuthority getAuthority()
    {
        return authority;
    }

    public void grant(AuthenticatedUser granter, Permission permission, String to, CFName resource, boolean grantOption) throws UnauthorizedException, InvalidRequestException
    {
        if (dynamicAuthority == null)
            throw new InvalidRequestException("GRANT operation is not supported by your authority: " + authority);

        if (permission.equals(Permission.READ) || permission.equals(Permission.WRITE))
            throw new InvalidRequestException(String.format("Error setting permission to: %s, available permissions are %s", permission, Permission.GRANULAR_PERMISSIONS));

        dynamicAuthority.grant(granter, permission, to, resource, grantOption);
    }

    public void revoke(AuthenticatedUser revoker, Permission permission, String from, CFName resource) throws UnauthorizedException, InvalidRequestException
    {
        if (dynamicAuthority == null)
            throw new InvalidRequestException("REVOKE operation is not supported by your authority: " + authority);

        dynamicAuthority.revoke(revoker, permission, from, resource);
    }

    public ResultMessage listPermissions(String username) throws UnauthorizedException, InvalidRequestException
    {
        if (dynamicAuthority == null)
            throw new InvalidRequestException("LIST GRANTS operation is not supported by your authority: " + authority);

        return dynamicAuthority.listPermissions(username);
    }
}
