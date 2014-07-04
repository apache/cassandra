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

import org.apache.cassandra.exceptions.AuthenticationException;

/**
 * An implementation of the PasswordAuthenticator that adds any roles, that have
 * been granted to the user, to the returned AuthenticatedUser. This should be
 * used with the CassandraRoleAwareAuthorizer to enable role based access control
 */
public class RoleAwarePasswordAuthenticator extends PasswordAuthenticator
{
    @Override
    public AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException
    {
        AuthenticatedUser user = super.authenticate(credentials);
        return new AuthenticatedUser(user.getName(), Auth.getRoles(Grantee.asUser(user.getName()), true));
    }
}
