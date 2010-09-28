/**
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

package org.apache.cassandra.service;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Config.RequestSchedulerId;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.InvalidRequestException;

/**
 * A container for per-client, thread-local state that Avro/Thrift threads must hold.
 * TODO: Kill thrift exceptions
 */
public class ClientState
{
    private static Logger logger = LoggerFactory.getLogger(ClientState.class);

    // Current user for the session
    private AuthenticatedUser user;
    private String keyspace;
    private Set<Permission> keyspaceAccess;

    /**
     * Construct a new, empty ClientState: can be reused after logout() or reset().
     */
    public ClientState()
    {
        reset();
    }

    /**
     * Called when the keyspace or user have changed.
     */
    private void updateKeyspaceAccess()
    {
        if (user == null || keyspace == null)
            // user is not logged in or keyspace is not set
            keyspaceAccess = null;
        else
            // authorize the user for the current keyspace
            keyspaceAccess = DatabaseDescriptor.getAuthority().authorize(user, keyspace);
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public void setKeyspace(String ks)
    {
        keyspace = ks;
        updateKeyspaceAccess();
    }

    public String getSchedulingValue()
    {
        switch(DatabaseDescriptor.getRequestSchedulerId())
        {
            case keyspace: return keyspace;
        }
        return "default";
    }

    /**
     * Attempts to login this client with the given credentials map.
     */
    public void login(Map<? extends CharSequence,? extends CharSequence> credentials) throws AuthenticationException
    {
        AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);
        if (logger.isDebugEnabled())
            logger.debug("logged in: {}", user);
        this.user = user;
        updateKeyspaceAccess();
    }

    public void logout()
    {
        if (logger.isDebugEnabled())
            logger.debug("logged out: {}", user);
        reset();
    }

    public void reset()
    {
        user = DatabaseDescriptor.getAuthenticator().defaultUser();
        keyspace = null;
        keyspaceAccess = null;
    }

    /**
     * Confirms that the client thread has the given Permission in the context of the current Keyspace.
     */
    public void hasKeyspaceAccess(Permission perm) throws InvalidRequestException
    {
        if (user == null)
            throw new InvalidRequestException("You have not logged in");
        if (keyspaceAccess == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
        if (!keyspaceAccess.contains(perm))
            throw new InvalidRequestException(String.format("You (%s) do not have permission %s for %s", user, perm, keyspace));
    }
}
