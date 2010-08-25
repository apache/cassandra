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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.Config.RequestSchedulerId;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.AccessLevel;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.InvalidRequestException;

/**
 * A container for per-client, thread-local state that Avro/Thrift threads must hold.
 * TODO: Kill thrift exceptions
 */
public class ClientState
{
    private static Logger logger = LoggerFactory.getLogger(ClientState.class);
    
    // true if the keyspace should be used as the scheduling id
    private final boolean SCHEDULE_ON_KEYSPACE = DatabaseDescriptor.getRequestSchedulerId().equals(RequestSchedulerId.keyspace);

    // Current user for the session
    private final ThreadLocal<AuthenticatedUser> user = new ThreadLocal<AuthenticatedUser>()
    {
        @Override
        public AuthenticatedUser initialValue()
        {
            return DatabaseDescriptor.getAuthenticator().defaultUser();
        }
    };

    // Keyspace and keyspace AccessLevels associated with the session
    private final ThreadLocal<String> keyspace = new ThreadLocal<String>();
    private final ThreadLocal<AccessLevel> keyspaceAccess = new ThreadLocal<AccessLevel>();

    /**
     * Called when the keyspace or user have changed.
     */
    private void updateKeyspaceAccess()
    {
        if (user.get() == null || keyspace.get() == null)
            // user is not logged in or keyspace is not set
            keyspaceAccess.set(null);
        else
            // authorize the user for the current keyspace
            keyspaceAccess.set(DatabaseDescriptor.getAuthority().authorize(user.get(), keyspace.get()));
    }

    public String getKeyspace()
    {
        return keyspace.get();
    }

    public void setKeyspace(String ks)
    {
        keyspace.set(ks);
        updateKeyspaceAccess();
    }

    public String getSchedulingId()
    {
        if (SCHEDULE_ON_KEYSPACE)
            return keyspace.get();
        return "default";
    }

    /**
     * Attempts to login this client with the given credentials map.
     */
    public void login(Map<String,String> credentials) throws AuthenticationException
    {
        AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);
        if (logger.isDebugEnabled())
            logger.debug("logged in: {}", user);
        this.user.set(user);
        updateKeyspaceAccess();
    }

    public void logout()
    {
        if (logger.isDebugEnabled())
            logger.debug("logged out: {}", user.get());
        user.remove();
        keyspace.remove();
        keyspaceAccess.remove();
    }

    /**
     * Confirms that the client thread has the given AccessLevel in the context of the current Keyspace.
     */
    public void hasKeyspaceAccess(AccessLevel level) throws InvalidRequestException
    {
        if (user.get() == null)
            throw new InvalidRequestException("You have not logged in");
        if (keyspaceAccess.get() == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
        if (keyspaceAccess.get().ordinal() < level.ordinal())
            throw new InvalidRequestException(String.format("Your user (%s) does not have permission to perform %s operations", user, level));
    }
}
