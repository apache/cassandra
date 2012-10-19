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
package org.apache.cassandra.service;

import java.util.*;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SemanticVersion;

import static org.apache.cassandra.tracing.Tracing.instance;

/**
 * A container for per-client, thread-local state that Avro/Thrift threads must hold.
 * TODO: Kill thrift exceptions
 */
public class ClientState
{
    private static final int MAX_CACHE_PREPARED = 10000;    // Enough to keep buggy clients from OOM'ing us
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);
    public static final SemanticVersion DEFAULT_CQL_VERSION = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

    // Current user for the session
    private AuthenticatedUser user;
    private String keyspace;
    private UUID preparedTracingSession;

    // Reusable array for authorization
    private final List<Object> resource = new ArrayList<Object>();
    private SemanticVersion cqlVersion = DEFAULT_CQL_VERSION;

    // An LRU map of prepared statements
    private final Map<Integer, CQLStatement> prepared = new LinkedHashMap<Integer, CQLStatement>(16, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<Integer, CQLStatement> eldest) {
            return size() > MAX_CACHE_PREPARED;
        }
    };

    private long clock;

    // internalCall is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace
    private final boolean internalCall;

    public ClientState()
    {
        this(false);
    }

    /**
     * Construct a new, empty ClientState
     */
    public ClientState(boolean internalCall)
    {
        this.internalCall = internalCall;

        user = DatabaseDescriptor.getAuthenticator().defaultUser();
        resourceClear();
        prepared.clear();
    }

    public Map<Integer, CQLStatement> getPrepared()
    {
        return prepared;
    }

    public String getRawKeyspace()
    {
        return keyspace;
    }

    public String getKeyspace() throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("no keyspace has been specified");
        return keyspace;
    }

    public void setKeyspace(String ks) throws InvalidRequestException
    {
        if (Schema.instance.getKSMetaData(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        keyspace = ks;
    }

    public boolean traceNextQuery()
    {
        if (preparedTracingSession != null)
        {
            return true;
        }

        double tracingProbability = StorageService.instance.getTracingProbability();
        return tracingProbability != 0 && FBUtilities.threadLocalRandom().nextDouble() < tracingProbability;
    }

    public void prepareTracingSession(UUID sessionId)
    {
        this.preparedTracingSession = sessionId;
    }

    public void createSession()
    {
        if (this.preparedTracingSession == null)
        {
            instance().newSession();
        }
        else
        {
            UUID session = this.preparedTracingSession;
            this.preparedTracingSession = null;
            instance().newSession(session);
        }
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
        this.user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);
    }

    private void resourceClear()
    {
        resource.clear();
        resource.add(Resources.ROOT);
        resource.add(Resources.KEYSPACES);
    }

    public void hasKeyspaceAccess(String keyspace, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        hasColumnFamilySchemaAccess(keyspace, perm);
    }

    /**
     * Confirms that the client thread has the given Permission for the ColumnFamily list of
     * the provided keyspace.
     */
    public void hasColumnFamilySchemaAccess(String keyspace, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        validateLogin();
        validateKeyspace(keyspace);

        preventSystemKSSchemaModification(keyspace, perm);

        resourceClear();
        resource.add(keyspace);
        Set<Permission> perms = DatabaseDescriptor.getAuthority().authorize(user, resource);

        hasAccess(user, perms, perm, resource);
    }

    private void preventSystemKSSchemaModification(String keyspace, Permission perm) throws InvalidRequestException
    {
        if (keyspace.equalsIgnoreCase(Table.SYSTEM_KS) && !Permission.ALLOWED_SYSTEM_ACTIONS.contains(perm))
            throw new InvalidRequestException("system keyspace is not user-modifiable.");
    }

    /**
     * Confirms that the client thread has the given Permission in the context of the given
     * ColumnFamily and the current keyspace.
     */
    public void hasColumnFamilyAccess(String columnFamily, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        hasColumnFamilyAccess(keyspace, columnFamily, perm);
    }

    public void hasColumnFamilyAccess(String keyspace, String columnFamily, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        validateLogin();
        validateKeyspace(keyspace);

        resourceClear();
        resource.add(keyspace);

        if (!internalCall)
            preventSystemKSSchemaModification(keyspace, perm);

        // check if keyspace access is set to Permission.FULL_ACCESS
        // (which means that user has all access on keyspace and it's underlying elements)
        if (DatabaseDescriptor.getAuthority().authorize(user, resource).contains(Permission.FULL_ACCESS))
            return;

        resource.add(columnFamily);
        Set<Permission> perms = DatabaseDescriptor.getAuthority().authorize(user, resource);

        hasAccess(user, perms, perm, resource);
    }

    public boolean isLogged()
    {
        return user != null;
    }

    private void validateLogin() throws InvalidRequestException
    {
        if (user == null)
            throw new InvalidRequestException("You have not logged in");
    }

    private static void validateKeyspace(String keyspace) throws InvalidRequestException
    {
        if (keyspace == null)
        {
            throw new InvalidRequestException("You have not set a keyspace for this session");
        }
    }

    private static void hasAccess(AuthenticatedUser user, Set<Permission> perms, Permission perm, List<Object> resource) throws UnauthorizedException
    {
        if (perms.contains(Permission.FULL_ACCESS))
            return; // full access

        if (perms.contains(Permission.NO_ACCESS))
            throw new UnauthorizedException(String.format("%s does not have permission %s for %s",
                                                          user,
                                                          perm,
                                                          Resources.toString(resource)));


        boolean granular = false;

        for (Permission p : perms)
        {
            // mixing of old and granular permissions is denied by IAuthorityContainer
            // and CQL grammar so it's name to assume that once a granular permission is found
            // all other permissions are going to be a subset of Permission.GRANULAR_PERMISSIONS
            if (Permission.GRANULAR_PERMISSIONS.contains(p))
            {
                granular = true;
                break;
            }
        }

        if (granular)
        {
            if (perms.contains(perm))
                return; // user has a given permission, perm is always one of Permission.GRANULAR_PERMISSIONS
        }
        else
        {
            for (Permission p : perms)
            {
                if (Permission.oldToNew.get(p).contains(perm))
                    return;
            }
        }

        throw new UnauthorizedException(String.format("%s does not have permission %s for %s",
                                                      user,
                                                      perm,
                                                      Resources.toString(resource)));
    }

    /**
     * This clock guarantees that updates from a given client will be ordered in the sequence seen,
     * even if multiple updates happen in the same millisecond.  This can be useful when a client
     * wants to perform multiple updates to a single column.
     */
    public long getTimestamp()
    {
        long current = System.currentTimeMillis() * 1000;
        clock = clock >= current ? clock + 1 : current;
        return clock;
    }

    public void setCQLVersion(String str) throws InvalidRequestException
    {
        SemanticVersion version;
        try
        {
            version = new SemanticVersion(str);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        SemanticVersion cql = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;
        SemanticVersion cql3 = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

        // We've made some backward incompatible changes between CQL3 beta1 and the final.
        // It's ok because it was a beta, but it still mean we don't support 3.0.0-beta1 so reject it.
        SemanticVersion cql3Beta = new SemanticVersion("3.0.0-beta1");
        if (version.equals(cql3Beta))
            throw new InvalidRequestException(String.format("There has been a few syntax breaking changes between 3.0.0-beta1 and 3.0.0 "
                                                           + "(mainly the syntax for options of CREATE KEYSPACE and CREATE TABLE). 3.0.0-beta1 "
                                                           + " is not supported; please upgrade to 3.0.0"));
        if (version.isSupportedBy(cql))
            cqlVersion = cql;
        else if (version.isSupportedBy(cql3))
            cqlVersion = cql3;
        else
            throw new InvalidRequestException(String.format("Provided version %s is not supported by this server (supported: %s)",
                                                            version,
                                                            StringUtils.join(getCQLSupportedVersion(), ", ")));
    }

    public SemanticVersion getCQLVersion()
    {
        return cqlVersion;
    }

    public static SemanticVersion[] getCQLSupportedVersion()
    {
        SemanticVersion cql = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;
        SemanticVersion cql3 = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

        return new SemanticVersion[]{ cql, cql3 };
    }

    public void grantPermission(Permission permission, String to, CFName on, boolean grantOption) throws UnauthorizedException, InvalidRequestException
    {
        DatabaseDescriptor.getAuthorityContainer().grant(user, permission, to, on, grantOption);
    }

    public void revokePermission(Permission permission, String from, CFName resource) throws UnauthorizedException, InvalidRequestException
    {
        DatabaseDescriptor.getAuthorityContainer().revoke(user, permission, from, resource);
    }

    public ResultMessage listPermissions(String username) throws UnauthorizedException, InvalidRequestException
    {
        return DatabaseDescriptor.getAuthorityContainer().listPermissions(username);
    }
}
