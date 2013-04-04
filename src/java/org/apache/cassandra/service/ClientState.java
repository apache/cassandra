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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SemanticVersion;

/**
 * State related to a client connection.
 */
public class ClientState
{
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);
    public static final SemanticVersion DEFAULT_CQL_VERSION = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

    private static final Set<IResource> READABLE_SYSTEM_RESOURCES = new HashSet<IResource>(5);
    private static final Set<IResource> PROTECTED_AUTH_RESOURCES = new HashSet<IResource>();

    // User-level permissions cache.
    private static final LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> permissionsCache = initPermissionsCache();

    static
    {
        // We want these system cfs to be always readable since many tools rely on them (nodetool, cqlsh, bulkloader, etc.)
        String[] cfs =  new String[] { SystemTable.LOCAL_CF,
                                       SystemTable.PEERS_CF,
                                       SystemTable.SCHEMA_KEYSPACES_CF,
                                       SystemTable.SCHEMA_COLUMNFAMILIES_CF,
                                       SystemTable.SCHEMA_COLUMNS_CF };
        for (String cf : cfs)
            READABLE_SYSTEM_RESOURCES.add(DataResource.columnFamily(Table.SYSTEM_KS, cf));

        PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
        PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());
    }

    // Current user for the session
    private volatile AuthenticatedUser user;
    private String keyspace;

    private SemanticVersion cqlVersion;

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
        if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
            this.user = AuthenticatedUser.ANONYMOUS_USER;
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
        // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
        // call set_keyspace() before calling login(), and we have to handle that.
        if (user != null && Schema.instance.getKSMetaData(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        keyspace = ks;
    }

    /**
     * Attempts to login this client with the given credentials map.
     */
    public void login(Map<String, String> credentials) throws AuthenticationException
    {
        AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);

        if (!user.isAnonymous() && !Auth.isExistingUser(user.getName()))
           throw new AuthenticationException(String.format("User %s doesn't exist - create it with CREATE USER query first",
                                                           user.getName()));

        this.user = user;
    }

    public void hasAllKeyspacesAccess(Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        if (internalCall)
            return;
        validateLogin();
        ensureHasPermission(perm, DataResource.root());
    }

    public void hasKeyspaceAccess(String keyspace, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        hasAccess(keyspace, perm, DataResource.keyspace(keyspace));
    }

    public void hasColumnFamilyAccess(String keyspace, String columnFamily, Permission perm)
    throws UnauthorizedException, InvalidRequestException
    {
        hasAccess(keyspace, perm, DataResource.columnFamily(keyspace, columnFamily));
    }

    private void hasAccess(String keyspace, Permission perm, DataResource resource)
    throws UnauthorizedException, InvalidRequestException
    {
        validateKeyspace(keyspace);
        if (internalCall)
            return;
        validateLogin();
        preventSystemKSSchemaModification(keyspace, resource, perm);
        if (perm.equals(Permission.SELECT) && READABLE_SYSTEM_RESOURCES.contains(resource))
            return;
        if (PROTECTED_AUTH_RESOURCES.contains(resource))
            if (perm.equals(Permission.CREATE) || perm.equals(Permission.ALTER) || perm.equals(Permission.DROP))
                throw new UnauthorizedException(String.format("%s schema is protected", resource));
        ensureHasPermission(perm, resource);
    }

    public void ensureHasPermission(Permission perm, IResource resource) throws UnauthorizedException
    {
        for (IResource r : Resources.chain(resource))
        {
            if (authorize(r).contains(perm))
                return;
        }
        throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                      user.getName(),
                                                      perm,
                                                      resource));
    }

    private void preventSystemKSSchemaModification(String keyspace, DataResource resource, Permission perm) throws UnauthorizedException
    {
        // we only care about schema modification.
        if (!(perm.equals(Permission.ALTER) || perm.equals(Permission.DROP) || perm.equals(Permission.CREATE)))
            return;

        if (Schema.systemKeyspaceNames.contains(keyspace.toLowerCase()))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        // we want to allow altering AUTH_KS itself.
        if (keyspace.equals(Auth.AUTH_KS) && !(resource.isKeyspaceLevel() && perm.equals(Permission.ALTER)))
            throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
    }

    public void validateLogin() throws UnauthorizedException
    {
        if (user == null)
            throw new UnauthorizedException("You have not logged in");
    }

    public void ensureNotAnonymous() throws UnauthorizedException
    {
        validateLogin();
        if (user.isAnonymous())
            throw new UnauthorizedException("You have to be logged in and not anonymous to perform this request");
    }

    private static void validateKeyspace(String keyspace) throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
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

    public AuthenticatedUser getUser()
    {
        return user;
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

    private static LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> initPermissionsCache()
    {
        if (DatabaseDescriptor.getAuthorizer() instanceof AllowAllAuthorizer)
            return null;

        int validityPeriod = DatabaseDescriptor.getPermissionsValidity();
        if (validityPeriod <= 0)
            return null;

        return CacheBuilder.newBuilder().expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                                        .build(new CacheLoader<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
                                        {
                                            public Set<Permission> load(Pair<AuthenticatedUser, IResource> userResource)
                                            {
                                                return DatabaseDescriptor.getAuthorizer().authorize(userResource.left,
                                                                                                    userResource.right);
                                            }
                                        });
    }

    private Set<Permission> authorize(IResource resource)
    {
        // AllowAllAuthorizer or manually disabled caching.
        if (permissionsCache == null)
            return DatabaseDescriptor.getAuthorizer().authorize(user, resource);

        try
        {
            return permissionsCache.get(Pair.create(user, resource));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
