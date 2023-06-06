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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ClientStateTest
{
    static WithProperties properties;

    @BeforeClass
    public static void beforeClass()
    {
        properties = new WithProperties().set(ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION, true);
        SchemaLoader.prepareServer();
        DatabaseDescriptor.setAuthFromRoot(true);
        // create the system_auth keyspace so the IRoleManager can function as normal
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, TableMetadata.class));

        AuthCacheService.initializeAndRegisterCaches();
    }

    @AfterClass
    public static void afterClass()
    {
        properties.close();
    }

    @Test
    public void permissionsCheckStartsAtHeadOfResourceChain()
    {
        // verify that when performing a permissions check, we start from the
        // root IResource in the applicable hierarchy and proceed to the more
        // granular resources until we find the required permission (or until
        // we reach the end of the resource chain). This is because our typical
        // usage is to grant blanket permissions on the root resources to users
        // and so we save lookups, cache misses and cache space by traversing in
        // this order. e.g. for DataResources, we typically grant perms on the
        // 'data' resource, so when looking up a users perms on a specific table
        // it makes sense to follow: data -> keyspace -> table

        final AtomicInteger getPermissionsRequestCount = new AtomicInteger(0);
        final IResource rootResource = DataResource.root();
        final IResource tableResource = DataResource.table("test_ks", "test_table");
        final AuthenticatedUser testUser = new AuthenticatedUser("test_user")
        {
            public Set<Permission> getPermissions(IResource resource)
            {
                getPermissionsRequestCount.incrementAndGet();
                if (resource.equals(rootResource))
                    return Permission.ALL;

                fail(String.format("Permissions requested for unexpected resource %s", resource));
                // need a return to make the compiler happy
                return null;
            }

            public boolean canLogin() { return true; }
        };

        Roles.cache.invalidate();

        // finally, need to configure CassandraAuthorizer so we don't shortcircuit out of the authz process
        DatabaseDescriptor.setAuthorizer(new AuthTestUtils.LocalCassandraAuthorizer());

        // check permissions on the table, which should check for the root resource first
        // & return successfully without needing to proceed further
        ClientState state = ClientState.forInternalCalls();
        state.login(testUser);
        state.ensurePermission(Permission.SELECT, tableResource);
        assertEquals(1, getPermissionsRequestCount.get());
    }
}