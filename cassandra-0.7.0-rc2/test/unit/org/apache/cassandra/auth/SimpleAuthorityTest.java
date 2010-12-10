/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.auth;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SimpleAuthorityTest
{
    private final SimpleAuthority authority = new SimpleAuthority();

    private final AuthenticatedUser USER1 = new AuthenticatedUser("user1");
    private final AuthenticatedUser USER2 = new AuthenticatedUser("user2");
    private final AuthenticatedUser USER3 = new AuthenticatedUser("user3");

    private final List<Object> KEYSPACES_RESOURCE = Arrays.<Object>asList(Resources.ROOT, Resources.KEYSPACES);
    private final List<Object> KEYSPACE1_RESOURCE = Arrays.<Object>asList(Resources.ROOT, Resources.KEYSPACES, "Keyspace1");
    private final List<Object> KEYSPACE2_RESOURCE = Arrays.<Object>asList(Resources.ROOT, Resources.KEYSPACES, "Keyspace2");
    private final List<Object> STANDARD1_RESOURCE = Arrays.<Object>asList(Resources.ROOT,
                                                                          Resources.KEYSPACES,
                                                                          "Keyspace1",
                                                                          "Standard1");

    @Test
    public void testValidateConfiguration() throws Exception
    {
        authority.validateConfiguration();
    }

    @Test
    public void testAuthorizeKeyspace() throws Exception
    {
        assertEquals(Permission.ALL, authority.authorize(USER1, KEYSPACE1_RESOURCE));
        assertEquals(Permission.ALL, authority.authorize(USER2, KEYSPACE1_RESOURCE));
        assertEquals(Permission.NONE, authority.authorize(USER3, KEYSPACE1_RESOURCE));

        assertEquals("a keyspace not listed in the access file should be inaccessible",
                     Permission.NONE,
                     authority.authorize(USER1, KEYSPACE2_RESOURCE));
    }

    @Test
    public void testAuthorizeKeyspaceList() throws Exception
    {
        assertEquals("user1 should be able to modify the keyspace list",
                     Permission.ALL,
                     authority.authorize(USER1, KEYSPACES_RESOURCE));
        assertEquals("user2 should only be able to read the keyspace list",
                     EnumSet.of(Permission.READ),
                     authority.authorize(USER2, KEYSPACES_RESOURCE));
        assertEquals("user3 should only be able to read the keyspace list",
                     EnumSet.of(Permission.READ),
                     authority.authorize(USER3, KEYSPACES_RESOURCE));
    }
    
    @Test
    public void testAuthorizeColumnFamily() throws Exception
    {
        assertEquals(Permission.ALL, authority.authorize(USER1, STANDARD1_RESOURCE));
        assertEquals(EnumSet.of(Permission.READ), authority.authorize(USER2, STANDARD1_RESOURCE));
        assertEquals(Permission.NONE, authority.authorize(USER3, STANDARD1_RESOURCE));
    }
}
