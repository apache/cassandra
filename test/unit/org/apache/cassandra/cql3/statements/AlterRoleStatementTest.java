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

package org.apache.cassandra.cql3.statements;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.auth.DCPermissions;
import org.apache.cassandra.cql3.QueryProcessor;

public class AlterRoleStatementTest
{
    private static AlterRoleStatement parse(String query)
    {
        ParsedStatement stmt = QueryProcessor.parseStatement(query);
        Assert.assertTrue(stmt instanceof AlterRoleStatement);
        return (AlterRoleStatement) stmt;
    }

    private static DCPermissions dcPerms(String query)
    {
        return parse(query).dcPermissions;
    }

    @Test
    public void dcsNotSpecified() throws Exception
    {
        Assert.assertNull(dcPerms("ALTER ROLE r1 WITH PASSWORD = 'password'"));
    }

    @Test
    public void dcsAllSpecified() throws Exception
    {
        DCPermissions dcPerms = dcPerms("ALTER ROLE r1 WITH ACCESS TO ALL DATACENTERS");
        Assert.assertNotNull(dcPerms);
        Assert.assertFalse(dcPerms.restrictsAccess());
    }

    @Test
    public void singleDc() throws Exception
    {
        DCPermissions dcPerms = dcPerms("ALTER ROLE r1 WITH ACCESS TO DATACENTERS {'dc1'}");
        Assert.assertNotNull(dcPerms);
        Assert.assertTrue(dcPerms.restrictsAccess());
        Assert.assertEquals(Sets.newHashSet("dc1"), dcPerms.allowedDCs());
    }

    @Test
    public void multiDcs() throws Exception
    {
        DCPermissions dcPerms = dcPerms("ALTER ROLE r1 WITH ACCESS TO DATACENTERS {'dc1', 'dc2'}");
        Assert.assertNotNull(dcPerms);
        Assert.assertTrue(dcPerms.restrictsAccess());
        Assert.assertEquals(Sets.newHashSet("dc1", "dc2"), dcPerms.allowedDCs());
    }
}
