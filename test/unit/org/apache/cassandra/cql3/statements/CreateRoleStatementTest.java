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

import org.apache.cassandra.auth.CIDRPermissions;
import org.apache.cassandra.auth.DCPermissions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;

public class CreateRoleStatementTest extends CQLTester
{

    private static CreateRoleStatement parse(String query)
    {
        CQLStatement.Raw stmt = QueryProcessor.parseStatement(query);
        Assert.assertTrue(stmt instanceof CreateRoleStatement);
        return (CreateRoleStatement) stmt;
    }

    private static DCPermissions dcPerms(String query)
    {
        return parse(query).dcPermissions;
    }

    private static CIDRPermissions cidrPerms(String query)
    {
        return parse(query).cidrPermissions;
    }

    @Test
    public void allDcsImplicit()
    {
        Assert.assertFalse(dcPerms("CREATE ROLE role").restrictsAccess());
    }

    @Test
    public void allDcsExplicit()
    {
        Assert.assertFalse(dcPerms("CREATE ROLE role WITH ACCESS TO ALL DATACENTERS").restrictsAccess());
    }

    @Test
    public void singleDc() throws Exception
    {
        DCPermissions perms = dcPerms("CREATE ROLE role WITH ACCESS TO DATACENTERS {'dc1'}");
        Assert.assertTrue(perms.restrictsAccess());
        Assert.assertEquals(Sets.newHashSet("dc1"), perms.allowedDCs());
    }

    @Test
    public void multiDcs()
    {
        DCPermissions perms = dcPerms("CREATE ROLE role WITH ACCESS TO DATACENTERS {'dc1', 'dc2'}");
        Assert.assertTrue(perms.restrictsAccess());
        Assert.assertEquals(Sets.newHashSet("dc1", "dc2"), perms.allowedDCs());
    }

    @Test
    public void allCidrsImplicit() throws Exception
    {
        Assert.assertFalse(cidrPerms("CREATE ROLE role").restrictsAccess());
    }

    @Test
    public void allCidrsExplicit()
    {
        Assert.assertFalse(dcPerms("CREATE ROLE role WITH ACCESS FROM ALL CIDRS").restrictsAccess());
    }

    @Test
    public void singleCidr()
    {
        CIDRPermissions perms = cidrPerms("CREATE ROLE role WITH ACCESS FROM CIDRS {'aodc'}");
        Assert.assertTrue(perms.restrictsAccess());
        Assert.assertEquals(Sets.newHashSet("aodc"), perms.allowedCIDRGroups());
    }

    @Test
    public void multiCidrs()
    {
        CIDRPermissions perms = cidrPerms("CREATE ROLE role WITH ACCESS FROM CIDRS {'aodc', 'aws'}");
        Assert.assertTrue(perms.restrictsAccess());
        Assert.assertEquals(Sets.newHashSet("aodc", "aws"), perms.allowedCIDRGroups());
    }
}
