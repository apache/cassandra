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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.auth.CIDRPermissions;
import org.apache.cassandra.auth.DCPermissions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;

public class CreateUserStatementTest
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
        Assert.assertFalse(dcPerms("CREATE USER u1").restrictsAccess());
    }

    @Test
    public void allCidrsImplicit() throws Exception
    {
        Assert.assertFalse(cidrPerms("CREATE USER u2").restrictsAccess());
    }
}
