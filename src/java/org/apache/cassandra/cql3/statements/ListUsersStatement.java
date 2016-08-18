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

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ListUsersStatement extends ListRolesStatement
{
    // pseudo-virtual cf as the actual datasource is dependent on the IRoleManager impl
    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String CF = "users";

    private static final List<ColumnSpecification> metadata =
        ImmutableList.of(new ColumnSpecification(KS, CF, new ColumnIdentifier("name", true), UTF8Type.instance),
                         new ColumnSpecification(KS, CF, new ColumnIdentifier("super", true), BooleanType.instance));

    @Override
    protected ResultMessage formatResults(List<RoleResource> sortedRoles)
    {
        ResultSet result = new ResultSet(metadata);

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        for (RoleResource role : sortedRoles)
        {
            if (!roleManager.canLogin(role))
                continue;
            result.addColumnValue(UTF8Type.instance.decompose(role.getRoleName()));
            result.addColumnValue(BooleanType.instance.decompose(Roles.hasSuperuserStatus(role)));
        }
        return new ResultMessage.Rows(result);
    }
}
