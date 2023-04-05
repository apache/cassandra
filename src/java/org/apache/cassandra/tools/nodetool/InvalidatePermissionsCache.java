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
package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "invalidatepermissionscache", description = "Invalidate the permissions cache")
public class InvalidatePermissionsCache extends NodeToolCmd
{
    @Arguments(usage = "[<role>]", description = "A role for which permissions to specified resources need to be invalidated")
    private List<String> args = new ArrayList<>();

    // Data Resources
    @Option(title = "all-keyspaces",
            name = {"--all-keyspaces"},
            description = "Invalidate permissions for 'ALL KEYSPACES'")
    private boolean allKeyspaces;

    @Option(title = "keyspace",
            name = {"--keyspace"},
            description = "Keyspace to invalidate permissions for")
    private String keyspace;

    @Option(title = "all-tables",
            name = {"--all-tables"},
            description = "Invalidate permissions for 'ALL TABLES'")
    private boolean allTables;

    @Option(title = "table",
            name = {"--table"},
            description = "Table to invalidate permissions for (you must specify --keyspace for using this option)")
    private String table;

    // Roles Resources
    @Option(title = "all-roles",
            name = {"--all-roles"},
            description = "Invalidate permissions for 'ALL ROLES'")
    private boolean allRoles;

    @Option(title = "role",
            name = {"--role"},
            description = "Role to invalidate permissions for")
    private String role;

    // Functions Resources
    @Option(title = "all-functions",
            name = {"--all-functions"},
            description = "Invalidate permissions for 'ALL FUNCTIONS'")
    private boolean allFunctions;

    @Option(title = "functions-in-keyspace",
            name = {"--functions-in-keyspace"},
            description = "Keyspace to invalidate permissions for")
    private String functionsInKeyspace;

    @Option(title = "function",
            name = {"--function"},
            description = "Function to invalidate permissions for (you must specify --functions-in-keyspace for using " +
                    "this option; function format: name[arg1^..^agrN], for example: foo[Int32Type^DoubleType])")
    private String function;

    // MBeans Resources
    @Option(title = "all-mbeans",
            name = {"--all-mbeans"},
            description = "Invalidate permissions for 'ALL MBEANS'")
    private boolean allMBeans;

    @Option(title = "mbean",
            name = {"--mbean"},
            description = "MBean to invalidate permissions for")
    private String mBean;

    @Override
    public void execute(NodeProbe probe)
    {
        if (args.isEmpty())
        {
            checkArgument(!allKeyspaces && StringUtils.isEmpty(keyspace) && StringUtils.isEmpty(table)
                    && !allRoles && StringUtils.isEmpty(role)
                    && !allFunctions && StringUtils.isEmpty(functionsInKeyspace) && StringUtils.isEmpty(function)
                    && !allMBeans && StringUtils.isEmpty(mBean),
                    "No resource options allowed without a <role> being specified");

            probe.invalidatePermissionsCache();
        }
        else
        {
            checkArgument(args.size() == 1,
                    "A single <role> is only supported / you have a typo in the resource options spelling");
            List<String> resourceNames = new ArrayList<>();

            // Data Resources
            if (allKeyspaces)
                resourceNames.add(DataResource.root().getName());

            if (allTables)
                if (StringUtils.isNotEmpty(keyspace))
                    resourceNames.add(DataResource.allTables(keyspace).getName());
                else
                    throw new IllegalArgumentException("--all-tables option should be passed along with --keyspace option");

            if (StringUtils.isNotEmpty(table))
                if (StringUtils.isNotEmpty(keyspace))
                    resourceNames.add(DataResource.table(keyspace, table).getName());
                else
                    throw new IllegalArgumentException("--table option should be passed along with --keyspace option");

            if (StringUtils.isNotEmpty(keyspace) && !allTables && StringUtils.isEmpty(table))
                resourceNames.add(DataResource.keyspace(keyspace).getName());

            // Roles Resources
            if (allRoles)
                resourceNames.add(RoleResource.root().getName());

            if (StringUtils.isNotEmpty(role))
                resourceNames.add(RoleResource.role(role).getName());

            // Function Resources
            if (allFunctions)
                resourceNames.add(FunctionResource.root().getName());

            if (StringUtils.isNotEmpty(function))
                if (StringUtils.isNotEmpty(functionsInKeyspace))
                    resourceNames.add(constructFunctionResource(functionsInKeyspace, function));
                else
                    throw new IllegalArgumentException("--function option should be passed along with --functions-in-keyspace option");
            else
                if (StringUtils.isNotEmpty(functionsInKeyspace))
                    resourceNames.add(FunctionResource.keyspace(functionsInKeyspace).getName());

            // MBeans Resources
            if (allMBeans)
                resourceNames.add(JMXResource.root().getName());

            if (StringUtils.isNotEmpty(mBean))
                resourceNames.add(JMXResource.mbean(mBean).getName());

            String roleName = args.get(0);

            if (resourceNames.isEmpty())
                throw new IllegalArgumentException("No resource options specified");

            for (String resourceName : resourceNames)
                probe.invalidatePermissionsCache(roleName, resourceName);
        }
    }

    private String constructFunctionResource(String functionsInKeyspace, String function) {
        try
        {
            return FunctionResource.fromName("functions/" + functionsInKeyspace + '/' + function).getName();
        } catch (ConfigurationException e)
        {
            throw new IllegalArgumentException("An error was encountered when looking up function definition: " + e.getMessage());
        }
    }
}