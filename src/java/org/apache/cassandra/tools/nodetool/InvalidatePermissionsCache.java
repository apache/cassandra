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

import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "invalidatepermissionscache", description = "Invalidate the permissions cache")
public class InvalidatePermissionsCache extends AbstractCommand
{
    @Parameters(paramLabel = "role", description = "A role for which permissions to specified resources need to be invalidated", arity = "0..1", index = "0")
    private String roleName;

    // Data Resources
    @Option(paramLabel = "all-keyspaces",
            names = { "--all-keyspaces" },
            description = "Invalidate permissions for 'ALL KEYSPACES'")
    private boolean allKeyspaces;

    @Option(paramLabel = "keyspace",
            names = { "--keyspace" },
            description = "Keyspace to invalidate permissions for")
    private String keyspace;

    @Option(paramLabel = "all-tables",
            names = { "--all-tables" },
            description = "Invalidate permissions for 'ALL TABLES'")
    private boolean allTables;

    @Option(paramLabel = "table",
            names = { "--table" },
            description = "Table to invalidate permissions for (you must specify --keyspace for using this option)")
    private String table;

    // Roles Resources
    @Option(paramLabel = "all-roles",
            names = { "--all-roles" },
            description = "Invalidate permissions for 'ALL ROLES'")
    private boolean allRoles;

    @Option(paramLabel = "role",
            names = { "--role" },
            description = "Role to invalidate permissions for")
    private String role;

    // Functions Resources
    @Option(paramLabel = "all-functions",
            names = { "--all-functions" },
            description = "Invalidate permissions for 'ALL FUNCTIONS'")
    private boolean allFunctions;

    @Option(paramLabel = "functions-in-keyspace",
            names = { "--functions-in-keyspace" },
            description = "Keyspace to invalidate permissions for")
    private String functionsInKeyspace;

    @Option(paramLabel = "function",
            names = { "--function" },
            description = "Function to invalidate permissions for (you must specify --functions-in-keyspace for using " +
                          "this option; function format: name[arg1^..^agrN], for example: foo[Int32Type^DoubleType])")
    private String function;

    // MBeans Resources
    @Option(paramLabel = "all-mbeans",
            names = { "--all-mbeans" },
            description = "Invalidate permissions for 'ALL MBEANS'")
    private boolean allMBeans;

    @Option(paramLabel = "mbean",
            names = { "--mbean" },
            description = "MBean to invalidate permissions for")
    private String mBean;

    @Override
    public void execute(NodeProbe probe)
    {
        if (StringUtils.isEmpty(roleName))
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