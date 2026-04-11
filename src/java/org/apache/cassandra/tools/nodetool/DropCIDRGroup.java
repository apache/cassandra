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

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Nodetool command to drop a CIDR group and associated mapping from the table {@link AuthKeyspace#CIDR_GROUPS}
 */
@Command(name = "dropcidrgroup", description = "Drop an existing cidr group")
public class DropCIDRGroup extends AbstractCommand
{
    @Parameters(paramLabel = "cidrGroup", description = "Requires a cidr group name", index = "0", arity = "1")
    private String cidrGroup;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.dropCidrGroup(cidrGroup);

        probe.output().out.println("Deleted CIDR group " + cidrGroup);
    }
}
