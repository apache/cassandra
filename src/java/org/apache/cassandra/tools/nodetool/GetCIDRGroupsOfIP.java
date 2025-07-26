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

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tools.nodetool.CommandUtils.printSet;

/**
 * Nodetool command to get CIDR groups(s) of given IP
 */
@Command(name = "getcidrgroupsofip", description = "Print CIDR groups associated with given IP")
public class GetCIDRGroupsOfIP extends AbstractCommand
{
    @Parameters(paramLabel = "ip_address", description = "Requires IP address as a string", arity = "1", index = "0")
    private String ipStr;

    @Override
    public void execute(NodeProbe probe)
    {
        printSet(probe.output().out, "CIDR Groups", probe.getCidrGroupsOfIp(ipStr));
    }
}
