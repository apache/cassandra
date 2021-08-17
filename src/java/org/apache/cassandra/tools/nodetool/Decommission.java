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

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "decommission", description = "Decommission the *node I am connecting to*")
public class Decommission extends NodeToolCmd
{

    @Option(title = "force",
    name = {"-f", "--force"},
    description = "Force decommission of this node even when it reduces the number of replicas to below configured RF")
    private boolean force = false;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            probe.decommission(force);
        } catch (InterruptedException e)
        {
            throw new RuntimeException("Error decommissioning node", e);
        } catch (UnsupportedOperationException e)
        {
            throw new IllegalStateException("Unsupported operation: " + e.getMessage(), e);
        }
    }
}