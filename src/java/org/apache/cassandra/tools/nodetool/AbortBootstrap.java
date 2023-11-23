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

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;

@Command(name = "abortbootstrap", description = "Abort a failed bootstrap")
public class AbortBootstrap extends NodeToolCmd
{
    @Option(title = "node id", name = "--node", description = "Node ID of the node that failed bootstrap", required = false)
    private String nodeId = EMPTY;

    @Option(title = "ip", name = "--ip", description = "IP of the node that failed bootstrap", required = false)
    private String endpoint = EMPTY;


    @Override
    public void execute(NodeProbe probe)
    {
        if (isEmpty(nodeId) && isEmpty(endpoint))
            throw new IllegalArgumentException("Either --node or --ip needs to be set");
        if (!isEmpty(nodeId) && !isEmpty(endpoint))
            throw new IllegalArgumentException("Only one of --node or --ip need to be set");
        probe.abortBootstrap(nodeId, endpoint);
    }
}
