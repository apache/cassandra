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

package org.apache.cassandra.tools.nodetool.clustermetadata;

import io.airlift.airline.Help;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;

public class ClusterMetadataHelper extends Help implements NodeTool.NodeToolCmdRunnable
{
    public ClusterMetadataHelper()
    {
        command.add("cms");
    }

    @Override
    public void run(INodeProbeFactory nodeProbeFactory, Output output)
    {
        run();
    }
}
