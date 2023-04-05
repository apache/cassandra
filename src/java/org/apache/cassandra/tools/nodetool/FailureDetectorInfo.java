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

import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "failuredetector", description = "Shows the failure detector information for the cluster")
public class FailureDetectorInfo extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        TabularData data = probe.getFailureDetectorPhilValues(printPort);
        probe.output().out.printf("%10s,%16s%n", "Endpoint", "Phi");
        for (Object o : data.keySet())
        {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            CompositeData datum = data.get(((List) o).toArray(new Object[((List) o).size()]));
            probe.output().out.printf("%10s,%16.8f%n", datum.get("Endpoint"), datum.get("PHI"));
        }
    }
}

