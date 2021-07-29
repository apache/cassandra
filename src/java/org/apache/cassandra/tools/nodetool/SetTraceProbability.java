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

import static com.google.common.base.Preconditions.checkArgument;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "settraceprobability", description = "Sets the probability for tracing any given request to value. 0 disables, 1 enables for all requests, 0 is the default")
public class SetTraceProbability extends NodeToolCmd
{
    @Arguments(title = "trace_probability", usage = "<value>", description = "Trace probability between 0 and 1 (ex: 0.2)", required = true)
    private Double traceProbability = null;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(traceProbability >= 0 && traceProbability <= 1, "Trace probability must be between 0 and 1");
        probe.setTraceProbability(traceProbability);
    }
}
