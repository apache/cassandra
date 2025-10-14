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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setmaxwaittimeintransportqueue",
        description = "Set the maximum wait time in transport queue in milliseconds")
public class SetMaxWaitTimeInTransportQueue extends NodeToolCmd
{
    @VisibleForTesting
    @Arguments(title = "<value>", usage = "<value>",
            description = "Set the max wait time in transport queue in milliseconds",
            required = true)
    protected List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 1, "setmaxwaittimeintransportqueue requires value.");
        probe.setMaxWaitTimeInTransportQueueMillis(Long.parseLong(args.get(0)));
    }
}
