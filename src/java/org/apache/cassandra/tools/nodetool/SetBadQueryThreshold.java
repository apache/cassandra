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

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setbadquerythreshold", description = "Sets the badquery threshold")
public class SetBadQueryThreshold extends NodeToolCmd
{
    @Arguments(title = "<badquerythresholdtype> <thresholdvalue>", usage = "<badquerythresholdtype> <thresholdvalue>", description = "badquery threshold type and value.\nPossible badquery threshold types are as following: [badquerytracingfraction|badquerytracingfraction|badqueryreadmaxpartitionsizeinbytes|badquerywritemaxpartitionsizeinbytes|badqueryreadslowlocallatencyinms|badquerywriteslowlocallatencyinms|badqueryreadslowcoordlatencyinms|badquerywriteslowcoordlatencyinms|badquerytombstonelimit|badqueryignorekeyspaces]", required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setbadquerythreshold requires threshold-type, and value args.");
        String thresholdType = args.get(0);
        String thresholdValue = args.get(1);

        if (!probe.isBadQueryTracingEnabled())
        {
            System.out.println("badquery tracing is disabled");
            return;
        }

        if (thresholdType.equals("badquerymaxsamplesinsyslog"))
        {
            probe.setBadQueryMaxSamplesInSyslog(Integer.parseInt(thresholdValue));
        }
        else if (thresholdType.equals("badquerytracingfraction"))
        {
            probe.setBadQueryTracingFraction(Double.parseDouble(thresholdValue));
        }
        else if (thresholdType.equals("badqueryreadmaxpartitionsizeinbytes"))
        {
            probe.setBadQueryReadMaxPartitionSizeInbytes(Long.parseLong(thresholdValue));
        }
        else if (thresholdType.equals("badquerywritemaxpartitionsizeinbytes"))
        {
            probe.setBadQueryWriteMaxPartitionSizeInbytes(Long.parseLong(thresholdValue));
        }
        else if (thresholdType.equals("badqueryreadslowlocallatencyinms"))
        {
            probe.setBadQueryReadSlowLocalLatencyInms(Integer.parseInt(thresholdValue));
        }
        else if (thresholdType.equals("badquerywriteslowlocallatencyinms"))
        {
            probe.setBadQueryWriteSlowLocalLatencyInms(Integer.parseInt(thresholdValue));
        }
        else if (thresholdType.equals("badqueryreadslowcoordlatencyinms"))
        {
            probe.setBadQueryReadSlowCoordLatencyInms(Integer.parseInt(thresholdValue));
        }
        else if (thresholdType.equals("badquerywriteslowcoordlatencyinms"))
        {
            probe.setBadQueryWriteSlowCoordLatencyInms(Integer.parseInt(thresholdValue));
        }
        else if (thresholdType.equals("badquerytombstonelimit"))
        {
            probe.setBadQueryTombstoneLimit(Integer.parseInt(thresholdValue));
        }
        else if (thresholdType.equals("badqueryignorekeyspaces"))
        {
            probe.setBadQueryIgnoreKeyspaces(thresholdValue);
        }
    }
}
