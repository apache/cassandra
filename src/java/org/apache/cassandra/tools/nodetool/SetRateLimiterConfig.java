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
import org.apache.cassandra.tools.NodeTool;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setratelimiterconfig", description = "sets the ratelimiter configuration")
public class SetRateLimiterConfig extends NodeTool.NodeToolCmd
{
    @Arguments(title = "<ratelimiterparam> <value>", usage = "<ratelimiterparam> <value>",
            description = "ratelimiter param and value.\nPossible ratelimiter parameters are as following: " +
                    "[enabled|cputhresholdcur|cputhresholdoneminute|nrthrottlingthresholdcur|" +
                    "nrthrottlingthresholdoneminute|pendingreadsthresholdcur|pendingreadsthresholdoneminute|" +
                    "pendingmutationsthresholdcur|pendingmutationsthresholdoneminute|" +
                    "percentageoftraffictothrottling|moreaggressivethrottlingafterinsec|" +
                    "resetafternothrottlingseeninsec|aggressivethrottlingqpsratio|aggressivethrottlinglatencyratio|ignorekeyspaces]",
            required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setratelimiterconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        if (paramType.equals("enabled"))
        {
            probe.setThrottlingOptionsEnabled(Boolean.parseBoolean(paramVal));
        }
        else if (paramType.equals("cputhresholdcur"))
        {
            probe.setThrottlingOptionsCpuThresholdCur(Long.parseLong(paramVal));
        }
        else if (paramType.equals("cputhresholdoneminute"))
        {
            probe.setThrottlingOptionsCpuThresholdOneMinute(Long.parseLong(paramVal));
        }
        else if (paramType.equals("nrthrottlingthresholdcur"))
        {
            probe.setThrottlingOptionsNrThrottlingThresholdCur(Long.parseLong(paramVal));
        }
        else if (paramType.equals("nrthrottlingthresholdoneminute"))
        {
            probe.setThrottlingOptionsNrThrottlingThresholdOneMinute(Long.parseLong(paramVal));
        }
        else if (paramType.equals("pendingreadsthresholdcur"))
        {
            probe.setThrottlingOptionsPendingReadsThresholdCur(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("pendingreadsthresholdoneminute"))
        {
            probe.setThrottlingOptionsPendingReadsThresholdOneMinute(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("pendingmutationsthresholdcur"))
        {
            probe.setThrottlingOptionsPendingMutationsThresholdCur(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("pendingmutationsthresholdoneminute"))
        {
            probe.setThrottlingOptionsPendingMutationsThresholdOneMinute(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("percentageoftraffictothrottling"))
        {
            probe.setThrottlingOptionsPercentageOfTrafficToThrottling(Double.parseDouble(paramVal));
        }
        else if (paramType.equals("moreaggressivethrottlingafterinsec"))
        {
            probe.setThrottlingOptionsMoreAggressiveThrottlingAfterInSec(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("resetafternothrottlingseeninsec"))
        {
            probe.setThrottlingOptionsResetAfterNoThrottlingSeenInSec(Integer.parseInt(paramVal));
        }
        else if (paramType.equals("aggressivethrottlingqpsratio"))
        {
            probe.setThrottlingOptionsAggressiveThrottlingQpsRatio(Double.parseDouble(paramVal));
        }
        else if (paramType.equals("aggressivethrottlinglatencyratio"))
        {
            probe.setThrottlingOptionsAggressiveThrottlingLatencyRatio(Double.parseDouble(paramVal));
        }
        else if (paramType.equals("ignorekeyspaces"))
        {
            probe.setIgnoreKeyspaces(paramVal);
        }
        else
        {
            System.out.println("unrecognized paramemter: " + paramType);
        }
    }
}
