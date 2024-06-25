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

@Command(name = "setuseweightbasedpreparedstatementscacheenabled", description = "Set use_weight_based_prepared_statements_cache. If disabled, " +
                                                                     "the fixed size prepared statement cache will be refreshed with max capacity set by prepared_statements_cache_max_capacity")
public class SetUseWeightBasedPreparedStatementsCacheEnabled extends NodeTool.NodeToolCmd
{
    @Arguments(title = "enabled", usage = "<true>|<false>", description = "Set use_weight_based_prepared_statements_cache", required = true)
    private String enabled;

    protected void execute(NodeProbe probe)
    {
        probe.setUseWeightBasedPreparedStatementsCacheEnabled(Boolean.parseBoolean(enabled));
    }
}
