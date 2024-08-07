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

@Command(name = "setandoverridelocalcompactionstrategy", description = "Set compaction_strategy_migration_options and start " +
                                                                       "compaction strategy migration")
public class SetAndOverrideLocalCompactionStrategy extends NodeTool.NodeToolCmd
{
    @Arguments(title = "compaction_strategy_migration_options JSON string",
    usage = "<compaction_strategy_migration_options>",
    description = "JSON string for compaction_strategy_migration_options. Example: {\"compaction_params_json\":\"{\\\"class\\\":\\\"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\\\",\\\"min_threshold\\\":\\\"3\\\",\\\"max_threshold\\\":\\\"64\\\"}\",\"enabled\":true}", required = true)
    private String options = "";

    @Override
    protected void execute(NodeProbe probe)
    {
        probe.setAndOverrideLocalCompactionStrategy(options);
    }
}
