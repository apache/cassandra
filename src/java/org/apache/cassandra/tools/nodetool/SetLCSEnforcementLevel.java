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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setlcsenforcementlevel", description = "Set LCS enforcement level, value in hard/soft/none")
public class SetLCSEnforcementLevel extends NodeTool.NodeToolCmd
{
    @Arguments(title = "<lcsenforcementlevel>", usage = "<lcsenforcementlevel>", description = "lcs_enforcement_level value in hard/soft/none", required = true)
    private String lcsEnforcementLevel;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(Arrays.stream(Config.LCSEnforcementLevel.values()).anyMatch((l) -> l.name().equals(lcsEnforcementLevel)),
                      "value should be in hard/soft/none.");
        probe.setLCSEnforcementLevel(Config.LCSEnforcementLevel.valueOf(lcsEnforcementLevel));
    }
}
