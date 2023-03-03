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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.config.Config.AuthEnforcementFlag;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;


@Command(name = "setauthenforcementflag", description = "Sets the auth enforcement flag")
public class SetAuthEnforcementFlag extends NodeToolCmd
{
    @Arguments(title = "<authenforcementflagvalue>", usage = "<authenforcementflagvalue>", description = "auth_enforcement_flag value in hard/soft/none", required = true)
    private String authEnforcementFlagValue;
    private final List<String> allowedList = Stream.of(AuthEnforcementFlag.values())
                                                   .map(Enum::name)
                                                   .collect(Collectors.toList());

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(allowedList.contains(authEnforcementFlagValue), "value should be in hard/soft/none.");
        probe.setAuthEnforcementFlag(AuthEnforcementFlag.valueOf(authEnforcementFlagValue));
    }
}
