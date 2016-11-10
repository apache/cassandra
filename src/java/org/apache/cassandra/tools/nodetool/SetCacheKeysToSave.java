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
import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "setcachekeystosave", description = "Set number of keys saved by each cache for faster post-restart warmup. 0 to disable")
public class SetCacheKeysToSave extends NodeToolCmd
{
    @Arguments(title = "<key-cache-keys-to-save> <row-cache-keys-to-save> <counter-cache-keys-to-save>",
               usage = "<key-cache-keys-to-save> <row-cache-keys-to-save> <counter-cache-keys-to-save>",
               description = "The number of keys saved by each cache. 0 to disable",
               required = true)
    private List<Integer> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3, "setcachekeystosave requires key-cache-keys-to-save, row-cache-keys-to-save, and counter-cache-keys-to-save args.");
        probe.setCacheKeysToSave(args.get(0), args.get(1), args.get(2));
    }
}