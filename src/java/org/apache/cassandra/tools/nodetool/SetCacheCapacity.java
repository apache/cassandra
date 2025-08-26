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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setcachecapacity", description = "Set global key, row, and counter cache capacities (in MB units)")
public class SetCacheCapacity extends AbstractCommand
{
    @CassandraUsage(usage = "<key-cache-capacity> <row-cache-capacity> <counter-cache-capacity>",
                    description = "Key cache, row cache, and counter cache (in MB)")
    private List<Integer> args = new ArrayList<>();

    @Parameters(paramLabel = "key-cache-capacity", description = "Key cache capacity in MB", arity = "0..1", index = "0")
    private Integer keyCacheCapacity = null;

    @Parameters(paramLabel = "row-cache-capacity", description = "Row cache capacity in MB", arity = "0..1", index = "1")
    private Integer rowCacheCapacity = null;

    @Parameters(paramLabel = "counter-cache-capacity", description = "Counter cache capacity in MB", arity = "0..1", index = "2")
    private Integer counterCacheCapacity = null;

    @Override
    public void execute(NodeProbe probe)
    {
        Optional.ofNullable(keyCacheCapacity).ifPresent(args::add);
        Optional.ofNullable(rowCacheCapacity).ifPresent(args::add);
        Optional.ofNullable(counterCacheCapacity).ifPresent(args::add);

        checkArgument(args.size() == 3, "setcachecapacity requires key-cache-capacity, row-cache-capacity, and counter-cache-capacity args.");
        probe.setCacheCapacities(args.get(0), args.get(1), args.get(2));
    }
}
