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

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "setcompactionthreshold", description = "Set min and max compaction thresholds for a given table")
public class SetCompactionThreshold extends AbstractCommand
{
    @CassandraUsage(usage = "<keyspace> <table> <minthreshold> <maxthreshold>", description = "The keyspace, the table, min and max threshold")
    private List<String> args = new ArrayList<>();

    @Parameters(paramLabel = "keyspace", description = "The keyspace name", arity = "0..1", index = "0")
    private String keyspace;

    @Parameters(paramLabel = "table", description = "The table name", arity = "0..1", index = "1")
    private String table;

    @Parameters(paramLabel = "minthreshold", description = "The min threshold", arity = "0..1", index = "2")
    private String minthreshold;

    @Parameters(paramLabel = "maxthreshold", description = "The max threshold", arity = "0..1", index = "3")
    private String maxthreshold;

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, table, minthreshold, maxthreshold);

        checkArgument(args.size() == 4, "setcompactionthreshold requires ks, cf, min, and max threshold args.");

        int minthreshold = parseInt(args.get(2));
        int maxthreshold = parseInt(args.get(3));
        checkArgument(minthreshold >= 0 && maxthreshold >= 0, "Thresholds must be positive integers");
        checkArgument(minthreshold <= maxthreshold, "Min threshold cannot be greater than max.");
        checkArgument(minthreshold >= 2 || maxthreshold == 0, "Min threshold must be at least 2");

        probe.setCompactionThreshold(args.get(0), args.get(1), minthreshold, maxthreshold);
    }
}
