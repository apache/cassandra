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

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.snapshot.SnapshotOptions;
import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static com.google.common.collect.Iterables.toArray;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.commons.lang3.StringUtils.join;

@Command(name = "snapshot", description = "Take a snapshot of specified keyspaces or a snapshot of the specified table")
public class Snapshot extends AbstractCommand
{
    @Parameters(description = "List of keyspaces. By default, all keyspaces", arity = "0..*")
    private List<String> keyspaces = new ArrayList<>();

    @Option(paramLabel = "table", names = { "-cf", "--column-family", "--table" }, description = "The table name (you must specify one and only one keyspace for using this option)")
    private String table = null;

    @Option(paramLabel = "tag", names = { "-t", "--tag" }, description = "The name of the snapshot")
    private String snapshotName = Long.toString(currentTimeMillis());

    @Option(paramLabel = "ktlist", names = { "-kt", "--kt-list", "-kc", "--kc.list" }, description = "The list of Keyspace.table to take snapshot.(you must not specify only keyspace)")
    private String ktList = null;

    @Option(paramLabel = "skip-flush", names = { "-sf", "--skip-flush" }, description = "Do not flush memtables before snapshotting (snapshot will not contain unflushed data)")
    private boolean skipFlush = false;

    @Option(paramLabel = "ttl", names = { "--ttl" }, description = "Specify a TTL of created snapshot")
    private String ttl = null;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        try
        {
            StringBuilder sb = new StringBuilder();

            sb.append("Requested creating snapshot(s) for ");

            Map<String, String> options = new HashMap<String,String>();
            options.put(SnapshotOptions.SKIP_FLUSH, Boolean.toString(skipFlush));
            if (null != ttl) {
                DurationSpec.LongNanosecondsBound d = new DurationSpec.LongNanosecondsBound(ttl);
                options.put(SnapshotOptions.TTL, d.toString());
            }

            if (!snapshotName.isEmpty() && snapshotName.contains(File.pathSeparator()))
            {
                throw new IOException("Snapshot name cannot contain " + File.pathSeparator());
            }
            // Create a separate path for kclist to avoid breaking of already existing scripts
            if (null != ktList && !ktList.isEmpty())
            {
                ktList = ktList.replace(" ", "");
                if (keyspaces.isEmpty() && null == table)
                    sb.append('[').append(ktList).append(']');
                else
                {
                    throw new IOException(
                            "When specifying the Keyspace table list (using -kt,--kt-list,-kc,--kc.list), you must not also specify keyspaces to snapshot");
                }
                if (!snapshotName.isEmpty())
                    sb.append(" with snapshot name [").append(snapshotName).append(']');
                sb.append(" and options ").append(options);
                out.println(sb);
                probe.takeMultipleTableSnapshot(snapshotName, options, ktList.split(","));
                out.println("Snapshot directory: " + snapshotName);
            }
            else
            {
                if (keyspaces.isEmpty())
                    sb.append("[all keyspaces]");
                else
                    sb.append('[').append(join(keyspaces, ", ")).append(']');

                if (!snapshotName.isEmpty())
                    sb.append(" with snapshot name [").append(snapshotName).append(']');
                sb.append(" and options ").append(options);
                out.println(sb);

                probe.takeSnapshot(snapshotName, table, options, toArray(keyspaces, String.class));
                out.println("Snapshot directory: " + snapshotName);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error during taking a snapshot", e);
        }
    }
}
