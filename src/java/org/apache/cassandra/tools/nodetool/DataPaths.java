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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.stats.DataPathsHolder;
import org.apache.cassandra.tools.nodetool.stats.DataPathsPrinter;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;

@Command(name = "datapaths", description = "Print all directories where data of tables are stored")
public class DataPaths extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace.table>...]", description = "List of table (or keyspace) names")
    private List<String> tableNames = new ArrayList<>();

    @Option(title = "format", name = {"-F", "--format"}, description = "Output format (json, yaml)")
    private String outputFormat = "";

    @Option(title = "",
            name = {"--list-orphaned"},
            description = "List directories in the data paths which are not controlled by Cassandra")
    private Boolean orphaned = false;

    @Override
    protected void execute(NodeProbe probe)
    {
        if (!outputFormat.isEmpty() && !"json".equals(outputFormat) && !"yaml".equals(outputFormat))
            throw new IllegalArgumentException("arguments for -F are yaml and json only.");

        DataPathsHolder holder = new DataPathsHolder(probe, tableNames);
        if (orphaned)
        {
            if (!outputFormat.isEmpty() || !tableNames.isEmpty())
                throw new IllegalArgumentException("Format option, keyspace and table name arguments aren't supported when listing orphans.");

            Set<String> dataDirs = new HashSet<>();
            dataDirs.addAll(Arrays.asList(probe.getStorageService().getNonLocalSystemKeyspacesDataFileLocations()));
            dataDirs.addAll(Arrays.asList(probe.getStorageService().getLocalSystemKeyspacesDataFileLocations()));
            
            Set<File> allFiles = new HashSet<>();
            for (String dataDir : dataDirs)
                for (File ksDir : new File(dataDir).listFiles())
                    if (ksDir.isDirectory())
                    {
                        for (File tableDir : ksDir.listFiles())
                            if (tableDir.isDirectory() && !holder.allPaths.contains(tableDir.toString()))
                                allFiles.add(tableDir);
                        if (ksDir.listFiles().length == 0)
                            allFiles.add(ksDir);
                    }

            for (File f : allFiles)
                output.out.println(f.toString());
        }
        else
        {
            StatsPrinter<DataPathsHolder> printer = DataPathsPrinter.from(outputFormat);
            printer.print(holder, probe.output().out);
        }
    }
}
