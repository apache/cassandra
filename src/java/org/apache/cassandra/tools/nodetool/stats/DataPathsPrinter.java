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
package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataPathsPrinter<T extends StatsHolder>
{
    public static StatsPrinter<DataPathsHolder> from(String format)
    {
        if ("json".equals(format))
            return new StatsPrinter.JsonPrinter<>();
        if ("yaml".equals(format))
            return new StatsPrinter.YamlPrinter<>();

        return new DefaultPrinter();
    }

    public static class DefaultPrinter implements StatsPrinter<DataPathsHolder>
    {
        @Override
        public void print(DataPathsHolder data, PrintStream out)
        {
            Iterator<Map.Entry<String, Object>> iterator = data.pathsHash.entrySet().iterator();

            while (iterator.hasNext())
            {
                Map.Entry<String, Object> entry = iterator.next();

                out.println("Keyspace: " + entry.getKey());
                Map<String, List<String>> ksPaths = (Map<String, List<String>>) entry.getValue();
                for (Map.Entry<String, List<String>> table : ksPaths.entrySet())
                {
                    out.println("\tTable: " + table.getKey());
                    out.println("\tPaths:");

                    for (String path : table.getValue())
                        out.println("\t\t" + path);

                    out.println("");
                }
            }
        }
    }
}
