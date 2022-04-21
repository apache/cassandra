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
            Map<String, List<String>> ksPaths = null;

            for (String keyspace : data.pathsHash.keySet())
            {
                if (ksPaths != null)
                    out.println("----------------");

                out.println("Keyspace : " + keyspace);
                ksPaths = (Map<String, List<String>>) data.pathsHash.get(keyspace);
                for (String table : ksPaths.keySet())
                {
                    out.println("\tTable : " + table);
                    out.println("\tPaths :");
                    for (String path : ksPaths.get(table))
                        out.println("\t\t" + path);

                    out.println("");
                }
            }
        }
    }
}
