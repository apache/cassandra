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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static com.google.common.collect.Iterables.toArray;

public class CompactionHistoryPrinter
{
    public static StatsPrinter from(String format)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter();
            case "yaml":
                return new StatsPrinter.YamlPrinter();
            default:
                return new DefaultPrinter();
        }

    }

    public static class DefaultPrinter implements StatsPrinter<CompactionHistoryHolder>
    {
        @Override
        public void print(CompactionHistoryHolder data, PrintStream out)
        {

            out.println("Compaction History: ");
            Map<String, Object> convertData = data.convert2Map();
            List<Object> compactionHistories = convertData.get("CompactionHistory") instanceof List<?> ? (List)convertData.get("CompactionHistory") : Collections.emptyList();
            List<String> indexNames = data.indexNames;

            if (compactionHistories.size() == 0) {
                out.printf("There is no compaction history");
                return;
            }

            TableBuilder table = new TableBuilder();

            table.add(toArray(indexNames, String.class));
            for (Object chr : compactionHistories)
            {
                Map value = chr instanceof Map<?, ?> ? (Map)chr : Collections.emptyMap();
                String[] obj = new String[7];
                obj[0] = (String)value.get("id");
                obj[1] = (String)value.get("keyspace_name");
                obj[2] = (String)value.get("columnfamily_name");
                obj[3] = (String)value.get("compacted_at");
                obj[4] = value.get("bytes_in").toString();
                obj[5] = value.get("bytes_out").toString();
                obj[6] = (String)value.get("rows_merged");
                table.add(obj);
            }
            table.printTo(out);
        }
    }
}
