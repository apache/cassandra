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
import java.util.Map;

import org.apache.cassandra.utils.JsonUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Interface for the Stats printer, that'd output statistics
 * given the {@code StatsHolder}
 *
 * @param <T> Stats property bad type
 */
public interface StatsPrinter<T extends StatsHolder>
{
    void print(T data, PrintStream out);

    static class JsonPrinter<T extends StatsHolder> implements StatsPrinter<T>
    {
        @Override
        public void print(T data, PrintStream out)
        {
            // First need to get a Map representation of stats
            final Map<String, Object> stats = data.convert2Map();
            // but then also need slight massaging to coerce NaN values into nulls
            for (Object statEntry : stats.values())
                if (statEntry instanceof Map<?,?>)
                    for (Map.Entry<String, Object> entry : ((Map<String, Object>) statEntry).entrySet())
                        if (entry.getValue() instanceof Double && !Double.isFinite((Double) entry.getValue()))
                            entry.setValue(null);

            // and then we can serialize
            out.println(JsonUtils.writeAsPrettyJsonString(stats));
        }
    }

    static class YamlPrinter<T extends StatsHolder> implements StatsPrinter<T>
    {
        @Override
        public void print(T data, PrintStream out)
        {
            DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

            Yaml yaml = new Yaml(options);
            out.println(yaml.dump(data.convert2Map()));
        }
    }
}
