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

package org.apache.cassandra.management;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;

public class SimpleCommandExecutionArgs implements CommandExecutionArgs
{
    private final Map<OptionMetadata, Object> options;
    private final Map<ParameterMetadata, Object> parameters;

    public SimpleCommandExecutionArgs(Map<OptionMetadata, Object> options,
                                      Map<ParameterMetadata, Object> parameters)
    {
        this.options = new LinkedHashMap<>(options);
        this.parameters = new LinkedHashMap<>(parameters);
    }

    @Override
    public Map<ParameterMetadata, Object> parameters()
    {
        return parameters;
    }

    @Override
    public Map<OptionMetadata, Object> options()
    {
        return options;
    }

    @Override
    public boolean hasParameter(ParameterMetadata param)
    {
        return parameters.containsKey(param);
    }

    @Override
    public boolean hasOption(OptionMetadata option)
    {
        return options.containsKey(option);
    }

    @Override
    public String toString()
    {
        return "SimpleCommandExecutionArgs{" +
               "options=" + options +
               ", parameters=" + parameters +
               '}';
    }
}
