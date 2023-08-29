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

package org.apache.cassandra.index.sai.analyzer;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class representing the name of a Lucene class and a map of arguments to pass as configuration.
 */
public class LuceneClassNameAndArgs
{
    private final String name;
    private final Map<String, String> args;

    public LuceneClassNameAndArgs(@JsonProperty("name") String name,
                                  @JsonProperty("args") Map<String, String> args)
    {
        this.name = name;
        this.args = args != null ? args : new HashMap<>();
    }

    public String getName()
    {
        return name;
    }

    public Map<String, String> getArgs()
    {
        return args;
    }
}
