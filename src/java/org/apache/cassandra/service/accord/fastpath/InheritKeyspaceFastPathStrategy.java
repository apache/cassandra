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

package org.apache.cassandra.service.accord.fastpath;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import accord.local.Node;

public class InheritKeyspaceFastPathStrategy implements FastPathStrategy
{
    static final FastPathStrategy instance = new InheritKeyspaceFastPathStrategy();

    private static final Map<String, String> SCHEMA_PARAMS = ImmutableMap.of(Kind.KEY, Kind.INHERIT_KEYSPACE.name());

    private InheritKeyspaceFastPathStrategy() {}

    @Override
    public Set<Node.Id> calculateFastPath(List<Node.Id> nodes, Set<Node.Id> unavailable, Map<Node.Id, String> dcMap)
    {
        throw new IllegalStateException("InheritKeyspaceFastPathStrategy should be replaced before calculateFastPath is called");
    }

    @Override
    public Kind kind()
    {
        return Kind.INHERIT_KEYSPACE;
    }

    @Override
    public String toString()
    {
        return "keyspace";
    }

    public Map<String, String> asMap()
    {
        return SCHEMA_PARAMS;
    }

    @Override
    public String asCQL()
    {
        return "'keyspace'";
    }
}
