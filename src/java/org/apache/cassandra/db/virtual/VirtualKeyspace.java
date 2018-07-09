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
package org.apache.cassandra.db.virtual;

import java.util.Collection;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Tables;

public class VirtualKeyspace
{
    private final String name;
    private final KeyspaceMetadata metadata;

    private final ImmutableCollection<VirtualTable> tables;

    public VirtualKeyspace(String name, Collection<VirtualTable> tables)
    {
        this.name = name;
        this.tables = ImmutableList.copyOf(tables);

        metadata = KeyspaceMetadata.virtual(name, Tables.of(Iterables.transform(tables, VirtualTable::metadata)));
    }

    public String name()
    {
        return name;
    }

    public KeyspaceMetadata metadata()
    {
        return metadata;
    }

    public ImmutableCollection<VirtualTable> tables()
    {
        return tables;
    }
}
