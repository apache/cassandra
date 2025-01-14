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

package org.apache.cassandra.cql3.ast;

import java.util.Objects;
import java.util.Optional;

import org.apache.cassandra.schema.TableMetadata;

public class TableReference implements Element
{
    public final Optional<String> keyspace;
    public final String name;

    public TableReference(String name)
    {
        this(Optional.empty(), name);
    }

    public TableReference(Optional<String> keyspace, String name)
    {
        this.keyspace = Objects.requireNonNull(keyspace);
        this.name = Objects.requireNonNull(name);
    }

    public static TableReference from(TableMetadata metadata)
    {
        return new TableReference(Optional.of(metadata.keyspace), metadata.name);
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        if (keyspace.isPresent())
        {
            Symbol.maybeQuote(sb, keyspace.get());
            sb.append('.');
        }
        Symbol.maybeQuote(sb, name);
    }
}
