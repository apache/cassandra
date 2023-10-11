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

package org.apache.cassandra.repair.consistent.admin;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.AbstractIterator;

public class SchemaArgsParser implements Iterable<ColumnFamilyStore>
{

    private final List<String> schemaArgs;

    private SchemaArgsParser(List<String> schemaArgs)
    {
        this.schemaArgs = schemaArgs;
    }

    private static class TableIterator extends AbstractIterator<ColumnFamilyStore>
    {
        private final Iterator<ColumnFamilyStore> tables;

        public TableIterator(String ksName, List<String> tableNames)
        {
            Preconditions.checkArgument(Schema.instance.getKeyspaceMetadata(ksName) != null);
            Keyspace keyspace = Keyspace.open(ksName);

            if (tableNames.isEmpty())
            {
                tables = keyspace.getColumnFamilyStores().iterator();
            }
            else
            {
                tables = Lists.newArrayList(Iterables.transform(tableNames, tn -> keyspace.getColumnFamilyStore(tn))).iterator();
            }
        }

        @Override
        protected ColumnFamilyStore computeNext()
        {
            return tables.hasNext() ? tables.next() : endOfData();
        }
    }

    @Override
    public Iterator<ColumnFamilyStore> iterator()
    {
        if (schemaArgs.isEmpty())
        {
            // iterate over everything
            Iterator<String> ksNames = Schema.instance.distributedKeyspaces().names().iterator();

            return new AbstractIterator<ColumnFamilyStore>()
            {
                TableIterator current = null;
                protected ColumnFamilyStore computeNext()
                {
                    for (;;)
                    {
                        if (current != null && current.hasNext())
                        {
                            return current.next();
                        }

                        if (ksNames.hasNext())
                        {
                            current = new TableIterator(ksNames.next(), Collections.emptyList());
                            continue;
                        }

                        return endOfData();
                    }
                }
            };

        }
        else
        {
            return new TableIterator(schemaArgs.get(0), schemaArgs.subList(1, schemaArgs.size()));
        }
    }

    public static Iterable<ColumnFamilyStore> parse(List<String> schemaArgs)
    {
        return new SchemaArgsParser(schemaArgs);
    }

    public static Iterable<ColumnFamilyStore> parse(String... schemaArgs)
    {
        return parse(Lists.newArrayList(schemaArgs));
    }
}
