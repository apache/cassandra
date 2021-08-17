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
package org.apache.cassandra.cql3.selection;

import java.util.List;
import java.util.function.Predicate;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.Selector.Factory;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@code Selectable} with an alias.
 */
final class AliasedSelectable implements Selectable
{
    /**
     * The selectable
     */
    private final Selectable selectable;

    /**
     * The alias associated to the selectable.
     */
    private final ColumnIdentifier alias;

    public AliasedSelectable(Selectable selectable, ColumnIdentifier alias)
    {
        this.selectable = selectable;
        this.alias = alias;
    }

    @Override
    public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        return selectable.testAssignment(keyspace, receiver);
    }

    @Override
    public Factory newSelectorFactory(TableMetadata table,
                                      AbstractType<?> expectedType,
                                      List<ColumnMetadata> defs,
                                      VariableSpecifications boundNames)
    {
        final Factory delegate = selectable.newSelectorFactory(table, expectedType, defs, boundNames);
        final ColumnSpecification columnSpec = delegate.getColumnSpecification(table).withAlias(alias);

        return new ForwardingFactory()
        {
            @Override
            protected Factory delegate()
            {
                return delegate;
            }

            @Override
            public ColumnSpecification getColumnSpecification(TableMetadata table)
            {
                return columnSpec;
            }
        };
    }

    @Override
    public AbstractType<?> getExactTypeIfKnown(String keyspace)
    {
        return selectable.getExactTypeIfKnown(keyspace);
    }

    @Override
    public boolean selectColumns(Predicate<ColumnMetadata> predicate)
    {
        return selectable.selectColumns(predicate);
    }
}
