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

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selector.Factory;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A base <code>Selector.Factory</code> for list/set, tuples, and vectors.
 */
abstract class MultiElementFactory extends Factory
{
    /**
     * The list/set, tuples, and vectors type.
     */
    private final AbstractType<?> type;

    /**
     * The list/set, tuples, and vectors element factories.
     */
    private final SelectorFactories factories;

    public MultiElementFactory(AbstractType<?> type, SelectorFactories factories)
    {
        this.type = type;
        this.factories = factories;
    }

    protected final AbstractType<?> getReturnType()
    {
        return type;
    }

    @Override
    public final void addFunctionsTo(List<Function> functions)
    {
        factories.addFunctionsTo(functions);
    }

    @Override
    public final boolean isAggregateSelectorFactory()
    {
        return factories.doesAggregation();
    }

    @Override
    public final boolean isWritetimeSelectorFactory()
    {
        return factories.containsWritetimeSelectorFactory();
    }

    @Override
    public final boolean isTTLSelectorFactory()
    {
        return factories.containsTTLSelectorFactory();
    }

    @Override
    boolean areAllFetchedColumnsKnown()
    {
        return factories.areAllFetchedColumnsKnown();
    }

    @Override
    void addFetchedColumns(Builder builder)
    {
        factories.addFetchedColumns(builder);
    }

    protected final void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
    {
        SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
        for (Factory factory : factories)
           factory.addColumnMapping(tmpMapping, resultsColumn);

        if (tmpMapping.getMappings().get(resultsColumn).isEmpty())
            // add a null mapping for cases where the collection is empty
            mapping.addMapping(resultsColumn, (ColumnMetadata)null);
        else
            // collate the mapped columns from the child factories & add those
            mapping.addMapping(resultsColumn, tmpMapping.getMappings().values());
    }
}
