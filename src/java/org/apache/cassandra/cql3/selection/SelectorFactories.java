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

import java.util.*;

import com.google.common.collect.Lists;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selector.Factory;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A set of {@code Selector} factories.
 */
final class SelectorFactories implements Iterable<Selector.Factory>
{
    /**
     * The {@code Selector} factories.
     */
    private final List<Selector.Factory> factories;

    /**
     * {@code true} if one of the factories creates writetime selectors.
     */
    private boolean containsWritetimeFactory;

    /**
     * {@code true} if one of the factories creates maxWritetime selectors.
     */
    private boolean containsMaxWritetimeFactory;

    /**
     * {@code true} if one of the factories creates TTL selectors.
     */
    private boolean containsTTLFactory;

    /**
     * The number of factories creating aggregates.
     */
    private int numberOfAggregateFactories;

    /**
     * Creates a new <code>SelectorFactories</code> instance and collect the column definitions.
     *
     * @param selectables the <code>Selectable</code>s for which the factories must be created
     * @param expectedTypes the returned types expected for each of the {@code selectables}, if there
     * is any such expectations, or {@code null} otherwise. This will be {@code null} when called on
     * the top-level selectables, but may not be for selectable nested within a function for instance
     * (as the argument selectable will be expected to be of the type expected by the function).
     * @param table the table Definition
     * @param defs the collector parameter for the column definitions
     * @param boundNames the collector for the specification of bound markers in the selection
     * @return a new <code>SelectorFactories</code> instance
     * @throws InvalidRequestException if a problem occurs while creating the factories
     */
    public static SelectorFactories createFactoriesAndCollectColumnDefinitions(List<Selectable> selectables,
                                                                               List<AbstractType<?>> expectedTypes,
                                                                               TableMetadata table,
                                                                               List<ColumnMetadata> defs,
                                                                               VariableSpecifications boundNames)
                                                                               throws InvalidRequestException
    {
        return new SelectorFactories(selectables, expectedTypes, table, defs, boundNames);
    }

    private SelectorFactories(List<Selectable> selectables,
                              List<AbstractType<?>> expectedTypes,
                              TableMetadata table,
                              List<ColumnMetadata> defs,
                              VariableSpecifications boundNames)
                              throws InvalidRequestException
    {
        factories = new ArrayList<>(selectables.size());

        for (int i = 0; i < selectables.size(); i++)
        {
            Selectable selectable = selectables.get(i);
            AbstractType<?> expectedType = expectedTypes == null ? null : expectedTypes.get(i);
            Factory factory = selectable.newSelectorFactory(table, expectedType, defs, boundNames);
            containsWritetimeFactory |= factory.isWritetimeSelectorFactory();
            containsTTLFactory |= factory.isTTLSelectorFactory();
            containsMaxWritetimeFactory |= factory.isMaxWritetimeSelectorFactory();
            if (factory.isAggregateSelectorFactory())
                ++numberOfAggregateFactories;
            factories.add(factory);
        }
    }

    public void addFunctionsTo(List<Function> functions)
    {
        factories.forEach(p -> p.addFunctionsTo(functions));
    }

    /**
     * Returns the factory with the specified index.
     *
     * @param i the factory index
     * @return the factory with the specified index
     */
    public Selector.Factory get(int i)
    {
        return factories.get(i);
    }

    /**
     * Returns the index of the {@code SimpleSelector.Factory} for the specified column.
     *
     * @param columnIndex the index of the column
     * @return the index of the {@code SimpleSelector.Factory} for the specified column or -1 if it does not exist.
     */
    public int indexOfSimpleSelectorFactory(int columnIndex)
    {
        for (int i = 0, m = factories.size(); i < m; i++)
        {
            if (factories.get(i).isSimpleSelectorFactoryFor(columnIndex))
                return i;
        }
        return -1;
    }

    /**
     * Adds a new <code>Selector.Factory</code> for a column that is needed only for ORDER BY purposes.
     * @param def the column that is needed for ordering
     * @param index the index of the column definition in the Selection's list of columns
     */
    public void addSelectorForOrdering(ColumnMetadata def, int index)
    {
        factories.add(SimpleSelector.newFactory(def, index, true));
    }

    /**
     * Whether the selector built by this factory does aggregation or not (either directly or in a sub-selector).
     *
     * @return <code>true</code> if the selector built by this factor does aggregation, <code>false</code> otherwise.
     */
    public boolean doesAggregation()
    {
        return numberOfAggregateFactories > 0;
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains at least one factory for writetime selectors.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains at least one factory for writetime
     * selectors, <code>false</code> otherwise.
     */
    public boolean containsWritetimeSelectorFactory()
    {
        return containsWritetimeFactory;
    }

    /**
     * Checks if this {@code SelectorFactories} contains at least one factory for maxWritetime selectors.
     *
     * @return {@link true} if this {@link SelectorFactories} contains at least one factory for maxWritetime
     * selectors, {@link false} otherwise.
     */
    public boolean containsMaxWritetimeSelectorFactory()
    {
        return containsMaxWritetimeFactory;
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains at least one factory for TTL selectors.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains at least one factory for TTL
     * selectors, <code>false</code> otherwise.
     */
    public boolean containsTTLSelectorFactory()
    {
        return containsTTLFactory;
    }

    /**
     * Creates a list of new <code>Selector</code> instances.
     *
     * @param options the query options for the query being executed.
     * @return a list of new <code>Selector</code> instances.
     */
    public List<Selector> newInstances(QueryOptions options) throws InvalidRequestException
    {
        List<Selector> selectors = new ArrayList<>(factories.size());
        for (Selector.Factory factory : factories)
            selectors.add(factory.newInstance(options));
        return selectors;
    }

    public Iterator<Factory> iterator()
    {
        return factories.iterator();
    }

    /**
     * Returns the names of the columns corresponding to the output values of the selector instances created by
     * these factories.
     *
     * @return a list of column names
     */
    public List<String> getColumnNames()
    {
        return Lists.transform(factories, new com.google.common.base.Function<Selector.Factory, String>()
        {
            public String apply(Selector.Factory factory)
            {
                return factory.getColumnName();
            }
        });
    }

    /**
     * Returns a list of the return types of the selector instances created by these factories.
     *
     * @return a list of types
     */
    public List<AbstractType<?>> getReturnTypes()
    {
        return Lists.transform(factories, new com.google.common.base.Function<Selector.Factory, AbstractType<?>>()
        {
            public AbstractType<?> apply(Selector.Factory factory)
            {
                return factory.getReturnType();
            }
        });
    }

    boolean areAllFetchedColumnsKnown()
    {
        for (Factory factory : factories)
        {
            if (!factory.areAllFetchedColumnsKnown())
                return false;
        }
        return true;
    }

    void addFetchedColumns(Builder builder)
    {
        for (Factory factory : factories)
            factory.addFetchedColumns(builder);
    }

    /**
     * Returns the number of factories.
     * @return the number of factories
     */
    public int size()
    {
        return factories.size();
    }
}
