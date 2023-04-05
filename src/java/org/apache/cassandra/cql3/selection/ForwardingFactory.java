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
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selector.Factory;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A <code>Selector.Factory</code> which forwards all its method calls to another factory.
 * Subclasses should override one or more methods to modify the behavior of the backing factory as desired per
 * the decorator pattern.
 */
abstract class ForwardingFactory extends Factory
{
    /**
     * Returns the backing delegate instance that methods are forwarded to.
     */
    protected abstract Factory delegate();

    public Selector newInstance(QueryOptions options) throws InvalidRequestException
    {
        return delegate().newInstance(options);
    }

    protected String getColumnName()
    {
        return delegate().getColumnName();
    }

    protected AbstractType<?> getReturnType()
    {
        return delegate().getReturnType();
    }

    protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
    {
        delegate().addColumnMapping(mapping, resultsColumn);
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        delegate().addFunctionsTo(functions);
    }

    @Override
    public boolean isAggregateSelectorFactory()
    {
        return delegate().isAggregateSelectorFactory();
    }

    @Override
    public boolean isWritetimeSelectorFactory()
    {
        return delegate().isWritetimeSelectorFactory();
    }

    @Override
    public boolean isTTLSelectorFactory()
    {
        return delegate().isTTLSelectorFactory();
    }

    @Override
    public boolean isSimpleSelectorFactory()
    {
        return delegate().isSimpleSelectorFactory();
    }

    @Override
    public boolean isSimpleSelectorFactoryFor(int index)
    {
        return delegate().isSimpleSelectorFactoryFor(index);
    }

    @Override
    boolean areAllFetchedColumnsKnown()
    {
        return delegate().areAllFetchedColumnsKnown();
    }

    @Override
    void addFetchedColumns(Builder builder)
    {
        delegate().addFetchedColumns(builder);
    }
}
