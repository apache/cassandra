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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

abstract class AbstractFunctionSelector<T extends Function> extends Selector
{
    protected final T fun;

    /**
     * The list used to pass the function arguments is recycled to avoid the cost of instantiating a new list
     * with each function call.
     */
    private final List<ByteBuffer> args;
    protected final List<Selector> argSelectors;

    public static Factory newFactory(final Function fun, final SelectorFactories factories) throws InvalidRequestException
    {
        if (fun.isAggregate())
        {
            if (factories.doesAggregation())
                throw new InvalidRequestException("aggregate functions cannot be used as arguments of aggregate functions");
        }

        return new Factory()
        {
            protected String getColumnName()
            {
                return fun.columnName(factories.getColumnNames());
            }

            protected AbstractType<?> getReturnType()
            {
                return fun.returnType();
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
                for (Factory factory : factories)
                   factory.addColumnMapping(tmpMapping, resultsColumn);

                if (tmpMapping.getMappings().get(resultsColumn).isEmpty())
                    // add a null mapping for cases where there are no
                    // further selectors, such as no-arg functions and count
                    mapping.addMapping(resultsColumn, (ColumnMetadata)null);
                else
                    // collate the mapped columns from the child factories & add those
                    mapping.addMapping(resultsColumn, tmpMapping.getMappings().values());
            }

            public void addFunctionsTo(List<Function> functions)
            {
                fun.addFunctionsTo(functions);
                factories.addFunctionsTo(functions);
            }

            public Selector newInstance(QueryOptions options) throws InvalidRequestException
            {
                return fun.isAggregate() ? new AggregateFunctionSelector(fun, factories.newInstances(options))
                                         : new ScalarFunctionSelector(fun, factories.newInstances(options));
            }

            public boolean isWritetimeSelectorFactory()
            {
                return factories.containsWritetimeSelectorFactory();
            }

            public boolean isTTLSelectorFactory()
            {
                return factories.containsTTLSelectorFactory();
            }

            public boolean isAggregateSelectorFactory()
            {
                return fun.isAggregate() || factories.doesAggregation();
            }
        };
    }

    protected AbstractFunctionSelector(T fun, List<Selector> argSelectors)
    {
        this.fun = fun;
        this.argSelectors = argSelectors;
        this.args = Arrays.asList(new ByteBuffer[argSelectors.size()]);
    }

    // Sets a given arg value. We should use that instead of directly setting the args list for the
    // sake of validation.
    protected void setArg(int i, ByteBuffer value) throws InvalidRequestException
    {
        RequestValidations.checkBindValueSet(value, "Invalid unset value for argument in call to function %s", fun.name().name);
        args.set(i, value);
    }

    protected List<ByteBuffer> args()
    {
        return args;
    }

    public AbstractType<?> getType()
    {
        return fun.returnType();
    }

    @Override
    public String toString()
    {
        return new StrBuilder().append(fun.name())
                               .append("(")
                               .appendWithSeparators(argSelectors, ", ")
                               .append(")")
                               .toString();
    }
}
