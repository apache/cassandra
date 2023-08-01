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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import org.apache.commons.lang3.text.StrBuilder;

import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.FunctionResolver;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.PartialScalarFunction;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static java.util.stream.Collectors.joining;

abstract class AbstractFunctionSelector<T extends Function> extends Selector
{
    protected static abstract class AbstractFunctionSelectorDeserializer extends SelectorDeserializer
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            // The selector will only be deserialized on the replicas for GROUP BY queries. Due to that we
            // can safely use the current protocol version.
            final ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;

            FunctionName name = new FunctionName(in.readUTF(), in.readUTF());

            int numberOfArguments = in.readUnsignedVInt32();
            List<AbstractType<?>> argTypes = new ArrayList<>(numberOfArguments);
            for (int i = 0; i < numberOfArguments; i++)
            {
                argTypes.add(readType(metadata, in));
            }

            Function function = FunctionResolver.get(metadata.keyspace, name, argTypes, metadata.keyspace, metadata.name, null);

            if (function == null)
                throw new IOException(String.format("Unknown serialized function %s(%s)",
                                                    name,
                                                    argTypes.stream()
                                                            .map(p -> p.asCQL3Type().toString())
                                                            .collect(joining(", "))));

            boolean isPartial = in.readBoolean();
            // if the function is partial we need to retrieve the resolved arguments.
            // The resolved arguments are encoded as follow: [vint]([vint][bytes])*
            // The first vint contains the bitset used to determine which arguments were resolved.
            // A bit equals to one meaning a resolved argument.
            // The arguments are encoded as [vint][bytes] where the vint contains the size in bytes of the
            // argument.
            if (isPartial)
            {
                // We use a bitset to track the position of the unresolved arguments
                int bitset = in.readUnsignedVInt32();
                List<ByteBuffer> partialArguments = new ArrayList<>(numberOfArguments);
                for (int i = 0; i < numberOfArguments; i++)
                {
                    boolean isArgumentResolved = getRightMostBit(bitset) == 1;
                    ByteBuffer argument = isArgumentResolved ? ByteBufferUtil.readWithVIntLength(in)
                                                             : Function.UNRESOLVED;
                    partialArguments.add(argument);
                    bitset >>= 1;
                }

                function = ((ScalarFunction) function).partialApplication(protocolVersion, partialArguments);
            }

            int numberOfRemainingArguments = in.readUnsignedVInt32();
            List<Selector> argSelectors = new ArrayList<>(numberOfRemainingArguments);
            for (int i = 0; i < numberOfRemainingArguments; i++)
            {
                argSelectors.add(Selector.serializer.deserialize(in, version, metadata));
            }

            return newFunctionSelector(protocolVersion, function, argSelectors);
        }

        /**
         * Returns the value of the right most bit.
         * @param bitset the bitset
         * @return the value of the right most bit
         */
        private int getRightMostBit(int bitset)
        {
            return bitset & 1;
        }

        protected abstract Selector newFunctionSelector(ProtocolVersion version,
                                                        Function function,
                                                        List<Selector> argSelectors);
    }

    protected final T fun;

    /**
     * The list used to pass the function arguments is recycled to avoid the cost of instantiating a new list
     * with each function call.
     */
    private final Arguments args;
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
                return fun.isAggregate() ? new AggregateFunctionSelector(options.getProtocolVersion(), fun, factories.newInstances(options))
                                         : createScalarSelector(options, (ScalarFunction) fun, factories.newInstances(options));
            }

            private Selector createScalarSelector(QueryOptions options, ScalarFunction function, List<Selector> argSelectors)
            {
                ProtocolVersion version = options.getProtocolVersion();
                int terminalCount = 0;
                List<ByteBuffer> terminalArgs = new ArrayList<>(argSelectors.size());
                for (Selector selector : argSelectors)
                {
                    if (selector.isTerminal())
                    {
                        ++terminalCount;
                        ByteBuffer output = selector.getOutput(version);
                        RequestValidations.checkBindValueSet(output, "Invalid unset value for argument in call to function %s", fun.name().name);
                        terminalArgs.add(output);
                    }
                    else
                    {
                        terminalArgs.add(Function.UNRESOLVED);
                    }
                }

                if (terminalCount == 0)
                    return new ScalarFunctionSelector(version, fun, argSelectors);

                // We have some terminal arguments, do a partial application
                ScalarFunction partialFunction = function.partialApplication(version, terminalArgs);

                // If all the arguments are terminal and the function is pure we can reduce to a simple value.
                if (terminalCount == argSelectors.size() && fun.isPure())
                {
                    Arguments arguments = partialFunction.newArguments(version);
                    return new TermSelector(partialFunction.execute(arguments), partialFunction.returnType());
                }

                List<Selector> remainingSelectors = new ArrayList<>(argSelectors.size() - terminalCount);
                for (Selector selector : argSelectors)
                {
                    if (!selector.isTerminal())
                        remainingSelectors.add(selector);
                }
                return new ScalarFunctionSelector(version, partialFunction, remainingSelectors);
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

            @Override
            public boolean areAllFetchedColumnsKnown()
            {
                return Iterables.all(factories, f -> f.areAllFetchedColumnsKnown());
            }

            @Override
            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                for (Selector.Factory factory : factories)
                    factory.addFetchedColumns(builder);
            }
        };
    }

    protected AbstractFunctionSelector(Kind kind, ProtocolVersion version, T fun, List<Selector> argSelectors)
    {
        super(kind);
        this.fun = fun;
        this.argSelectors = argSelectors;
        this.args = fun.newArguments(version);
    }

    @Override
    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        for (Selector selector : argSelectors)
            selector.addFetchedColumns(builder);
    }

    // Sets a given arg value. We should use that instead of directly setting the args list for the
    // sake of validation.
    protected void setArg(int i, ByteBuffer value) throws InvalidRequestException
    {
        RequestValidations.checkBindValueSet(value, "Invalid unset value for argument in call to function %s", fun.name().name);
        args.set(i, value);
    }

    protected Arguments args()
    {
        return args;
    }

    public AbstractType<?> getType()
    {
        return fun.returnType();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof AbstractFunctionSelector))
            return false;

        AbstractFunctionSelector<?> s = (AbstractFunctionSelector<?>) o;

        return Objects.equal(fun.name(), s.fun.name())
            && Objects.equal(fun.argTypes(), s.fun.argTypes())
            && Objects.equal(argSelectors, s.argSelectors);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(fun.name(), fun.argTypes(), argSelectors);
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

    @Override
    protected int serializedSize(int version)
    {
        boolean isPartial = fun instanceof PartialScalarFunction;
        Function function = isPartial ? ((PartialScalarFunction) fun).getFunction() : fun;

        FunctionName name = function.name();
        int size =  TypeSizes.sizeof(name.keyspace) + TypeSizes.sizeof(name.name);

        List<AbstractType<?>> argTypes = function.argTypes();
        size += TypeSizes.sizeofUnsignedVInt(argTypes.size());
        for (int i = 0, m = argTypes.size(); i < m; i++)
        {
            size += sizeOf(argTypes.get(i));
        }

        size += TypeSizes.sizeof(isPartial);

        if (isPartial)
        {
            List<ByteBuffer> partialArguments = ((PartialScalarFunction) fun).getPartialArguments();

            // We use a bitset to track the position of the unresolved arguments
            size += TypeSizes.sizeofUnsignedVInt(computeBitSet(partialArguments));

            for (int i = 0, m = partialArguments.size(); i < m; i++)
            {
                ByteBuffer buffer = partialArguments.get(i);
                if (buffer != Function.UNRESOLVED)
                    size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);
            }
        }

        int numberOfRemainingArguments = argSelectors.size();
        size += TypeSizes.sizeofUnsignedVInt(numberOfRemainingArguments);
        for (int i = 0; i < numberOfRemainingArguments; i++)
            size += serializer.serializedSize(argSelectors.get(i), version);

        return size;
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        boolean isPartial = fun instanceof PartialScalarFunction;
        Function function = isPartial ? ((PartialScalarFunction) fun).getFunction() : fun;

        FunctionName name = function.name();
        out.writeUTF(name.keyspace);
        out.writeUTF(name.name);

        List<AbstractType<?>> argTypes = function.argTypes();
        int numberOfArguments = argTypes.size();
        out.writeUnsignedVInt32(numberOfArguments);

        for (int i = 0; i < numberOfArguments; i++)
            writeType(out, argTypes.get(i));

        out.writeBoolean(isPartial);

        if (isPartial)
        {
            List<ByteBuffer> partialArguments = ((PartialScalarFunction) fun).getPartialArguments();

            // We use a bitset to track the position of the unresolved arguments
            out.writeUnsignedVInt32(computeBitSet(partialArguments));

            for (int i = 0, m = partialArguments.size(); i < m; i++)
            {
                ByteBuffer buffer = partialArguments.get(i);
                if (buffer != Function.UNRESOLVED)
                    ByteBufferUtil.writeWithVIntLength(buffer, out);
            }
        }

        int numberOfRemainingArguments = argSelectors.size();
        out.writeUnsignedVInt32(numberOfRemainingArguments);
        for (int i = 0; i < numberOfRemainingArguments; i++)
            serializer.serialize(argSelectors.get(i), out, version);
    }

    private int computeBitSet(List<ByteBuffer> partialArguments)
    {
        assert partialArguments.size() <= 32 : "cannot serialize partial function with more than 32 arguments";
        int bitset = 0;
        for (int i = 0, m = partialArguments.size(); i < m; i++)
        {
            if (partialArguments.get(i) != Function.UNRESOLVED)
                bitset |= 1 << i;
        }
        return bitset;
    }
}
