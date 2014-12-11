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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;

/**
 * Base class for user-defined-aggregates.
 */
public class UDAggregate extends AbstractFunction implements AggregateFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDAggregate.class);

    protected final AbstractType<?> stateType;
    protected final ByteBuffer initcond;
    private final ScalarFunction stateFunction;
    private final ScalarFunction finalFunction;

    public UDAggregate(FunctionName name,
                       List<AbstractType<?>> argTypes,
                       AbstractType<?> returnType,
                       ScalarFunction stateFunc,
                       ScalarFunction finalFunc,
                       ByteBuffer initcond)
    {
        super(name, argTypes, returnType);
        this.stateFunction = stateFunc;
        this.finalFunction = finalFunc;
        this.stateType = stateFunc != null ? stateFunc.returnType() : null;
        this.initcond = initcond;
    }

    public boolean hasReferenceTo(Function function)
    {
        return stateFunction == function || finalFunction == function;
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        return super.usesFunction(ksName, functionName)
            || stateFunction != null && stateFunction.name().keyspace.equals(ksName) && stateFunction.name().name.equals(functionName)
            || finalFunction != null && finalFunction.name().keyspace.equals(ksName) && finalFunction.name().name.equals(functionName);
    }

    public boolean isAggregate()
    {
        return true;
    }

    public boolean isPure()
    {
        return false;
    }

    public boolean isNative()
    {
        return false;
    }

    public Aggregate newAggregate() throws InvalidRequestException
    {
        return new Aggregate()
        {
            private ByteBuffer state;
            {
                reset();
            }

            public void addInput(int protocolVersion, List<ByteBuffer> values) throws InvalidRequestException
            {
                List<ByteBuffer> copy = new ArrayList<>(values.size() + 1);
                copy.add(state);
                copy.addAll(values);
                state = stateFunction.execute(protocolVersion, copy);
            }

            public ByteBuffer compute(int protocolVersion) throws InvalidRequestException
            {
                if (finalFunction == null)
                    return state;
                return finalFunction.execute(protocolVersion, Collections.singletonList(state));
            }

            public void reset()
            {
                state = initcond != null ? initcond.duplicate() : null;
            }
        };
    }

    private static ScalarFunction resolveScalar(FunctionName aName, FunctionName fName, List<AbstractType<?>> argTypes) throws InvalidRequestException
    {
        Function func = Functions.find(fName, argTypes);
        if (func == null)
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' does not exist",
                                                            fName, Arrays.toString(UDHelper.driverTypes(argTypes)), aName));
        if (!(func instanceof ScalarFunction))
            throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' is not a scalar function",
                                                            fName, Arrays.toString(UDHelper.driverTypes(argTypes)), aName));
        return (ScalarFunction) func;
    }

    private static Mutation makeSchemaMutation(FunctionName name)
    {
        UTF8Type kv = (UTF8Type)SystemKeyspace.SchemaAggregatesTable.getKeyValidator();
        return new Mutation(SystemKeyspace.NAME, kv.decompose(name.keyspace));
    }

    public Mutation toSchemaDrop(long timestamp)
    {
        Mutation mutation = makeSchemaMutation(name);
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_AGGREGATES_TABLE);

        Composite prefix = SystemKeyspace.SchemaAggregatesTable.comparator.make(name.name, UDHelper.computeSignature(argTypes));
        int ldt = (int) (System.currentTimeMillis() / 1000);
        cf.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        return mutation;
    }

    public static Map<Composite, UDAggregate> fromSchema(Row row)
    {
        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system." + SystemKeyspace.SCHEMA_AGGREGATES_TABLE, row);
        Map<Composite, UDAggregate> udfs = new HashMap<>(results.size());
        for (UntypedResultSet.Row result : results)
            udfs.put(SystemKeyspace.SchemaAggregatesTable.comparator.make(result.getString("aggregate_name"), result.getBlob("signature")),
                     fromSchema(result));
        return udfs;
    }

    public Mutation toSchemaUpdate(long timestamp)
    {
        Mutation mutation = makeSchemaMutation(name);
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_AGGREGATES_TABLE);

        Composite prefix = SystemKeyspace.SchemaAggregatesTable.comparator.make(name.name, UDHelper.computeSignature(argTypes));
        CFRowAdder adder = new CFRowAdder(cf, prefix, timestamp);

        adder.resetCollection("argument_types");
        adder.add("return_type", returnType.toString());
        adder.add("state_func", stateFunction.name().name);
        if (stateType != null)
            adder.add("state_type", stateType.toString());
        if (finalFunction != null)
            adder.add("final_func", finalFunction.name().name);
        if (initcond != null)
            adder.add("initcond", initcond);

        for (AbstractType<?> argType : argTypes)
            adder.addListEntry("argument_types", argType.toString());

        return mutation;
    }

    public static UDAggregate fromSchema(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("aggregate_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<String> types = row.getList("argument_types", UTF8Type.instance);

        List<AbstractType<?>> argTypes;
        if (types == null)
        {
            argTypes = Collections.emptyList();
        }
        else
        {
            argTypes = new ArrayList<>(types.size());
            for (String type : types)
                argTypes.add(parseType(type));
        }

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        FunctionName stateFunc = new FunctionName(ksName, row.getString("state_func"));
        FunctionName finalFunc = row.has("final_func") ? new FunctionName(ksName, row.getString("final_func")) : null;
        AbstractType<?> stateType = row.has("state_type") ? parseType(row.getString("state_type")) : null;
        ByteBuffer initcond = row.has("initcond") ? row.getBytes("initcond") : null;

        try
        {
            return create(name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond);
        }
        catch (InvalidRequestException reason)
        {
            return createBroken(name, argTypes, returnType, initcond, reason);
        }
    }

    private static UDAggregate createBroken(FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType,
                                            ByteBuffer initcond, final InvalidRequestException reason)
    {
        return new UDAggregate(name, argTypes, returnType, null, null, initcond) {
            public Aggregate newAggregate() throws InvalidRequestException
            {
                throw new InvalidRequestException(String.format("Aggregate '%s' exists but hasn't been loaded successfully for the following reason: %s. "
                                                                + "Please see the server log for more details", this, reason.getMessage()));
            }
        };
    }

    private static UDAggregate create(FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType,
                                      FunctionName stateFunc, FunctionName finalFunc, AbstractType<?> stateType, ByteBuffer initcond)
    throws InvalidRequestException
    {
        List<AbstractType<?>> stateTypes = new ArrayList<>(argTypes.size() + 1);
        stateTypes.add(stateType);
        stateTypes.addAll(argTypes);
        List<AbstractType<?>> finalTypes = Collections.<AbstractType<?>>singletonList(stateType);
        return new UDAggregate(name, argTypes, returnType,
                               resolveScalar(name, stateFunc, stateTypes),
                               finalFunc != null ? resolveScalar(name, finalFunc, finalTypes) : null,
                               initcond);
    }

    private static AbstractType<?> parseType(String str)
    {
        // We only use this when reading the schema where we shouldn't get an error
        try
        {
            return TypeParser.parse(str);
        }
        catch (SyntaxException | ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDAggregate))
            return false;

        UDAggregate that = (UDAggregate) o;
        return Objects.equal(this.name, that.name)
               && Functions.typeEquals(this.argTypes, that.argTypes)
               && Functions.typeEquals(this.returnType, that.returnType)
               && Objects.equal(this.stateFunction, that.stateFunction)
               && Objects.equal(this.finalFunction, that.finalFunction)
               && Objects.equal(this.stateType, that.stateType)
               && Objects.equal(this.initcond, that.initcond);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argTypes, returnType, stateFunction, finalFunction, stateType, initcond);
    }
}
