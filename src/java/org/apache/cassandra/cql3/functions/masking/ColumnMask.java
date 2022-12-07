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

package org.apache.cassandra.cql3.functions.masking;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionResolver;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Dynamic data mask that can be applied to a schema column.
 * <p>
 * It consists on a partial application of a certain {@link MaskingFunction} to the values of a column, with the
 * precondition that the type of any masked column is compatible with the type of the first argument of the function.
 * <p>
 * This partial application is meant to be associated to specific columns in the schema, acting as a mask for the values
 * of those columns. It's associated to queries such as:
 * <pre>
 *    CREATE TABLE %t (k int PRIMARY KEY, v int MASKED WITH mask_inner(1, 1));
 *    ALTER TABLE t ALTER v MASKED WITH mask_inner(2, 1);
 *    ALTER TABLE t ALTER v DROP MASKED;
 * </pre>
 * Note that in the example above we are referencing the {@code mask_inner} function with two arguments. However, that
 * CQL function actually has three arguments. The first argument is always ommitted when attaching the function to a
 * schema column. The value of that first argument is always the value of the masked column, in this case an int.
 */
public class ColumnMask
{
    /** The CQL function used for masking. */
    public final ScalarFunction function;

    /** The values of the arguments of the partially applied masking function. */
    public final List<ByteBuffer> partialArgumentValues;

    public ColumnMask(ScalarFunction function, List<ByteBuffer> partialArgumentValues)
    {
        assert function.argTypes().size() == partialArgumentValues.size() + 1;
        this.function = function;
        this.partialArgumentValues = partialArgumentValues;
    }

    /**
     * @return The types of the arguments of the partially applied masking function.
     */
    public List<AbstractType<?>> partialArgumentTypes()
    {
        List<AbstractType<?>> argTypes = function.argTypes();
        return argTypes.size() == 1
               ? Collections.emptyList()
               : argTypes.subList(1, argTypes.size());
    }

    /**
     * @return A copy of this mask for a version of its masked column that has its type reversed.
     */
    public ColumnMask withReversedType()
    {
        AbstractType<?> reversed = ReversedType.getInstance(function.argTypes().get(0));
        List<AbstractType<?>> args = ImmutableList.<AbstractType<?>>builder()
                                                  .add(reversed)
                                                  .addAll(partialArgumentTypes())
                                                  .build();
        Function newFunction = FunctionResolver.get(function.name().keyspace, function.name(), args, null, null, null);
        assert newFunction != null;
        return new ColumnMask((ScalarFunction) newFunction, partialArgumentValues);
    }

    /**
     * @param protocolVersion the used version of the transport protocol
     * @param value           a column value to be masked
     * @return the specified value after having been masked by the masked function
     */
    public ByteBuffer mask(ProtocolVersion protocolVersion, ByteBuffer value)
    {
        List<ByteBuffer> args = new ArrayList<>(partialArgumentValues.size() + 1);
        args.add(value);
        args.addAll(partialArgumentValues);
        return function.execute(protocolVersion, args);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ColumnMask mask = (ColumnMask) o;
        return function.name().equals(mask.function.name())
               && partialArgumentValues.equals(mask.partialArgumentValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function.name(), partialArgumentValues);
    }

    @Override
    public String toString()
    {
        List<AbstractType<?>> types = partialArgumentTypes();
        List<String> arguments = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++)
        {
            CQL3Type type = types.get(i).asCQL3Type();
            ByteBuffer value = partialArgumentValues.get(i);
            arguments.add(type.toCQLLiteral(value, ProtocolVersion.CURRENT));
        }
        return format("%s(%s)", function.name(), StringUtils.join(arguments, ", "));
    }

    public void appendCqlTo(CqlBuilder builder)
    {
        builder.append(" MASKED WITH ").append(toString());
    }

    /**
     * A parsed but not prepared column mask.
     */
    public final static class Raw
    {
        public final FunctionName name;
        public final List<Term.Raw> rawPartialArguments;

        public Raw(FunctionName name, List<Term.Raw> rawPartialArguments)
        {
            this.name = name;
            this.rawPartialArguments = rawPartialArguments;
        }

        public ColumnMask prepare(String keyspace, String table, ColumnIdentifier column, AbstractType<?> type)
        {
            ScalarFunction function = findMaskingFunction(keyspace, table, column, type);

            List<ByteBuffer> partialArguments = preparePartialArguments(keyspace, function);

            return new ColumnMask(function, partialArguments);
        }

        private ScalarFunction findMaskingFunction(String keyspace, String table, ColumnIdentifier column, AbstractType<?> type)
        {
            List<AssignmentTestable> args = new ArrayList<>(rawPartialArguments.size() + 1);
            args.add(type);
            args.addAll(rawPartialArguments);

            Function function = FunctionResolver.get(keyspace, name, args, keyspace, table, type);

            if (function == null)
                throw invalidRequest("Unable to find masking function for %s, " +
                                     "no declared function matches the signature %s",
                                     column, this);

            if (function.isAggregate())
                throw invalidRequest("Aggregate function %s cannot be used for masking table columns", this);

            if (!function.isNative())
                throw invalidRequest("User defined function %s cannot be used for masking table columns", this);

            if (!(function instanceof MaskingFunction))
                throw invalidRequest("Not-masking function %s cannot be used for masking table columns", this);

            if (!function.returnType().equals(type))
                throw invalidRequest("Masking function %s return type is %s. " +
                                     "This is different to the type of the masked column %s of type %s. " +
                                     "Masking functions can only be attached to table columns " +
                                     "if they return the same data type as the masked column.",
                                     this, function.returnType().asCQL3Type(), column, type.asCQL3Type());

            return (ScalarFunction) function;
        }

        private List<ByteBuffer> preparePartialArguments(String keyspace, ScalarFunction function)
        {
            // Note that there could be null arguments
            List<ByteBuffer> arguments = new ArrayList<>(rawPartialArguments.size());

            for (int i = 0; i < rawPartialArguments.size(); i++)
            {
                String term = rawPartialArguments.get(i).toString();
                AbstractType<?> type = function.argTypes().get(i + 1);
                arguments.add(Terms.asBytes(keyspace, term, type));
            }

            return arguments;
        }

        @Override
        public String toString()
        {
            return format("%s(%s)", name, StringUtils.join(rawPartialArguments, ", "));
        }
    }
}
