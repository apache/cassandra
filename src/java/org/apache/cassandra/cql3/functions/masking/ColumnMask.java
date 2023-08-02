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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionResolver;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.exceptions.InvalidRequestException;
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
 *    CREATE TABLE t (k int PRIMARY KEY, v int MASKED WITH mask_inner(1, 1));
 *    ALTER TABLE t ALTER v MASKED WITH mask_inner(2, 1);
 *    ALTER TABLE t ALTER v DROP MASKED;
 * </pre>
 * Note that in the example above we are referencing the {@code mask_inner} function with two arguments. However, that
 * CQL function actually has three arguments. The first argument is always ommitted when attaching the function to a
 * schema column. The value of that first argument is always the value of the masked column, in this case an int.
 */
public class ColumnMask
{
    public static final String DISABLED_ERROR_MESSAGE = "Cannot mask columns because dynamic data masking is not " +
                                                        "enabled. You can enable it with the " +
                                                        "dynamic_data_masking_enabled property on cassandra.yaml";

    /** The CQL function used for masking. */
    public final ScalarFunction function;

    /** The values of the arguments of the partially applied masking function. */
    protected final ByteBuffer[] partialArgumentValues;

    public ColumnMask(ScalarFunction function, ByteBuffer... partialArgumentValues)
    {
        assert function.argTypes().size() == partialArgumentValues.length + 1;
        this.function = function;
        this.partialArgumentValues = partialArgumentValues;
    }

    /**
     * @return The types of the arguments of the partially applied masking function, as an unmodifiable list.
     */
    public List<AbstractType<?>> partialArgumentTypes()
    {
        List<AbstractType<?>> argTypes = function.argTypes();
        return argTypes.size() == 1
               ? Collections.emptyList()
               : Collections.unmodifiableList(argTypes.subList(1, argTypes.size()));
    }

    /**
     * @return The values of the arguments of the partially applied masking function, as an unmodifiable list that can
     * contain nulls.
     */
    public List<ByteBuffer> partialArgumentValues()
    {
        return Collections.unmodifiableList(Arrays.asList(partialArgumentValues));
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
     * @param version the used version of the transport protocol
     * @return a masker instance that caches the terminal masking function arguments
     */
    public Masker masker(ProtocolVersion version)
    {
        return new Masker(version, function, partialArgumentValues);
    }

    public static class Masker
    {
        private final ScalarFunction function;
        private final Arguments arguments;

        private Masker(ProtocolVersion version, ScalarFunction function, ByteBuffer[] partialArgumentValues)
        {
            this.function = function;
            arguments = function.newArguments(version);
            for (int i = 0; i < partialArgumentValues.length; i++)
                arguments.set(i + 1, partialArgumentValues[i]);
        }

        public ByteBuffer mask(ByteBuffer value)
        {
            arguments.set(0, value);
            return function.execute(arguments);
        }
    }

    public static void ensureEnabled()
    {
        if (!DatabaseDescriptor.getDynamicDataMaskingEnabled())
            throw new InvalidRequestException(DISABLED_ERROR_MESSAGE);
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
               && Arrays.equals(partialArgumentValues, mask.partialArgumentValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function.name(), Arrays.hashCode(partialArgumentValues));
    }

    @Override
    public String toString()
    {
        List<AbstractType<?>> types = partialArgumentTypes();
        List<String> arguments = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++)
        {
            CQL3Type type = types.get(i).asCQL3Type();
            ByteBuffer value = partialArgumentValues[i];
            arguments.add(type.toCQLLiteral(value));
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
            ByteBuffer[] partialArguments = preparePartialArguments(keyspace, function);
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

            if (function.isNative() && !(function instanceof MaskingFunction))
                throw invalidRequest("Not-masking function %s cannot be used for masking table columns", this);

            if (!function.isNative() && !function.name().keyspace.equals(keyspace))
                throw invalidRequest("Masking function %s doesn't belong to the same keyspace as the table %s.%s",
                                     this, keyspace, table);

            CQL3Type returnType = function.returnType().asCQL3Type();
            CQL3Type expectedType = type.asCQL3Type();
            if (!returnType.equals(expectedType))
                throw invalidRequest("Masking function %s return type is %s. " +
                                     "This is different to the type of the masked column %s of type %s. " +
                                     "Masking functions can only be attached to table columns " +
                                     "if they return the same data type as the masked column.",
                                     this, returnType, column, expectedType);

            return (ScalarFunction) function;
        }

        private ByteBuffer[] preparePartialArguments(String keyspace, ScalarFunction function)
        {
            // Note that there could be null arguments
            ByteBuffer[] arguments = new ByteBuffer[rawPartialArguments.size()];

            for (int i = 0; i < rawPartialArguments.size(); i++)
            {
                String term = rawPartialArguments.get(i).toString();
                AbstractType<?> type = function.argTypes().get(i + 1);
                arguments[i] = Terms.asBytes(keyspace, term, type);
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
