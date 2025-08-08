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

package org.apache.cassandra.cql3.constraints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ConstraintResolver.UnaryFunctions;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.UNARY_FUNCTION;

public class UnaryFunctionColumnConstraint extends AbstractFunctionConstraint<UnaryFunctionColumnConstraint>
{
    public static final Serializer serializer = new Serializer();

    private final ConstraintFunction function;

    public final static class Raw
    {
        public final ConstraintFunction function;

        public Raw(String name)
        {
            this(new ColumnIdentifier(name, true));
        }

        public Raw(ColumnIdentifier functionName, List<String> arguments)
        {
            function = ConstraintResolver.getUnaryConstraintFunction(functionName.toString(), arguments);
        }

        public Raw(ColumnIdentifier functionName)
        {
            function = ConstraintResolver.getUnaryConstraintFunction(functionName.toString(), List.of());
        }

        public UnaryFunctionColumnConstraint prepare()
        {
            return new UnaryFunctionColumnConstraint(function);
        }
    }

    public UnaryFunctionColumnConstraint(ConstraintFunction function)
    {
        super(null, null);
        this.function = function;
        this.columnName = function.columnName;
    }

    @Override
    public void setColumnName(ColumnIdentifier columnName)
    {
        this.columnName = columnName;
        this.function.columnName = columnName;
    }

    @Override
    public String name()
    {
        return function.name;
    }

    public ConstraintFunction function()
    {
        return function;
    }

    @Override
    public MetadataSerializer<UnaryFunctionColumnConstraint> serializer()
    {
        return serializer;
    }

    @Override
    public List<Operator> getSupportedOperators()
    {
        return List.of();
    }

    @Override
    public List<AbstractType<?>> getSupportedTypes()
    {
        return function.getSupportedTypes();
    }

    @Override
    public boolean enablesDuplicateDefinitions(String name)
    {
        return UnaryFunctions.valueOf(name).enableDuplicateDefinitions();
    }

    @Override
    public void internalEvaluate(AbstractType<?> valueType, ByteBuffer columnValue) throws ConstraintViolationException
    {
        function.evaluate(valueType, columnValue);
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        validateTypes(columnMetadata);
        function.validate(columnMetadata, term);
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return UNARY_FUNCTION;
    }

    @Override
    public String toString()
    {
        if (function.isParameterless())
        {
            return function.toString();
        }
        else
        {
            String arguments = String.join(",", function.rawArgs);
            return function.toString() + '(' + arguments + ')';
        }
    }

    public static class Serializer implements MetadataSerializer<UnaryFunctionColumnConstraint>
    {
        @Override
        public void serialize(UnaryFunctionColumnConstraint columnConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(columnConstraint.function.name);

            int argsSize = columnConstraint.function.args.size();
            out.writeInt(argsSize);
            for (int i = 0; i < argsSize; i++)
                out.writeUTF(columnConstraint.function.args.get(i));
        }

        @Override
        public UnaryFunctionColumnConstraint deserialize(DataInputPlus in, Version version) throws IOException
        {
            String functionName = in.readUTF();

            List<String> args = new ArrayList<>();
            int argsSize = in.readInt();
            for (int i = 0; i < argsSize; i++)
                args.add(in.readUTF());

            ConstraintFunction function;
            try
            {
                function = getConstraintFunction(functionName, args);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }

            return new UnaryFunctionColumnConstraint(function);
        }

        @VisibleForTesting
        public ConstraintFunction getConstraintFunction(String functionName, List<String> args)
        {
            return ConstraintResolver.getUnaryConstraintFunction(functionName, args);
        }

        @Override
        public long serializedSize(UnaryFunctionColumnConstraint columnConstraint, Version version)
        {
            return TypeSizes.sizeof(columnConstraint.function.getClass().getName())
                   + TypeSizes.sizeof(columnConstraint.columnName.toCQLString());
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof UnaryFunctionColumnConstraint))
            return false;

        UnaryFunctionColumnConstraint other = (UnaryFunctionColumnConstraint) o;

        return function.equals(other.function)
               && columnName.equals(other.columnName);
    }
}
