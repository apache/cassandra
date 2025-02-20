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
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.SatisfiabilityChecker.UnaryFunctionSatisfiabilityChecker;
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
        public final ColumnIdentifier columnName;

        public Raw(ColumnIdentifier functionName, ColumnIdentifier columnName)
        {
            this.columnName = columnName;
            function = createConstraintFunction(functionName.toCQLString(), columnName);
        }

        public UnaryFunctionColumnConstraint prepare()
        {
            return new UnaryFunctionColumnConstraint(function, columnName);
        }
    }

    public enum Functions implements UnaryFunctionSatisfiabilityChecker
    {
        NOT_NULL(NotNullConstraint::new),
        JSON(JsonConstraint::new);

        private final Function<ColumnIdentifier, ConstraintFunction> functionCreator;

        Functions(Function<ColumnIdentifier, ConstraintFunction> functionCreator)
        {
            this.functionCreator = functionCreator;
        }
    }

    private static ConstraintFunction createConstraintFunction(String functionName, ColumnIdentifier columnName)
    {
        return getEnum(Functions.class, functionName).functionCreator.apply(columnName);
    }

    private UnaryFunctionColumnConstraint(ConstraintFunction function, ColumnIdentifier columnName)
    {
        super(columnName, null, null);
        this.function = function;
    }

    @Override
    public String name()
    {
        return function.name;
    }

    @Override
    public MetadataSerializer<UnaryFunctionColumnConstraint> serializer()
    {
        return serializer;
    }

    @Override
    public Set<Operator> getSupportedOperators()
    {
        return Set.of();
    }

    @Override
    public boolean enablesDuplicateDefinitions(String name)
    {
        return Functions.valueOf(name).enableDuplicateDefinitions();
    }

    @Override
    public void internalEvaluate(AbstractType<?> valueType, ByteBuffer columnValue) throws ConstraintViolationException
    {
        function.evaluate(valueType, columnValue);
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        validateArgs(columnMetadata);
        function.validate(columnMetadata);
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return UNARY_FUNCTION;
    }

    void validateArgs(ColumnMetadata columnMetadata)
    {
        if (!columnMetadata.name.equals(columnName))
            throw new InvalidConstraintDefinitionException(String.format("Parameter of %s constraint should be the column name (%s)",
                                                                         name(),
                                                                         columnMetadata.name));
    }

    @Override
    public String toString()
    {
        return function.name + "(" + columnName + ")";
    }

    public static class Serializer implements MetadataSerializer<UnaryFunctionColumnConstraint>
    {
        @Override
        public void serialize(UnaryFunctionColumnConstraint columnConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(columnConstraint.function.name);
            out.writeUTF(columnConstraint.columnName.toCQLString());
        }

        @Override
        public UnaryFunctionColumnConstraint deserialize(DataInputPlus in, Version version) throws IOException
        {
            String functionName = in.readUTF();
            ConstraintFunction function;
            String columnNameString = in.readUTF();
            ColumnIdentifier columnName = new ColumnIdentifier(columnNameString, true);
            try
            {
                function = createConstraintFunction(functionName, columnName);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }

            return new UnaryFunctionColumnConstraint(function, columnName);
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
