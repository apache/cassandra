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
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.cql3.Operator.NEQ;
import static org.apache.cassandra.cql3.constraints.AbstractFunctionSatisfiabilityChecker.FUNCTION_SATISFIABILITY_CHECKER;

public class FunctionColumnConstraint extends AbstractFunctionConstraint<FunctionColumnConstraint>
{
    public static final Serializer serializer = new Serializer();

    private final ConstraintFunction function;

    public final static class Raw
    {
        public final ConstraintFunction function;
        public final ColumnIdentifier columnName;
        public final Operator relationType;
        public final String term;

        public Raw(ColumnIdentifier functionName, ColumnIdentifier columnName, Operator relationType, String term)
        {
            this.relationType = relationType;
            this.columnName = columnName;
            this.term = term;
            function = createConstraintFunction(functionName.toCQLString(), columnName);
        }

        public FunctionColumnConstraint prepare()
        {
            return new FunctionColumnConstraint(function, columnName, relationType, term);
        }
    }

    public enum Functions implements SatisfiabilityChecker
    {
        LENGTH(LengthConstraint::new)
        {
            @Override
            public void checkSatisfiability(List<ColumnConstraint<?>> constraints, ColumnMetadata columnMetadata)
            {
                FUNCTION_SATISFIABILITY_CHECKER.check(name(), constraints, columnMetadata);
            }
        };

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

    private FunctionColumnConstraint(ConstraintFunction function, ColumnIdentifier columnName, Operator relationType, String term)
    {
        super(columnName, relationType, term);
        this.function = function;
    }

    public ConstraintFunction function()
    {
        return function;
    }

    @Override
    public Set<Operator> getSupportedOperators()
    {
        return function.getSupportedOperators();
    }

    @Override
    public String name()
    {
        return function.name;
    }

    @Override
    public String fullName()
    {
        return function.name + ' ' + relationType;
    }

    @Override
    public boolean enablesDuplicateDefinitions(String name)
    {
        return relationType == NEQ;
    }

    @Override
    public MetadataSerializer<FunctionColumnConstraint> serializer()
    {
        return serializer;
    }

    @Override
    public void evaluate(AbstractType<?> valueType, ByteBuffer columnValue)
    {
        function.evaluate(valueType, relationType, term, columnValue);
    }

    @Override
    protected void internalEvaluate(AbstractType<?> valueType, ByteBuffer columnValue)
    {
        // evaluation is done on function
    }

    @Override
    public void validate(ColumnMetadata columnMetadata)
    {
        validateArgs(columnMetadata);
        function.validate(columnMetadata);
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return ConstraintType.FUNCTION;
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
        return function.name + "(" + columnName + ") " + relationType + " " + term;
    }

    public static class Serializer implements MetadataSerializer<FunctionColumnConstraint>
    {
        @Override
        public void serialize(FunctionColumnConstraint columnConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(columnConstraint.function.name);
            out.writeUTF(columnConstraint.columnName.toCQLString());
            columnConstraint.relationType.writeTo(out);
            out.writeUTF(columnConstraint.term);
        }

        @Override
        public FunctionColumnConstraint deserialize(DataInputPlus in, Version version) throws IOException
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
            Operator relationType = Operator.readFrom(in);
            final String term = in.readUTF();
            return new FunctionColumnConstraint(function, columnName, relationType, term);
        }

        @Override
        public long serializedSize(FunctionColumnConstraint columnConstraint, Version version)
        {
            return TypeSizes.sizeof(columnConstraint.function.getClass().getName())
                   + TypeSizes.sizeof(columnConstraint.columnName.toCQLString())
                   + TypeSizes.sizeof(columnConstraint.term)
                   + Operator.serializedSize();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof FunctionColumnConstraint))
            return false;

        FunctionColumnConstraint other = (FunctionColumnConstraint) o;

        return function.equals(other.function)
               && columnName.equals(other.columnName)
               && relationType == other.relationType
               && term.equals(other.term);
    }
}
