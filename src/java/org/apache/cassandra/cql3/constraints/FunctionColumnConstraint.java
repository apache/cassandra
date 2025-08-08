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

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ConstraintResolver.Functions;
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
        public final Operator relationType;
        public final String term;

        public Raw(ColumnIdentifier functionName, List<String> arguments, Operator relationType, String term)
        {
            this.relationType = relationType;
            this.term = term;
            if (arguments == null)
                arguments = new ArrayList<>();
            function = ConstraintResolver.getConstraintFunction(functionName.toCQLString(), arguments);
        }

        public FunctionColumnConstraint prepare()
        {
            return new FunctionColumnConstraint(function, relationType, term);
        }
    }

    public static SatisfiabilityChecker[] getSatisfiabilityCheckers()
    {
        SatisfiabilityChecker[] satisfiabilityCheckers = new SatisfiabilityChecker[Functions.values().length];
        for (int i = 0; i < Functions.values().length; i++)
        {
            String name = Functions.values()[i].name();
            satisfiabilityCheckers[i] = (constraints, columnMetadata) -> FUNCTION_SATISFIABILITY_CHECKER.check(name, constraints, columnMetadata);
        }

        return satisfiabilityCheckers;
    }

    private FunctionColumnConstraint(ConstraintFunction function, Operator relationType, String term)
    {
        super(relationType, term);
        this.function = function;
        this.columnName = function.columnName;
    }

    @Override
    public void setColumnName(ColumnIdentifier columnName)
    {
        this.columnName = columnName;
        this.function.columnName = columnName;
    }

    public ConstraintFunction function()
    {
        return function;
    }

    @Override
    public List<Operator> getSupportedOperators()
    {
        return function.getSupportedOperators();
    }

    @Override
    public List<AbstractType<?>> getSupportedTypes()
    {
        return function.getSupportedTypes();
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
        validateTypes(columnMetadata);
        function.validate(columnMetadata, term);
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return ConstraintType.FUNCTION;
    }

    @Override
    public String toString()
    {
        String arguments = String.join(",", function.rawArgs);
        return function.name + '(' + arguments + ") " + relationType + ' ' + term;
    }

    public static class Serializer implements MetadataSerializer<FunctionColumnConstraint>
    {
        @Override
        public void serialize(FunctionColumnConstraint columnConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(columnConstraint.function.name);

            int argsSize = columnConstraint.function.args.size();
            out.writeInt(argsSize);
            for (int i = 0; i < argsSize; i++)
                out.writeUTF(columnConstraint.function.args.get(i));

            columnConstraint.relationType.writeTo(out);
            out.writeUTF(columnConstraint.term);
        }

        @Override
        public FunctionColumnConstraint deserialize(DataInputPlus in, Version version) throws IOException
        {
            String functionName = in.readUTF();

            List<String> args = new ArrayList<>();
            int argsSize = in.readInt();
            for (int i = 0; i < argsSize; i++)
                args.add(in.readUTF());

            ConstraintFunction function;
            try
            {
                function = ConstraintResolver.getConstraintFunction(functionName, args);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
            Operator relationType = Operator.readFrom(in);
            final String term = in.readUTF();
            return new FunctionColumnConstraint(function, relationType, term);
        }

        @Override
        public long serializedSize(FunctionColumnConstraint columnConstraint, Version version)
        {
            int argsSizes = 0;
            for (String arg : columnConstraint.function.args)
                argsSizes += TypeSizes.sizeof(arg);

            return TypeSizes.sizeof(columnConstraint.function.getClass().getName())
                   + TypeSizes.sizeof(columnConstraint.function.args.size())
                   + argsSizes
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
