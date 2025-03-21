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

package org.apache.cassandra.constraints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ColumnConstraint;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.ConstraintFunction;
import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.cql3.constraints.UnaryConstraintFunction;
import org.apache.cassandra.cql3.constraints.UnaryFunctionColumnConstraint;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static java.lang.String.format;
import static org.apache.cassandra.schema.ColumnMetadata.Kind.REGULAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class ConstraintArgumentsTest
{
    private static final ColumnIdentifier columnIdentifier = new ColumnIdentifier("a_column", false);
    private static final ColumnMetadata columnMetadata = new ColumnMetadata("a", "b", columnIdentifier, UTF8Type.instance, ColumnMetadata.NO_UNIQUE_ID, -1, REGULAR, null);

    @Test
    public void testDeserOfContraintsWithArguments() throws Throwable
    {
        List<ColumnConstraint<?>> checkConstraints = new ArrayList<>();
        checkConstraints.add(new UnaryFunctionColumnConstraint(new Enumeration(List.of("a", "b", "c"))));
        ColumnConstraints constraints = new ColumnConstraints(checkConstraints);
        constraints.setColumnName(columnIdentifier);

        MetadataSerializer<ColumnConstraints> serializer = new TestingSerializer();

        DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
        serializer.serialize(constraints, dataOutputBuffer, Version.V7);

        DataInputBuffer dataInputBuffer = new DataInputBuffer(dataOutputBuffer.getData());
        ColumnConstraints deserialize = serializer.deserialize(dataInputBuffer, Version.V7);

        List<ColumnConstraint<?>> deserializeConstraints = deserialize.getConstraints();
        assertEquals(1, deserializeConstraints.size());
        ColumnConstraint<?> constraint = deserializeConstraints.get(0);

        assertEquals(Enumeration.FUNCTION_NAME, constraint.name());
        assertEquals(ColumnConstraint.ConstraintType.UNARY_FUNCTION, constraint.getConstraintType());

        constraint.validate(columnMetadata);

        UnaryFunctionColumnConstraint c = ((UnaryFunctionColumnConstraint) constraint);
        List<String> arguments = c.function().arguments();
        assertEquals(List.of("a", "b", "c"), arguments);
    }

    @Test
    public void testDeserOfContraintsWithoutArguments() throws Throwable
    {
        List<ColumnConstraint<?>> checkConstraints = new ArrayList<>();
        checkConstraints.add(new UnaryFunctionColumnConstraint(new ParamerterlessContraint(List.of("a", "b", "c"))));
        ColumnConstraints constraints = new ColumnConstraints(checkConstraints);
        constraints.setColumnName(columnIdentifier);

        MetadataSerializer<ColumnConstraints> serializer = new TestingSerializer();

        DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
        serializer.serialize(constraints, dataOutputBuffer, Version.V7);

        DataInputBuffer dataInputBuffer = new DataInputBuffer(dataOutputBuffer.getData());
        ColumnConstraints deserialize = serializer.deserialize(dataInputBuffer, Version.V7);

        List<ColumnConstraint<?>> deserializeConstraints = deserialize.getConstraints();
        assertEquals(1, deserializeConstraints.size());
        ColumnConstraint<?> constraint = deserializeConstraints.get(0);

        assertEquals(ParamerterlessContraint.FUNCTION_NAME, constraint.name());
        assertEquals(ColumnConstraint.ConstraintType.UNARY_FUNCTION, constraint.getConstraintType());

        assertThatThrownBy(() -> constraint.validate(columnMetadata))
        .isInstanceOf(InvalidConstraintDefinitionException.class)
        .hasMessage("Constraint PARAMERTERLESS does not accept any arguments.");
    }

    private static class TestingUnaryFunctionSerializer extends UnaryFunctionColumnConstraint.Serializer
    {
        @Override
        public ConstraintFunction getConstraintFunction(String functionName, List<String> args)
        {
            if (functionName.equals(Enumeration.FUNCTION_NAME))
                return new Enumeration(args);

            if (functionName.equals(ParamerterlessContraint.FUNCTION_NAME))
                return new ParamerterlessContraint(args);

            throw new IllegalStateException("not supported");
        }
    }

    private static class TestingSerializer extends ColumnConstraints.Serializer
    {
        private static final TestingUnaryFunctionSerializer constraintSerializer = new TestingUnaryFunctionSerializer();

        @Override
        public ColumnConstraint<?> deserializeConstraint(DataInputPlus in, int serializerPosition, Version version) throws IOException
        {
            return constraintSerializer.deserialize(in, version);
        }
    }

    private static class ParamerterlessContraint extends UnaryConstraintFunction
    {
        public static final String FUNCTION_NAME = "PARAMERTERLESS";

        public ParamerterlessContraint(List<String> args)
        {
            super(FUNCTION_NAME, args);
        }

        @Override
        protected void internalEvaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue)
        {

        }

        @Override
        public List<AbstractType<?>> getSupportedTypes()
        {
            return null;
        }
    }

    private static class Enumeration extends UnaryConstraintFunction
    {
        private static final List<AbstractType<?>> SUPPORTED_TYPES = List.of(UTF8Type.instance, AsciiType.instance);

        public static final String FUNCTION_NAME = "ENUM";

        public Enumeration(List<String> args)
        {
            super(FUNCTION_NAME, args);
        }

        @Override
        public void internalEvaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue)
        {
            if (!args.contains(valueType.getString(columnValue)))
            {
                throw new ConstraintViolationException(format("Value for column '%s' violated %s constraint as its value is not one of %s.",
                                                              columnName.toCQLString(),
                                                              name,
                                                              args));
            }
        }

        @Override
        public List<AbstractType<?>> getSupportedTypes()
        {
            return SUPPORTED_TYPES;
        }

        @Override
        public boolean isParameterless()
        {
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Enumeration))
                return false;

            Enumeration other = (Enumeration) o;

            return columnName.equals(other.columnName) && name.equals(other.name);
        }
    }
}
