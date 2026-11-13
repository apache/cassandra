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
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.constraints.ConstraintsProviderTest.CustomConstraintProvider.MyCustomConstraint;
import org.apache.cassandra.constraints.ConstraintsProviderTest.CustomConstraintProvider.MyCustomUnaryConstraint;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ColumnConstraint;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.ConstraintFunction;
import org.apache.cassandra.cql3.constraints.ConstraintProvider;
import org.apache.cassandra.cql3.constraints.ConstraintResolver;
import org.apache.cassandra.cql3.constraints.ConstraintResolver.Functions;
import org.apache.cassandra.cql3.constraints.ConstraintResolver.UnaryFunctions;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.cql3.constraints.JsonConstraint;
import org.apache.cassandra.cql3.constraints.LengthConstraint;
import org.apache.cassandra.cql3.constraints.SatisfiabilityChecker;
import org.apache.cassandra.cql3.constraints.UnaryConstraintFunction;
import org.apache.cassandra.cql3.constraints.UnaryFunctionColumnConstraint;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.cql3.constraints.AbstractFunctionSatisfiabilityChecker.FUNCTION_SATISFIABILITY_CHECKER;
import static org.apache.cassandra.cql3.constraints.ConstraintResolver.customConstraintProvider;
import static org.apache.cassandra.cql3.constraints.ConstraintResolver.getConstraintFunction;
import static org.apache.cassandra.cql3.constraints.ConstraintResolver.getUnaryConstraintFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ConstraintsProviderTest
{
    private static final ColumnIdentifier COLUMN = ColumnIdentifier.getInterned("v", true);

    private ConstraintProvider previousProvider;

    @Before
    public void setProvider()
    {
        previousProvider = customConstraintProvider;
        customConstraintProvider = new CustomConstraintProvider();
    }

    @After
    public void resetProvider()
    {
        customConstraintProvider = previousProvider;
    }

    @Test
    public void testContraintProvider()
    {

        ConstraintFunction constraintFunction = getConstraintFunction(MyCustomConstraint.NAME, List.of());
        constraintFunction.prepare("20");
        assertNotNull(constraintFunction);
        constraintFunction.evaluate(UTF8Type.instance,
                                    Operator.EQ,
                                    Integer.toString(MyCustomConstraint.NAME.length()),
                                    UTF8Type.instance.fromString(MyCustomConstraint.NAME));

        ConstraintFunction unaryConstraint = getUnaryConstraintFunction(MyCustomUnaryConstraint.NAME, List.of());
        assertNotNull(unaryConstraint);
        unaryConstraint.evaluate(UTF8Type.instance, UTF8Type.instance.fromString("{\"a\": 4, \"b\": 10}"));

        assertThatThrownBy(() -> getConstraintFunction("not_existing", List.of()))
        .isInstanceOf(InvalidConstraintDefinitionException.class);

        SatisfiabilityChecker[] functionSatCheckers = ConstraintResolver.getConstraintFunctionSatisfiabilityCheckers();
        assertNotNull(functionSatCheckers);
        // in built + these in provider
        assertEquals(functionSatCheckers.length,
                     Functions.values().length + customConstraintProvider.getConstraintFunctionSatisfiabilityCheckers().size());

        SatisfiabilityChecker[] unarySatCheckers = ConstraintResolver.getUnarySatisfiabilityCheckers();
        assertNotNull(unarySatCheckers);
        // in built + these in provider
        assertEquals(unarySatCheckers.length,
                     UnaryFunctions.values().length + customConstraintProvider.getUnaryConstraintSatisfiabilityCheckers().size());
    }

    @Test
    public void testCustomConstraintIsNotEqualToBuiltInOneItExtends()
    {
        // MyCustomConstraint extends LengthConstraint and MyCustomUnaryConstraint extends JsonConstraint
        assertNotEquals(constraints(function("length")), constraints(function(MyCustomConstraint.NAME)));
        assertNotEquals(constraints(function(MyCustomConstraint.NAME)), constraints(function("length")));
        assertNotEquals(constraints(unaryFunction("json")), constraints(unaryFunction(MyCustomUnaryConstraint.NAME)));
        assertNotEquals(constraints(unaryFunction(MyCustomUnaryConstraint.NAME)), constraints(unaryFunction("json")));

        assertEquals(constraints(function(MyCustomConstraint.NAME)), constraints(function(MyCustomConstraint.NAME)));
        assertEquals(constraints(function(MyCustomConstraint.NAME)).hashCode(), constraints(function(MyCustomConstraint.NAME)).hashCode());
        assertEquals(constraints(unaryFunction(MyCustomUnaryConstraint.NAME)), constraints(unaryFunction(MyCustomUnaryConstraint.NAME)));
        assertEquals(constraints(unaryFunction(MyCustomUnaryConstraint.NAME)).hashCode(), constraints(unaryFunction(MyCustomUnaryConstraint.NAME)).hashCode());
    }

    @Test
    public void testCustomConstraintsAreCopiedAndDeserializedByProvider() throws IOException
    {
        ColumnConstraints original = constraints(function(MyCustomConstraint.NAME), unaryFunction(MyCustomUnaryConstraint.NAME));

        ColumnConstraints copy = original.copy();
        assertNotSame(original, copy);
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertTrue(((FunctionColumnConstraint) copy.getConstraints().get(0)).function() instanceof MyCustomConstraint);
        assertTrue(((UnaryFunctionColumnConstraint) copy.getConstraints().get(1)).function() instanceof MyCustomUnaryConstraint);

        ColumnMetadata column = new ColumnMetadata("ks", "tb", COLUMN, UTF8Type.instance, 0, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null, original);
        ColumnMetadata renamed = column.withNewName(ColumnIdentifier.getInterned("other", true));
        assertNotEquals(column.getColumnConstraints(), renamed.getColumnConstraints());
        assertEquals(constraints(function(MyCustomConstraint.NAME), unaryFunction(MyCustomUnaryConstraint.NAME)), column.getColumnConstraints());

        DataOutputBuffer out = new DataOutputBuffer();
        ColumnConstraints.serializer.serialize(original, out, Version.V6);
        ColumnConstraints deserialized = ColumnConstraints.serializer.deserialize(new DataInputBuffer(out.getData()), Version.V6);
        ColumnMetadata fromDeserialized = new ColumnMetadata("ks", "tb", COLUMN, UTF8Type.instance, 0, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null, deserialized);
        assertEquals(column, fromDeserialized);
        assertEquals(column.hashCode(), fromDeserialized.hashCode());
    }

    private static ColumnConstraint<?> function(String name)
    {
        return new FunctionColumnConstraint.Raw(new ColumnIdentifier(name, false), List.of(), Operator.GT, "10").prepare();
    }

    private static ColumnConstraint<?> unaryFunction(String name)
    {
        return new UnaryFunctionColumnConstraint.Raw(new ColumnIdentifier(name, false)).prepare();
    }

    private static ColumnConstraints constraints(ColumnConstraint<?>... constraints)
    {
        return new ColumnConstraints.Raw(List.of(constraints)).prepare(COLUMN);
    }

    public static class CustomConstraintProvider implements ConstraintProvider
    {
        @Override
        public Optional<UnaryConstraintFunction> getUnaryConstraint(String functionName, List<String> arguments)
        {
            if (!functionName.equalsIgnoreCase(MyCustomUnaryConstraint.NAME))
                return Optional.empty();

            return Optional.of(new MyCustomUnaryConstraint(arguments));
        }

        @Override
        public Optional<ConstraintFunction> getConstraintFunction(String functionName, List<String> arguments)
        {
            if (!functionName.equalsIgnoreCase(MyCustomConstraint.NAME))
                return Optional.empty();

            return Optional.of(new MyCustomConstraint(arguments));
        }

        @Override
        public List<? extends SatisfiabilityChecker> getUnaryConstraintSatisfiabilityCheckers()
        {
            return List.of(new MyCustomUnaryConstraint(List.of()));
        }

        @Override
        public List<? extends SatisfiabilityChecker> getConstraintFunctionSatisfiabilityCheckers()
        {
            return List.of((constraints, columnMetadata) ->
                           FUNCTION_SATISFIABILITY_CHECKER.check(MyCustomConstraint.NAME,
                                                                 constraints,
                                                                 columnMetadata));
        }

        /**
         * Same as length constraint, just under different name to prove the point
         */
        public static class MyCustomConstraint extends LengthConstraint
        {
            public static final String NAME = "MY_CUSTOM_CONSTRAINT";

            public MyCustomConstraint(List<String> arguments)
            {
                super(NAME, arguments);
            }
        }

        /**
         * Same as JSON constraint, just under different name to prove the point
         */
        public static class MyCustomUnaryConstraint extends JsonConstraint
        {
            public static final String NAME = "MY_CUSTOM_UNARY_CONSTRAINT";

            public MyCustomUnaryConstraint(List<String> arguments)
            {
                super(NAME, arguments);
            }
        }
    }
}
