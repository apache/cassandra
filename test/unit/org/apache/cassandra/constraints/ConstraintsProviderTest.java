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

import java.util.List;
import java.util.Optional;

import org.junit.Test;

import org.apache.cassandra.constraints.ConstraintsProviderTest.CustomConstraintProvider.MyCustomConstraint;
import org.apache.cassandra.constraints.ConstraintsProviderTest.CustomConstraintProvider.MyCustomUnaryConstraint;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ConstraintFunction;
import org.apache.cassandra.cql3.constraints.ConstraintProvider;
import org.apache.cassandra.cql3.constraints.ConstraintResolver;
import org.apache.cassandra.cql3.constraints.ConstraintResolver.Functions;
import org.apache.cassandra.cql3.constraints.ConstraintResolver.UnaryFunctions;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.cql3.constraints.JsonConstraint;
import org.apache.cassandra.cql3.constraints.LengthConstraint;
import org.apache.cassandra.cql3.constraints.SatisfiabilityChecker;
import org.apache.cassandra.cql3.constraints.UnaryConstraintFunction;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.apache.cassandra.cql3.constraints.AbstractFunctionSatisfiabilityChecker.FUNCTION_SATISFIABILITY_CHECKER;
import static org.apache.cassandra.cql3.constraints.ConstraintResolver.customConstraintProvider;
import static org.apache.cassandra.cql3.constraints.ConstraintResolver.getConstraintFunction;
import static org.apache.cassandra.cql3.constraints.ConstraintResolver.getUnaryConstraintFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConstraintsProviderTest
{
    @Test
    public void testContraintProvider()
    {
        customConstraintProvider = new CustomConstraintProvider();

        ConstraintFunction constraintFunction = getConstraintFunction(MyCustomConstraint.NAME, List.of());
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
