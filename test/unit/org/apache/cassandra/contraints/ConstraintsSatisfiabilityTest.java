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

package org.apache.cassandra.contraints;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint;
import org.apache.cassandra.cql3.constraints.ScalarColumnConstraint;
import org.apache.cassandra.cql3.constraints.ScalarColumnConstraint.Raw;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.assertj.core.api.ThrowableAssert;

import static java.util.List.of;
import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.cql3.Operator.GTE;
import static org.apache.cassandra.cql3.Operator.LT;
import static org.apache.cassandra.cql3.Operator.LTE;
import static org.apache.cassandra.cql3.Operator.NEQ;
import static org.apache.cassandra.cql3.constraints.ScalarColumnConstraint.SUPPORTED_OPERATORS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ConstraintsSatisfiabilityTest
{
    private static final ColumnIdentifier columnIdentifier = new ColumnIdentifier("a_column", false);
    private static final ColumnIdentifier lengthFunctionIdentifier = new ColumnIdentifier("LENGTH", false);
    private static final ColumnMetadata regularIntColumn = new ColumnMetadata("a", "b", columnIdentifier, IntegerType.instance, -1, ColumnMetadata.Kind.REGULAR, null);
    private static final ColumnMetadata regularStringColumn = new ColumnMetadata("a", "b", columnIdentifier, UTF8Type.instance, -1, ColumnMetadata.Kind.REGULAR, null);

    @Test
    public void testScalarSatisfiability() throws Throwable
    {
        run(this::scalar, regularIntColumn);
    }

    @Test
    public void testLengthSatisfiability() throws Throwable
    {
        run(this::length, regularStringColumn);
    }

    private <T> void run(QuadFunction<T> quadFunction, ColumnMetadata columnMetadata) throws Throwable
    {
        for (Operator op1 : SUPPORTED_OPERATORS)
        {
            for (Operator op2 : SUPPORTED_OPERATORS)
            {
                if (op1 == op2)
                {
                    if (op1 == NEQ)
                    {
                        // a_column != 0 and a_column != 10 -> valid
                        check(op1, 0, op2, 100, quadFunction, null, columnMetadata);
                        // does not make sense to check twice
                        // check a_column != 0 and a_column != 0
                        check(op1, 0, op2, 0, quadFunction, "There are duplicate constraint definitions on column", columnMetadata);
                    }
                    else
                        check(op1, 0, op2, 100, quadFunction, "There are duplicate constraint definitions on column", columnMetadata);
                }
                else if ((op1 == GT && op2 == GTE) ||
                         (op1 == GTE && op2 == GT) ||
                         (op1 == LT && op2 == LTE) ||
                         (op1 == LTE && op2 == LT) ||
                         (op1 == EQ || op2 == EQ))
                {
                    check(op1, 0, op2, 100, quadFunction, "not supported", columnMetadata);
                }
                else if ((op1 == LTE && op2 == GT) ||
                         (op1 == LT && op2 == GT) ||
                         (op1 == LTE && op2 == GTE) ||
                         (op1 == LT && op2 == GTE))
                {
                    check(op1, 0, op2, 100, quadFunction, "are not satisfiable", columnMetadata);
                }
                else if (!(op1 == NEQ || op2 == NEQ))
                {
                    check(op1, 0, op2, 100, quadFunction, null, columnMetadata);
                }
                else
                {
                    // this is valid
                    // a_column < 0, a_column != 10
                }
            }
        }
    }

    @Test
    public void testNumberOfScalarConstraints()
    {
        // one
        new ColumnConstraints(of(scalar(LT, 5))).validate(regularIntColumn);

        // two
        new ColumnConstraints(of(scalar(LT, 5), scalar(GT, 0))).validate(regularIntColumn);

        // three - invalid

        assertThatThrownBy(() -> new ColumnConstraints(of(scalar(LT, 5), scalar(GT, 0), scalar(GTE, 0))).validate(regularIntColumn))
        .hasMessage("There can not be more than 2 constraints (not including non-equal relations) on a column 'a_column' but you have specified 3");

        // valid
        new ColumnConstraints(of(scalar(LT, 5), scalar(GT, 0), scalar(NEQ, 3))).validate(regularIntColumn);

        // valid, because NEQs have different terms
        new ColumnConstraints(of(scalar(LT, 5), scalar(GT, 0), scalar(NEQ, 3), scalar(NEQ, 4))).validate(regularIntColumn);

        // this has duplicate a_column != 3
        assertThatThrownBy(() -> new ColumnConstraints(of(scalar(LT, 5), scalar(GT, 0), scalar(NEQ, 3), scalar(NEQ, 3))).validate(regularIntColumn))
        .hasMessage("There are duplicate constraint definitions on column 'a_column': a_column != 3");
    }

    private interface QuadFunction<T>
    {
        ColumnConstraints f(Operator op1, Integer term1, Operator o2, Integer term2);
    }

    private <T> void check(Operator operator,
                           Integer term,
                           Operator operator2,
                           Integer term2,
                           QuadFunction<T> quadFunction,
                           String exceptionMessage,
                           ColumnMetadata columnMetadata) throws Throwable
    {
        ThrowableAssert.ThrowingCallable callable = () -> quadFunction.f(operator, term, operator2, term2).validate(columnMetadata);

        if (exceptionMessage != null)
            assertThatThrownBy(callable).hasMessageContaining(exceptionMessage);
        else
            callable.call();
    }

    private ColumnConstraints scalar(Operator operator, Integer term, Operator operator2, Integer term2)
    {
        return new ColumnConstraints(of(scalar(operator, term),
                                        scalar(operator2, term2)));
    }

    private ScalarColumnConstraint scalar(Operator operator, Integer term)
    {
        return new Raw(columnIdentifier, operator, term.toString()).prepare();
    }

    private FunctionColumnConstraint length(Operator operator, Integer term)
    {
        return new FunctionColumnConstraint.Raw(lengthFunctionIdentifier,
                                                columnIdentifier,
                                                operator,
                                                term.toString()).prepare();
    }

    private ColumnConstraints length(Operator operator, Integer term, Operator operator2, Integer term2)
    {
        return new ColumnConstraints(of(length(operator, term),
                                        length(operator2, term2)));
    }
}
