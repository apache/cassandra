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
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ColumnConstraint;
import org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint;
import org.apache.cassandra.cql3.constraints.ScalarColumnConstraint;
import org.apache.cassandra.cql3.constraints.UnaryFunctionColumnConstraint;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.COMPOSED;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.FUNCTION;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.SCALAR;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.UNARY_FUNCTION;
import static org.junit.Assert.assertEquals;

public class ColumnConstraintsTest
{
    private static final ColumnIdentifier COLUMN = ColumnIdentifier.getInterned("v", true);
    private static final ConstraintType[] EXPECTED_VALUES = { COMPOSED, FUNCTION, SCALAR, UNARY_FUNCTION };

    @Test
    public void testEnumCodesAndNames()
    {
        ConstraintType[] values = ConstraintType.values();

        for (int i = 0; i < values.length; i++)
        {
            assertEquals("Column Constraint Serializer mismatch in the enum " + values[i],
                         EXPECTED_VALUES[i].name(), values[i].name());
            assertEquals("Column Constraint Serializer mismatch in the enum for value " + values[i],
                         ConstraintType.getSerializer(EXPECTED_VALUES[i].ordinal()), ConstraintType.getSerializer(i));
        }

        assertEquals("Column Constraint Serializer enum constants has changed. Update the test.",
                     EXPECTED_VALUES.length, values.length);
    }

    @Test
    public void testEqualConstraintsHaveEqualHashCode()
    {
        assertEqualHashCode(() -> new FunctionColumnConstraint.Raw(new ColumnIdentifier("length", true), List.of(), Operator.GT, "10").prepare());
        assertEqualHashCode(() -> new FunctionColumnConstraint.Raw(new ColumnIdentifier("octet_length", true), List.of(), Operator.LT, "100").prepare());
        assertEqualHashCode(() -> new FunctionColumnConstraint.Raw(new ColumnIdentifier("regexp", true), List.of(), Operator.EQ, "'a.*'").prepare());
        assertEqualHashCode(() -> new ScalarColumnConstraint.Raw(COLUMN, Operator.GT, "5").prepare());
        assertEqualHashCode(() -> new UnaryFunctionColumnConstraint.Raw(new ColumnIdentifier("json", true)).prepare());
        assertEqualHashCode(() -> new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare());
    }

    @Test
    public void testEqualColumnMetadataWithConstraintsHaveEqualHashCode()
    {
        Supplier<ColumnConstraint<?>> constraint = () -> new FunctionColumnConstraint.Raw(new ColumnIdentifier("length", true), List.of(), Operator.GT, "10").prepare();
        ColumnMetadata a = new ColumnMetadata("ks", "tb", COLUMN, UTF8Type.instance, 0, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null, prepare(constraint.get()));
        ColumnMetadata b = new ColumnMetadata("ks", "tb", COLUMN, UTF8Type.instance, 0, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null, prepare(constraint.get()));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    private static void assertEqualHashCode(Supplier<ColumnConstraint<?>> constraint)
    {
        ColumnConstraints a = prepare(constraint.get());
        ColumnConstraints b = prepare(constraint.get());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    private static ColumnConstraints prepare(ColumnConstraint<?> constraint)
    {
        return new ColumnConstraints.Raw(List.of(constraint)).prepare(COLUMN);
    }
}
