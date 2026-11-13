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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ColumnConstraint;
import org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint;
import org.apache.cassandra.cql3.constraints.ScalarColumnConstraint;
import org.apache.cassandra.cql3.constraints.UnaryFunctionColumnConstraint;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.COMPOSED;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.FUNCTION;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.SCALAR;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.UNARY_FUNCTION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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
        assertEqualColumnMetadata(COLUMN, prepare(constraint.get()), prepare(constraint.get()));
    }

    @Test
    public void testSerializationRoundTripKeepsEqualsAndHashCode() throws IOException
    {
        for (ColumnIdentifier column : List.of(COLUMN, ColumnIdentifier.getInterned("MyCol", true)))
        {
            assertRoundTrip(column, new FunctionColumnConstraint.Raw(new ColumnIdentifier("length", true), List.of(), Operator.GT, "10").prepare());
            assertRoundTrip(column, new FunctionColumnConstraint.Raw(new ColumnIdentifier("octet_length", true), List.of(), Operator.LT, "100").prepare());
            assertRoundTrip(column, new FunctionColumnConstraint.Raw(new ColumnIdentifier("regexp", true), List.of(), Operator.EQ, "'a.*'").prepare());
            assertRoundTrip(column, new ScalarColumnConstraint.Raw(column, Operator.GT, "5").prepare());
            assertRoundTrip(column, new UnaryFunctionColumnConstraint.Raw(new ColumnIdentifier("json", true)).prepare());
            assertRoundTrip(column, new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare());
        }
    }

    @Test
    public void testScalarConstraintWithDifferentColumnNameHasEqualHashCodeInColumnMetadata()
    {
        ColumnConstraints a = new ColumnConstraints(List.of(new ScalarColumnConstraint.Raw(COLUMN, Operator.GT, "5").prepare()));
        ColumnConstraints b = new ColumnConstraints(List.of(new ScalarColumnConstraint.Raw(ColumnIdentifier.getInterned("other", true), Operator.GT, "5").prepare()));
        assertEqualColumnMetadata(COLUMN, a, b);
    }

    @Test
    public void testDifferentConstraintsAreNotEqual()
    {
        Supplier<List<ColumnConstraints>> supplier = () -> List.of(prepare(length(Operator.GT, "10")),
                                                                   prepare(length(Operator.LT, "10")),
                                                                   prepare(length(Operator.GT, "11")),
                                                                   prepare(new FunctionColumnConstraint.Raw(new ColumnIdentifier("octet_length", true), List.of(), Operator.GT, "10").prepare()),
                                                                   prepare(new ScalarColumnConstraint.Raw(COLUMN, Operator.GT, "10").prepare()),
                                                                   prepare(new ScalarColumnConstraint.Raw(COLUMN, Operator.GT, "11").prepare()),
                                                                   prepare(new UnaryFunctionColumnConstraint.Raw(new ColumnIdentifier("json", true)).prepare()),
                                                                   prepare(new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare()),
                                                                   ColumnConstraints.NO_OP);

        // the same constraints, created twice, so no object in one list is in the other one, except NO_OP
        List<ColumnConstraints> first = supplier.get();
        List<ColumnConstraints> second = supplier.get();

        Set<Integer> hashCodes = new HashSet<>();
        for (int i = 0; i < first.size(); i++)
        {
            hashCodes.add(first.get(i).hashCode());
            for (int j = 0; j < second.size(); j++)
            {
                if (i == j)
                {
                    assertEquals(first.get(i), second.get(j));
                    assertEquals(first.get(i).hashCode(), second.get(j).hashCode());
                }
                else
                {
                    assertNotEquals(first.get(i), second.get(j));
                }
            }
        }

        // equal hash codes of unequal constraints are allowed, a hash code which does not depend on the constraint is not
        assertTrue("Hash codes of different constraints should differ, got " + hashCodes, hashCodes.size() > first.size() / 2);
    }

    @Test
    public void testSameConstraintOnDifferentColumnsIsNotEqual()
    {
        ColumnConstraints onV = new ColumnConstraints.Raw(List.of(length(Operator.GT, "10"))).prepare(COLUMN);
        ColumnConstraints onOther = new ColumnConstraints.Raw(List.of(length(Operator.GT, "10"))).prepare(ColumnIdentifier.getInterned("other", true));
        assertNotEquals(onV, onOther);
    }

    @Test
    public void testMultipleConstraintsHaveEqualHashCode()
    {
        Supplier<ColumnConstraints> constraints = () -> new ColumnConstraints.Raw(List.of(length(Operator.GT, "10"),
                                                                                           length(Operator.LT, "100"),
                                                                                           new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare())).prepare(COLUMN);
        ColumnConstraints a = constraints.get();
        ColumnConstraints b = constraints.get();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEqualColumnMetadata(COLUMN, a, b);

        assertNotEquals(a, prepare(length(Operator.GT, "10")));
    }

    @Test
    public void testNoConstraintsAreEqualToNoOp()
    {
        ColumnConstraints empty = new ColumnConstraints(List.of());
        assertEquals(ColumnConstraints.NO_OP, empty);
        assertEquals(empty, ColumnConstraints.NO_OP);
        assertEquals(ColumnConstraints.NO_OP.hashCode(), empty.hashCode());
        assertSame(ColumnConstraints.NO_OP, new ColumnConstraints.Raw().prepare(COLUMN));
        assertEqualColumnMetadata(COLUMN, ColumnConstraints.NO_OP, empty);
    }

    /**
     * Constraints which were deserialized have no column name until they are given to a column.
     */
    @Test
    public void testConstraintsWithoutColumnNameCanBeCompared() throws IOException
    {
        for (ColumnConstraint<?> constraint : List.of(length(Operator.GT, "10"),
                                                      new UnaryFunctionColumnConstraint.Raw(new ColumnIdentifier("json", true)).prepare(),
                                                      new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare()))
        {
            ColumnConstraints original = new ColumnConstraints.Raw(List.of(constraint)).prepare(COLUMN);
            ColumnConstraints first = roundTrip(original, Version.V6);
            ColumnConstraints second = roundTrip(original, Version.V6);

            assertEquals(first, second);
            assertEquals(first.hashCode(), second.hashCode());
            assertNotEquals(original, first);
            assertNotEquals(first, original);
        }
    }

    @Test
    public void testCopyIsEqualAndSharesNoConstraint()
    {
        ColumnConstraints original = new ColumnConstraints.Raw(List.of(length(Operator.GT, "10"),
                                                                       new FunctionColumnConstraint.Raw(new ColumnIdentifier("regexp", true), List.of(), Operator.EQ, "'a.*'").prepare(),
                                                                       new ScalarColumnConstraint.Raw(COLUMN, Operator.GT, "5").prepare(),
                                                                       new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare())).prepare(COLUMN);
        ColumnConstraints copy = original.copy();

        assertNotSame(original, copy);
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        for (int i = 0; i < original.getSize(); i++)
            assertNotSame(original.getConstraints().get(i), copy.getConstraints().get(i));

        copy.setColumnName(ColumnIdentifier.getInterned("other", true));
        assertNotEquals(original, copy);
        assertEquals(original, original.copy());

        assertSame(ColumnConstraints.NO_OP, ColumnConstraints.NO_OP.copy());
    }

    @Test
    public void testRenamedColumnDoesNotChangeConstraintsOfOriginalColumn()
    {
        ColumnMetadata original = columnMetadata(COLUMN, prepare(length(Operator.GT, "10")));
        ColumnMetadata renamed = original.withNewName(ColumnIdentifier.getInterned("other", true));

        // hashed only after the rename, so the hash code is not one cached before it
        ColumnMetadata expected = columnMetadata(COLUMN, prepare(length(Operator.GT, "10")));
        assertEquals(expected, original);
        assertEquals(expected.hashCode(), original.hashCode());

        assertNotSame(original.getColumnConstraints(), renamed.getColumnConstraints());
        assertNotEquals(original.getColumnConstraints(), renamed.getColumnConstraints());
        assertEquals(new ColumnConstraints.Raw(List.of(length(Operator.GT, "10"))).prepare(ColumnIdentifier.getInterned("other", true)),
                     renamed.getColumnConstraints());
    }

    @Test
    public void testColumnWithNewTypeDoesNotShareConstraintsWithOriginalColumn()
    {
        ColumnConstraints constraints = new ColumnConstraints.Raw(List.of(new ScalarColumnConstraint.Raw(COLUMN, Operator.GT, "5").prepare())).prepare(COLUMN);
        ColumnMetadata original = new ColumnMetadata("ks", "tb", COLUMN, Int32Type.instance, 0, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null, constraints);
        ColumnMetadata withNewType = original.withNewType(UTF8Type.instance);

        assertSame(constraints, original.getColumnConstraints());
        assertNotSame(original.getColumnConstraints(), withNewType.getColumnConstraints());
        assertEquals(original.getColumnConstraints(), withNewType.getColumnConstraints());

        // the constraint of the original column is still prepared for an int
        original.getColumnConstraints().evaluate(Int32Type.instance, Int32Type.instance.decompose(6));
    }

    /**
     * Constraints which were deserialized, and which were not given to a column yet, have no column name on the group,
     * while a scalar constraint in that group has one. A copy has to keep it.
     */
    @Test
    public void testCopyOfDeserializedConstraintsIsEqual() throws IOException
    {
        ColumnConstraints original = new ColumnConstraints.Raw(List.of(new ScalarColumnConstraint.Raw(COLUMN, Operator.GT, "5").prepare(),
                                                                       length(Operator.GT, "10"),
                                                                       new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare())).prepare(COLUMN);
        ColumnConstraints deserialized = roundTrip(original, Version.V6);
        ColumnConstraints copy = deserialized.copy();

        assertNotSame(deserialized, copy);
        assertEquals(deserialized, copy);
        assertEquals(deserialized.hashCode(), copy.hashCode());
        for (int i = 0; i < deserialized.getSize(); i++)
            assertNotSame(deserialized.getConstraints().get(i), copy.getConstraints().get(i));

        assertEqualColumnMetadata(COLUMN, original, copy);
    }

    @Test
    public void testColumnWithNewTypeHasItsFunctionConstraintsPrepared()
    {
        ColumnConstraints constraints = new ColumnConstraints.Raw(List.of(length(Operator.GT, "3"),
                                                                          new FunctionColumnConstraint.Raw(new ColumnIdentifier("regexp", true), List.of(), Operator.EQ, "'a.*'").prepare())).prepare(COLUMN);
        ColumnMetadata original = columnMetadata(COLUMN, constraints);
        ColumnMetadata withNewType = original.withNewType(AsciiType.instance);

        assertNotSame(original.getColumnConstraints(), withNewType.getColumnConstraints());
        assertEquals(original.getColumnConstraints(), withNewType.getColumnConstraints());

        // the term of a function constraint is prepared in a copy as well, so both of them enforce the same
        for (ColumnMetadata column : List.of(original, withNewType))
        {
            column.getColumnConstraints().evaluate(column.type, column.type.fromString("abcd"));

            assertThatThrownBy(() -> column.getColumnConstraints().evaluate(column.type, column.type.fromString("abc")))
            .isInstanceOf(ConstraintViolationException.class)
            .hasMessageContaining("It has a length of 3");

            assertThatThrownBy(() -> column.getColumnConstraints().evaluate(column.type, column.type.fromString("bcde")))
            .isInstanceOf(ConstraintViolationException.class)
            .hasMessageContaining("does not match regular expression");
        }
    }

    private static ColumnConstraint<?> length(Operator operator, String term)
    {
        return new FunctionColumnConstraint.Raw(new ColumnIdentifier("length", true), List.of(), operator, term).prepare();
    }

    private static ColumnConstraints roundTrip(ColumnConstraints constraints, Version version) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        ColumnConstraints.serializer.serialize(constraints, out, version);
        return ColumnConstraints.serializer.deserialize(new DataInputBuffer(out.getData()), version);
    }

    private static void assertEqualHashCode(Supplier<ColumnConstraint<?>> constraint)
    {
        ColumnConstraints a = prepare(constraint.get());
        ColumnConstraints b = prepare(constraint.get());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    private static void assertRoundTrip(ColumnIdentifier column, ColumnConstraint<?> constraint) throws IOException
    {
        ColumnConstraints original = new ColumnConstraints.Raw(List.of(constraint)).prepare(column);
        for (Version version : Version.values())
        {
            if (version == Version.UNKNOWN || !version.isAtLeast(Version.V6))
                continue;

            DataOutputBuffer out = new DataOutputBuffer();
            ColumnConstraints.serializer.serialize(original, out, version);
            ColumnConstraints read = ColumnConstraints.serializer.deserialize(new DataInputBuffer(out.getData()), version);
            assertEqualColumnMetadata(column, original, read);
        }
    }

    private static void assertEqualColumnMetadata(ColumnIdentifier column, ColumnConstraints a, ColumnConstraints b)
    {
        ColumnMetadata first = columnMetadata(column, a);
        ColumnMetadata second = columnMetadata(column, b);
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    private static ColumnMetadata columnMetadata(ColumnIdentifier column, ColumnConstraints constraints)
    {
        return new ColumnMetadata("ks", "tb", column, UTF8Type.instance, 0, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null, constraints);
    }

    private static ColumnConstraints prepare(ColumnConstraint<?> constraint)
    {
        return new ColumnConstraints.Raw(List.of(constraint)).prepare(COLUMN);
    }
}
