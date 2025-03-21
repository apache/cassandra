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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.cql3.constraints.NotNullConstraint;
import org.apache.cassandra.cql3.constraints.ScalarColumnConstraint;
import org.apache.cassandra.cql3.constraints.UnaryFunctionColumnConstraint;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.ColumnMetadata;

import static java.util.List.of;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.schema.ColumnMetadata.Kind.REGULAR;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TODO - UDTs are not supported yet in constraints as such
 */
public class NotNullConstraintTest
{
    private static final ColumnIdentifier columnIdentifier = new ColumnIdentifier("a_column", false);
    private static final ColumnConstraints unaryConstraint = new ColumnConstraints(of(new UnaryFunctionColumnConstraint.Raw(new ColumnIdentifier(NotNullConstraint.FUNCTION_NAME, false), List.of()).prepare()));
    private static final ColumnConstraints scalarConstraint = new ColumnConstraints(of(new ScalarColumnConstraint.Raw(columnIdentifier, GT, "5").prepare()));
    private static final ColumnConstraints functionConstraint = new ColumnConstraints(of(new FunctionColumnConstraint.Raw(new ColumnIdentifier("LENGTH", false), List.of(), GT, "5").prepare()));

    @Test
    public void testNotNullConstraintValidation()
    {
        unaryConstraint.setColumnName(columnIdentifier);
        scalarConstraint.setColumnName(columnIdentifier);
        functionConstraint.setColumnName(columnIdentifier);
        // unary
        unaryConstraint.validate(getColumnOfType(UTF8Type.instance));
        assertThatThrownBy(() -> unaryConstraint.evaluate(UTF8Type.instance, EMPTY_BYTE_BUFFER))
        .hasMessage("Column value does not satisfy value constraint for column 'a_column' as it is null.")
        .isInstanceOf(ConstraintViolationException.class);

        // not null / empty
        unaryConstraint.evaluate(UTF8Type.instance, UTF8Type.instance.fromString("a value"));

        // scalar
        scalarConstraint.validate(getColumnOfType(Int32Type.instance));
        assertThatThrownBy(() -> scalarConstraint.evaluate(Int32Type.instance, EMPTY_BYTE_BUFFER))
        .hasMessage("Column value does not satisfy value constraint for column 'a_column' as it is null.")
        .isInstanceOf(ConstraintViolationException.class);

        // function, e.g. length
        functionConstraint.validate(getColumnOfType(UTF8Type.instance));
        assertThatThrownBy(() -> functionConstraint.evaluate(UTF8Type.instance, EMPTY_BYTE_BUFFER))
        .hasMessage("Column value does not satisfy value constraint for column 'a_column' as it is null.")
        .isInstanceOf(ConstraintViolationException.class);

        // empty string is not _null_ string so this passes
        unaryConstraint.evaluate(UTF8Type.instance, UTF8Type.instance.fromString(""));

        // test a type for which empty value is meaningless

        assertThatThrownBy(() -> unaryConstraint.evaluate(UUIDType.instance, ByteBuffer.allocate(0)))
        .hasMessage("Column value does not satisfy value constraint for column 'a_column' as it is empty.")
        .isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    public void testCollections()
    {
        unaryConstraint.setColumnName(columnIdentifier);
        scalarConstraint.setColumnName(columnIdentifier);
        functionConstraint.setColumnName(columnIdentifier);
        checkList(false);
        checkSet(false);
        checkMap(false);

        checkList(true);
        checkSet(true);
        checkMap(true);
    }

    private static ColumnMetadata getColumnOfType(AbstractType<?> type)
    {
        return new ColumnMetadata("a", "b", columnIdentifier, type, -1, -1, REGULAR, null);
    }

    private void checkList(boolean frozen)
    {
        if (frozen)
        {
            ListType<Integer> listType = ListType.getInstance(Int32Type.instance, false);
            ByteBuffer payload = listType.getSerializer().serialize(List.of(1, 2, 3));
            checkFrozenCollection(listType, payload);
        }
        else
            checkUnfrozenCollection(ListType.getInstance(Int32Type.instance, true));
    }

    private void checkMap(boolean frozen)
    {
        if (frozen)
        {
            MapType<Integer, Integer> mapType = MapType.getInstance(Int32Type.instance, Int32Type.instance, false);
            ByteBuffer payload = mapType.getSerializer().serialize(Map.of(1, 1, 2, 2, 3, 3));
            checkFrozenCollection(mapType, payload);
        }
        else
            checkUnfrozenCollection(MapType.getInstance(Int32Type.instance, Int32Type.instance, true));
    }

    private void checkSet(boolean frozen)
    {
        if (frozen)
        {
            SetType<Integer> setType = SetType.getInstance(Int32Type.instance, false);
            ByteBuffer payload = setType.getSerializer().serialize(Set.of(1, 2, 3));
            checkFrozenCollection(setType, payload);
        }
        else
            checkUnfrozenCollection(SetType.getInstance(Int32Type.instance, true));
    }

    private void checkFrozenCollection(AbstractType<?> type, ByteBuffer payload)
    {
        unaryConstraint.validate(getColumnOfType(type));
        unaryConstraint.evaluate(type, payload);

        assertThatThrownBy(() -> unaryConstraint.evaluate(type, EMPTY_BYTE_BUFFER))
        .hasMessage("Column value does not satisfy value constraint for column 'a_column' as it is null.")
        .isInstanceOf(ConstraintViolationException.class);
    }

    private void checkUnfrozenCollection(AbstractType<?> type)
    {
        assertThatThrownBy(() -> unaryConstraint.validate(getColumnOfType(type)))
        .hasMessageContaining("Constraint cannot be defined on the column")
        .hasMessageContaining("When using collections, constraints can be used only of frozen collections")
        .isInstanceOf(InvalidConstraintDefinitionException.class);
    }
}
