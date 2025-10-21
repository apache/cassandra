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

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint.Raw;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.cql3.constraints.RegexpConstraint;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.assertj.core.api.ThrowableAssert;

import static java.util.List.of;
import static org.apache.cassandra.schema.ColumnMetadata.Kind.REGULAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RegexpConstraintTest
{
    private static final ColumnIdentifier columnIdentifier = new ColumnIdentifier("a_column", false);
    private static final ColumnIdentifier regexpFunctionIdentifier = new ColumnIdentifier(RegexpConstraint.FUNCTION_NAME, false);
    private static final ColumnMetadata regularStringColumn = getColumnOfType(UTF8Type.instance);
    private static final ColumnMetadata regularAsciiColumn = getColumnOfType(AsciiType.instance);

    private static final ColumnConstraints regexp = new ColumnConstraints(of(new Raw(regexpFunctionIdentifier, List.of(), Operator.EQ, "'a..b'").prepare()));
    private static final ColumnConstraints negatedRegexp = new ColumnConstraints(of(new Raw(regexpFunctionIdentifier, List.of(), Operator.NEQ, "'a..b'").prepare()));

    @Test
    public void testRegexpConstraint() throws Throwable
    {
        run(regexp, "acdb");
        run(regexp, "aaaaaaa", "Value does not match regular expression 'a..b'");
        run(negatedRegexp, "acdb", "Value does match regular expression 'a..b'");
        run(negatedRegexp, "aaaaa");
    }

    @Test
    public void testInvalidPattern()
    {
        ColumnConstraints invalid = new ColumnConstraints(of(new Raw(regexpFunctionIdentifier, List.of(), Operator.EQ, "'*abc'").prepare()));
        assertThatThrownBy(() -> invalid.validate(regularStringColumn))
        .hasMessage("String '*abc' is not a valid regular expression")
        .isInstanceOf(InvalidConstraintDefinitionException.class);
    }

    @Test
    public void testInvalidTypes()
    {
        assertThatThrownBy(() -> regexp.validate(getColumnOfType(IntegerType.instance)))
        .hasMessage("Constraint 'REGEXP' can be used only for columns of type " +
                    "[org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.AsciiType] " +
                    "but it was class org.apache.cassandra.db.marshal.IntegerType");
    }

    private void run(ColumnConstraints regexp, String input) throws Throwable
    {
        run(regexp, input, null);
    }

    private void run(ColumnConstraints regexp, String input, String exceptionMessage) throws Throwable
    {
        ThrowableAssert.ThrowingCallable callable = () ->
        {
            regexp.validate(regularStringColumn);
            regexp.evaluate(regularStringColumn.type, regularStringColumn.type.fromString(input));

            regexp.validate(regularAsciiColumn);
            regexp.evaluate(regularAsciiColumn.type, regularAsciiColumn.type.fromString(input));
        };

        if (exceptionMessage == null)
            callable.call();
        else
            assertThatThrownBy(callable).hasMessageContaining(exceptionMessage);
    }

    private static ColumnMetadata getColumnOfType(AbstractType<?> type)
    {
        return new ColumnMetadata("a", "b", columnIdentifier, type, ColumnMetadata.NO_UNIQUE_ID, -1, REGULAR, null);
    }
}
