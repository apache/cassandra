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
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.UnaryFunctionColumnConstraint.Raw;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

import static java.util.List.of;
import static org.apache.cassandra.cql3.constraints.JsonConstraint.FUNCTION_NAME;
import static org.apache.cassandra.schema.ColumnMetadata.Kind.REGULAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonConstraintTest
{
    private static final ColumnIdentifier columnIdentifier = new ColumnIdentifier("a_column", false);
    private static final ColumnIdentifier jsonFunctionIdentifier = new ColumnIdentifier(FUNCTION_NAME, false);
    private static final ColumnMetadata regularStringColumn = getColumnOfType(UTF8Type.instance);
    private static final ColumnMetadata regularAsciiColumn = getColumnOfType(AsciiType.instance);

    private static final ColumnConstraints json = new ColumnConstraints(of(new Raw(jsonFunctionIdentifier, columnIdentifier).prepare()));

    @Test
    public void testJsonConstraint() throws Throwable
    {
        run("{}");
        run("{\"a\": 5, \"b\": \"1\", \"c\": [1,2,3]}");
        run("nonsense", "Value for column 'a_column' violated JSON constraint as it is not a valid JSON.");
        run("", "Column value does not satisfy value constraint for column 'a_column' as it is null.");
    }

    @Test
    public void testInvalidTypes()
    {
        assertThatThrownBy(() -> json.validate(getColumnOfType(IntegerType.instance)))
        .hasMessage("Constraint 'JSON' can be used only for columns of type " +
                    "[org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.AsciiType] " +
                    "but it was class org.apache.cassandra.db.marshal.IntegerType");
    }

    private void run(String jsonToCheck) throws Throwable
    {
        run(jsonToCheck, null);
    }

    private void run(String jsonToCheck, String exceptionMessage) throws Throwable
    {
        ThrowingCallable callable = () ->
        {
            json.validate(regularStringColumn);
            json.evaluate(regularStringColumn.type, regularAsciiColumn.type.fromString(jsonToCheck));

            json.validate(regularAsciiColumn);
            json.evaluate(regularAsciiColumn.type, regularAsciiColumn.type.fromString(jsonToCheck));
        };

        if (exceptionMessage == null)
            callable.call();
        else
            assertThatThrownBy(callable).hasMessageContaining(exceptionMessage);
    }

    private static ColumnMetadata getColumnOfType(AbstractType<?> type)
    {
        return new ColumnMetadata("a", "b", columnIdentifier, type, -1, REGULAR, null);
    }
}
