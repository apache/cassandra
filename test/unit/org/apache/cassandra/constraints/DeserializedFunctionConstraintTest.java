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

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint.Raw;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.schema.ColumnMetadata.Kind.REGULAR;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Constraints loaded from a ClusterMetadata snapshot go through {@link FunctionColumnConstraint.Serializer#deserialize}
 * and are never passed to validate(). They must behave exactly like the constraint that was originally created
 * through the DDL path, which does call validate().
 */
public class DeserializedFunctionConstraintTest
{
    private static final ColumnIdentifier columnIdentifier = new ColumnIdentifier("a_column", false);
    private static final ColumnMetadata column = new ColumnMetadata("ks", "tbl", columnIdentifier, UTF8Type.instance, ColumnMetadata.NO_UNIQUE_ID, -1, REGULAR, null);

    @Test
    public void lengthLessThan() throws IOException
    {
        assertBehavesAfterRoundTrip("LENGTH", Operator.LT, "5", "abc", "abcdefgh");
    }

    @Test
    public void octetLengthLessThan() throws IOException
    {
        assertBehavesAfterRoundTrip("OCTET_LENGTH", Operator.LT, "5", "abc", "abcdefgh");
    }

    @Test
    public void regexp() throws IOException
    {
        assertBehavesAfterRoundTrip("REGEXP", Operator.EQ, "'a..b'", "acdb", "aaaaaaa");
    }

    /**
     * Control: the validated constraint must behave correctly, so any failure on the deserialized copy
     * is attributable to serialization alone and not to a missing validate().
     */
    private static void assertBehavesAfterRoundTrip(String function, Operator operator, String term, String accepted, String rejected) throws IOException
    {
        FunctionColumnConstraint original = new Raw(new ColumnIdentifier(function, false), List.of(), operator, term).prepare();
        original.setColumnName(columnIdentifier);
        original.validate(column);
        assertBehaves("validated original", original, accepted, rejected);

        FunctionColumnConstraint deserialized = roundTrip(original);
        assertBehaves("deserialized copy", deserialized, accepted, rejected);
    }

    private static void assertBehaves(String description, FunctionColumnConstraint constraint, String accepted, String rejected)
    {
        assertThatCode(() -> evaluate(constraint, accepted)).as("%s must accept '%s'", description, accepted).doesNotThrowAnyException();
        assertThatThrownBy(() -> evaluate(constraint, rejected)).as("%s must reject '%s'", description, rejected).isInstanceOf(ConstraintViolationException.class);
    }

    private static void evaluate(FunctionColumnConstraint constraint, String value)
    {
        constraint.evaluate(UTF8Type.instance, UTF8Type.instance.fromString(value));
    }

    private static FunctionColumnConstraint roundTrip(FunctionColumnConstraint original) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            FunctionColumnConstraint.serializer.serialize(original, out, Version.V7);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                FunctionColumnConstraint deserialized = FunctionColumnConstraint.serializer.deserialize(in, Version.V7);
                deserialized.setColumnName(columnIdentifier);
                return deserialized;
            }
        }
    }
}
