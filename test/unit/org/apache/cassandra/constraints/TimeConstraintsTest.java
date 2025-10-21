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

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.cql3.constraints.ScalarColumnConstraint.Raw;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.schema.ColumnMetadata;

import static java.util.List.of;
import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.cql3.Operator.GTE;
import static org.apache.cassandra.cql3.Operator.LT;
import static org.apache.cassandra.cql3.Operator.LTE;
import static org.apache.cassandra.cql3.Operator.NEQ;
import static org.apache.cassandra.cql3.functions.types.ParseUtils.quote;
import static org.apache.cassandra.schema.ColumnMetadata.Kind.REGULAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TimeConstraintsTest
{
    private static final ColumnIdentifier columnIdentifier = new ColumnIdentifier("a_column", false);
    private static final ColumnMetadata timeColumn = getColumnOfType(TimeType.instance);
    private static final ColumnMetadata dateColumn = getColumnOfType(SimpleDateType.instance);
    private static final ColumnMetadata timestampColumn = getColumnOfType(TimestampType.instance);

    @Test
    public void testTimeConstraint()
    {
        evaluateTime(EQ, "12:00:00", "12:00:00");
        evaluateTime(NEQ, "12:00:00", "11:00:00");
        evaluateTime(LT, "12:00:00", "11:00:00");
        evaluateTime(GT, "12:00:00", "13:00:00");
        evaluateTime(GT, "12:00:00", "12:00:00.1234");
        evaluateTime(GTE, "12:00:00", "12:00:00");
        evaluateTime(LTE, "12:00:00", "12:00:00");

        assertThatThrownBy(() -> evaluateTime(GT, "12:00:00", "01:00:00"))
        .isInstanceOf(ConstraintViolationException.class);

        assertThatThrownBy(() -> evaluateTime(LT, "12:00:00", "13:00:00"))
        .isInstanceOf(ConstraintViolationException.class);

        assertThatThrownBy(() -> evaluateTime(LT, "-3:00:00", "13:00:00"))
        .isInstanceOf(ConstraintViolationException.class)
        .hasMessage(cantParse("-3:00:00"));

        evaluate(timeColumn, TimeType.instance, GT, "06:00:00", LT, "15:00:00", "12:00:00");

        assertThatThrownBy(() -> evaluate(timeColumn, TimeType.instance, GT, "06:00:00", LT, "15:00:00", "18:00:00"))
        .isInstanceOf(ConstraintViolationException.class)
        .hasMessage("Column value does not satisfy value constraint for column 'a_column'. It should be a_column < '15:00:00'");

        assertThatThrownBy(() -> evaluate(timeColumn, TimeType.instance, GT, "06:00:00", LT, "03:00:00", "18:00:00"))
        .isInstanceOf(InvalidConstraintDefinitionException.class)
        .hasMessage("Constraints of scalar are not satisfiable: a_column > '06:00:00', a_column < '03:00:00'");
    }

    @Test
    public void testDateConstraint()
    {
        evaluateDate(EQ, "2000-01-01", "2000-01-01");
        evaluateDate(NEQ, "2000-01-01", "1999-12-31");
        evaluateDate(LT, "2000-01-01", "1999-12-31");
        evaluateDate(GT, "2000-01-01", "2000-01-02");
        evaluateDate(GTE, "2000-01-01", "2000-01-01");
        evaluateDate(LTE, "2000-01-01", "2000-01-01");

        assertThatThrownBy(() -> evaluateDate(GT, "2000-01-01", "1999-12-31"))
        .isInstanceOf(ConstraintViolationException.class);

        assertThatThrownBy(() -> evaluateDate(LT, "2000-01-01", "2000-01-02"))
        .isInstanceOf(ConstraintViolationException.class);

        assertThatThrownBy(() -> evaluateDate(LT, "2000-54-01", "13:00:00"))
        .isInstanceOf(ConstraintViolationException.class)
        .hasMessage(cantParse("2000-54-01"));

        evaluate(dateColumn, SimpleDateType.instance, GT, "2000-01-01", LT, "2000-01-31", "2000-01-10");

        assertThatThrownBy(() -> evaluate(dateColumn, SimpleDateType.instance, GT, "2000-01-01", LT, "2000-01-31", "2000-02-10"))
        .isInstanceOf(ConstraintViolationException.class)
        .hasMessage("Column value does not satisfy value constraint for column 'a_column'. It should be a_column < '2000-01-31'");

        assertThatThrownBy(() -> evaluate(dateColumn, SimpleDateType.instance, GT, "2000-01-31", LT, "2000-01-01", "2000-01-10"))
        .isInstanceOf(InvalidConstraintDefinitionException.class)
        .hasMessage("Constraints of scalar are not satisfiable: a_column > '2000-01-31', a_column < '2000-01-01'");
    }

    @Test
    public void testTimestampConstraint()
    {
        evaluateTimestamp(EQ, "2025-03-18 12:34:56", "2025-03-18 12:34:56");
        evaluateTimestamp(NEQ, "2025-03-18 12:34:56", "2025-03-18 12:34:55");
        evaluateTimestamp(LT, "2025-03-18 12:34:56", "2025-03-18 12:34:55");
        evaluateTimestamp(GT, "2025-03-18 12:34:56", "2025-03-18 12:34:57");
        evaluateTimestamp(GTE, "2025-03-18 12:34:56", "2025-03-18 12:34:56");
        evaluateTimestamp(LTE, "2025-03-18 12:34:56", "2025-03-18 12:34:56");

        assertThatThrownBy(() -> evaluateTimestamp(GT, "2025-03-18 12:34:56", "2025-03-18 12:34:55"))
        .isInstanceOf(ConstraintViolationException.class);

        assertThatThrownBy(() -> evaluateTimestamp(LT, "2025-03-18 12:34:56", "2025-03-18 12:34:57"))
        .isInstanceOf(ConstraintViolationException.class);

        assertThatThrownBy(() -> evaluateTimestamp(LT, "2025-55-18 12:34:56", "13:00:00"))
        .isInstanceOf(ConstraintViolationException.class)
        .hasMessage(cantParse("2025-55-18 12:34:56"));

        evaluate(timestampColumn, TimestampType.instance, GT, "2025-03-18 12:34:56", LT, "2025-03-18 12:35:56", "2025-03-18 12:35:12");

        assertThatThrownBy(() -> evaluate(timestampColumn, TimestampType.instance, GT, "2025-03-18 12:34:56", LT, "2025-03-18 12:35:56", "2025-03-18 13:35:12"))
        .isInstanceOf(ConstraintViolationException.class)
        .hasMessage("Column value does not satisfy value constraint for column 'a_column'. It should be a_column < '2025-03-18 12:35:56'");

        assertThatThrownBy(() -> evaluate(timestampColumn, TimestampType.instance, GT, "2025-03-18 12:34:56", LT, "2025-03-18 12:33:56", "2025-03-18 12:35:12"))
        .isInstanceOf(InvalidConstraintDefinitionException.class)
        .hasMessage("Constraints of scalar are not satisfiable: a_column > '2025-03-18 12:34:56', a_column < '2025-03-18 12:33:56'");
    }

    private void evaluate(ColumnMetadata columnMetadata, AbstractType<?> type, Operator operator1, String term1, Operator operator2, String term2, String value)
    {
        ColumnConstraints constraint = new ColumnConstraints(of(new Raw(columnIdentifier, operator1, quote(term1)).prepare(),
                                                                new Raw(columnIdentifier, operator2, quote(term2)).prepare()));
        constraint.validate(columnMetadata);
        constraint.evaluate(type, type.fromString(value));
    }

    private void evaluateTime(Operator operator, String term, String value)
    {
        evaluate(TimeType.instance, timeColumn, operator, quote(term), value);
    }

    private void evaluateDate(Operator operator, String term, String value)
    {
        evaluate(SimpleDateType.instance, dateColumn, operator, quote(term), value);
    }

    private void evaluateTimestamp(Operator operator, String term, String value)
    {
        evaluate(TimestampType.instance, timestampColumn, operator, quote(term), value);
    }

    private void evaluate(AbstractType<?> type, ColumnMetadata columnMetadata, Operator operator, String term, String value)
    {
        ColumnConstraints constraint = new ColumnConstraints(of(new Raw(columnIdentifier, operator, term).prepare()));
        constraint.validate(columnMetadata);

        constraint.evaluate(type, type.fromString(value));
    }

    private String cantParse(String value)
    {
        return "Cannot parse constraint value from '" + value + "' for column '" + columnIdentifier + '\'';
    }

    private static ColumnMetadata getColumnOfType(AbstractType<?> type)
    {
        return new ColumnMetadata("a", "b", columnIdentifier, type, ColumnMetadata.NO_UNIQUE_ID, -1, REGULAR, null);
    }
}
