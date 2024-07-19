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

package org.apache.cassandra.harry.model;

import java.util.*;
import java.util.function.Supplier;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.model.reconciler.PartitionState;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.model.reconciler.Reconciler;
import org.apache.cassandra.harry.tracker.DataTracker;

import static org.apache.cassandra.harry.gen.DataGenerators.NIL_DESCR;
import static org.apache.cassandra.harry.gen.DataGenerators.UNSET_DESCR;

public class QuiescentChecker implements Model
{
    protected final OpSelectors.Clock clock;

    protected final DataTracker tracker;
    protected final SystemUnderTest sut;
    protected final Reconciler reconciler;
    protected final SchemaSpec schema;

    public QuiescentChecker(Run run)
    {
        this(run, new Reconciler(run));
    }

    public QuiescentChecker(Run run, Reconciler reconciler)
    {
        this(run.clock, run.sut, run.tracker, run.schemaSpec, reconciler);
    }

    public QuiescentChecker(OpSelectors.Clock clock,
                            SystemUnderTest sut,
                            DataTracker tracker,
                            SchemaSpec schema,
                            Reconciler reconciler)
    {
        this.clock = clock;
        this.sut = sut;
        this.reconciler = reconciler;
        this.tracker = tracker;
        this.schema = schema;
    }

    public void validate(Query query)
    {
        tracker.beginValidation(query.pd);
        validate(() -> SelectHelper.execute(sut, clock, query), query);
        tracker.endValidation(query.pd);
    }

    protected void validate(Supplier<List<ResultSetRow>> rowsSupplier, Query query)
    {
        List<ResultSetRow> actualRows = rowsSupplier.get();
        PartitionState partitionState = reconciler.inflatePartitionState(query.pd, tracker, query);
        validate(schema, tracker, partitionState, actualRows, query);
    }

    public static void validate(SchemaSpec schema, DataTracker tracker, PartitionState partitionState, List<ResultSetRow> actualRows, Query query)
    {
        Set<ColumnSpec<?>> columns = new HashSet<>();
        columns.addAll(schema.allColumns);
        validate(schema, tracker, columns, partitionState, actualRows, query);
    }

    public static Reconciler.RowState adjustForSelection(Reconciler.RowState row, SchemaSpec schema, Set<ColumnSpec<?>> selection, boolean isStatic)
    {
        if (selection.size() == schema.allColumns.size())
            return row;

        List<ColumnSpec<?>> columns = isStatic ? schema.staticColumns : schema.regularColumns;
        Reconciler.RowState newRowState = row.clone();
        assert newRowState.vds.length == columns.size();
        for (int i = 0; i < columns.size(); i++)
        {
            if (!selection.contains(columns.get(i)))
            {
                newRowState.vds[i] = UNSET_DESCR;
                newRowState.lts[i] = NO_TIMESTAMP;
            }
        }
        return newRowState;
    }

    public static void validate(SchemaSpec schema, DataTracker tracker, Set<ColumnSpec<?>> selection, PartitionState partitionState, List<ResultSetRow> actualRows, Query query)
    {
        boolean isWildcardQuery = selection == null;
        String trackerBefore = tracker.toString();
        if (isWildcardQuery)
            selection = new HashSet<>(schema.allColumns);

        Iterator<ResultSetRow> actual = actualRows.iterator();
        Collection<Reconciler.RowState> expectedRows = partitionState.rows(query.reverse);

        Iterator<Reconciler.RowState> expected = expectedRows.iterator();

        String trackerState = String.format("Tracker before: %s, Tracker after: %s", trackerBefore, tracker);

        // It is possible that we only get a single row in response, and it is equal to static row
        if (partitionState.isEmpty() && partitionState.staticRow() != null && actual.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            if (actualRowState.cd != UNSET_DESCR && actualRowState.cd != partitionState.staticRow().cd)
            {
                throw new ValidationException(trackerState,
                                              partitionState.toString(schema),
                                              toString(actualRows),
                                              "Found a row while model predicts statics only:" +
                                              "\nExpected: %s" +
                                              "\nActual: %s" +
                                              "\nQuery: %s",
                                              partitionState.staticRow(),
                                              actualRowState,
                                              query.toSelectStatement());
            }

            for (int i = 0; i < actualRowState.vds.length; i++)
            {
                if (actualRowState.vds[i] != NIL_DESCR || actualRowState.lts[i] != NO_TIMESTAMP)
                    throw new ValidationException(trackerState,
                                                  partitionState.toString(schema),
                                                  toString(actualRows),
                                                  "Found a row while model predicts statics only:" +
                                                  "\nActual: %s" +
                                                  "\nQuery: %s",
                                                  actualRowState, query.toSelectStatement());
            }

            assertStaticRow(partitionState, actualRows,
                            adjustForSelection(partitionState.staticRow(), schema, selection, true),
                            actualRowState, query, trackerState, schema);
        }

        while (actual.hasNext() && expected.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            Reconciler.RowState originalExpectedRowState = expected.next();
            Reconciler.RowState expectedRowState = adjustForSelection(originalExpectedRowState, schema, selection, false);

            if (schema.trackLts)
                partitionState.compareVisitedLts(actualRowState.visited_lts);

            // TODO: this is not necessarily true. It can also be that ordering is incorrect.
            if (actualRowState.cd != UNSET_DESCR && actualRowState.cd != expectedRowState.cd)
            {
                throw new ValidationException(trackerState,
                                              partitionState.toString(schema),
                                              toString(actualRows),
                                              "Found a row in the model that is not present in the resultset:" +
                                              "\nExpected: %s" +
                                              "\nActual: %s" +
                                              "\nQuery: %s",
                                              expectedRowState.toString(schema),
                                              actualRowState, query.toSelectStatement());
            }

            if (!Arrays.equals(expectedRowState.vds, actualRowState.vds))
                throw new ValidationException(trackerState,
                                              partitionState.toString(schema),
                                              toString(actualRows),
                                              "Returned row state doesn't match the one predicted by the model:" +
                                              "\nExpected: %s (%s)" +
                                              "\nActual:   %s (%s)." +
                                              "\nQuery: %s",
                                              descriptorsToString(expectedRowState.vds), expectedRowState.toString(schema),
                                              descriptorsToString(actualRowState.vds), actualRowState,
                                              query.toSelectStatement());

            if (!ltsEqual(expectedRowState.lts, actualRowState.lts))
                throw new ValidationException(trackerState,
                                              partitionState.toString(schema),
                                              toString(actualRows),
                                              "Timestamps in the row state don't match ones predicted by the model:" +
                                              "\nExpected: %s (%s)" +
                                              "\nActual:   %s (%s)." +
                                              "\nQuery: %s",
                                              Arrays.toString(expectedRowState.lts), expectedRowState.toString(schema),
                                              Arrays.toString(actualRowState.lts), actualRowState,
                                              query.toSelectStatement());

            if (partitionState.staticRow() != null || actualRowState.hasStaticColumns())
            {
                Reconciler.RowState expectedStaticRowState = adjustForSelection(partitionState.staticRow(), schema, selection, true);
                assertStaticRow(partitionState, actualRows, expectedStaticRowState, actualRowState, query, trackerState, schema);
            }
        }

        if (actual.hasNext() || expected.hasNext())
        {
            throw new ValidationException(trackerState,
                                          partitionState.toString(schema),
                                          toString(actualRows),
                                          "Expected results to have the same number of results, but %s result iterator has more results." +
                                          "\nExpected: %s" +
                                          "\nActual:   %s" +
                                          "\nQuery: %s",
                                          actual.hasNext() ? "actual" : "expected",
                                          expectedRows,
                                          actualRows,
                                          query.toSelectStatement());
        }
    }

    public static boolean ltsEqual(long[] expected, long[] actual)
    {
        if (actual == expected)
            return true;
        if (actual == null || expected == null)
            return false;

        int length = actual.length;
        if (expected.length != length)
            return false;

        for (int i = 0; i < actual.length; i++)
        {
            if (actual[i] == NO_TIMESTAMP)
                continue;
            if (actual[i] != expected[i])
                return false;
        }
        return true;
    }

    public static void assertStaticRow(PartitionState partitionState,
                                       List<ResultSetRow> actualRows,
                                       Reconciler.RowState staticRow,
                                       ResultSetRow actualRowState,
                                       Query query,
                                       String trackerState,
                                       SchemaSpec schemaSpec)
    {
        if (!Arrays.equals(staticRow.vds, actualRowState.sds))
            throw new ValidationException(trackerState,
                                          partitionState.toString(schemaSpec),
                                          toString(actualRows),
                                          "Returned static row state doesn't match the one predicted by the model:" +
                                          "\nExpected: %s (%s)" +
                                          "\nActual:   %s (%s)." +
                                          "\nQuery: %s",
                                          descriptorsToString(staticRow.vds), staticRow.toString(schemaSpec),
                                          descriptorsToString(actualRowState.sds), actualRowState,
                                          query.toSelectStatement());

        if (!ltsEqual(staticRow.lts, actualRowState.slts))
            throw new ValidationException(trackerState,
                                          partitionState.toString(schemaSpec),
                                          toString(actualRows),
                                          "Timestamps in the static row state don't match ones predicted by the model:" +
                                          "\nExpected: %s (%s)" +
                                          "\nActual:   %s (%s)." +
                                          "\nQuery: %s",
                                          Arrays.toString(staticRow.lts), staticRow.toString(schemaSpec),
                                          Arrays.toString(actualRowState.slts), actualRowState,
                                          query.toSelectStatement());
    }

    public static String descriptorsToString(long[] descriptors)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < descriptors.length; i++)
        {
            if (descriptors[i] == NIL_DESCR)
                sb.append("NIL");
            if (descriptors[i] == UNSET_DESCR)
                sb.append("UNSET");
            else
                sb.append(descriptors[i]);
            if (i > 0)
                sb.append(", ");
        }
        return sb.toString();
    }

    public static String toString(Collection<Reconciler.RowState> collection, SchemaSpec schema)
    {
        StringBuilder builder = new StringBuilder();

        for (Reconciler.RowState rowState : collection)
            builder.append(rowState.toString(schema)).append("\n");
        return builder.toString();
    }

    public static String toString(List<ResultSetRow> collection)
    {
        StringBuilder builder = new StringBuilder();

        for (ResultSetRow rowState : collection)
            builder.append(rowState.toString()).append("\n");
        return builder.toString();
    }
}
