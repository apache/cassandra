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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import accord.utils.Invariants;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.execution.ResultSetRow;
import org.apache.cassandra.harry.gen.ValueGenerators;

import static org.apache.cassandra.harry.MagicConstants.LTS_UNKNOWN;
import static org.apache.cassandra.harry.MagicConstants.NIL_DESCR;
import static org.apache.cassandra.harry.MagicConstants.NO_TIMESTAMP;
import static org.apache.cassandra.harry.MagicConstants.UNKNOWN_DESCR;
import static org.apache.cassandra.harry.MagicConstants.UNSET_DESCR;

/**
 * Unfortunately model is not reducible to a simple interface of apply/validate, at least not if
 * we intend to support concurrent validation and in-flight/timed out queries. Validation needs to
 * know which queries might have been applied.
 *
 *
 */
public class QuiescentChecker implements Model
{
    private final DataTracker tracker;
    private final Replay replay;
    private final ValueGenerators valueGenerators;

    public QuiescentChecker(ValueGenerators valueGenerators, DataTracker tracker, Replay replay)
    {
        this.valueGenerators = valueGenerators;
        this.tracker = tracker;
        this.replay = replay;
    }

    @Override
    public void validate(Operations.SelectStatement select, List<ResultSetRow> actualRows)
    {
        PartitionState partitionState = new PartitionState(select.pd, valueGenerators);
        PartitionStateBuilder stateBuilder = new PartitionStateBuilder(valueGenerators, partitionState);

        long prevLts = -1;
        for (LtsOperationPair potentialVisit : tracker.potentialVisits(select.pd()))
        {
            if (tracker.isFinished(potentialVisit.lts))
            {
                if (potentialVisit.lts != prevLts)
                {
                    if (prevLts != -1)
                        stateBuilder.endLts(prevLts);
                    stateBuilder.beginLts(potentialVisit.lts);
                    prevLts = potentialVisit.lts;
                }
                Operations.Operation op = replay.replay(potentialVisit.lts, potentialVisit.opId);
                stateBuilder.operation(op);
            }
        }

        // Close last open LTS
        if (prevLts != -1)
            stateBuilder.endLts(prevLts);

        partitionState.filter(select);
        if (select.orderBy() == Operations.ClusteringOrderBy.DESC)
        {
            partitionState.reverse();
        }

        validate(valueGenerators, partitionState, actualRows);
    }

    // TODO: reverse
    public static void validate(ValueGenerators valueGenerators, PartitionState partitionState, List<ResultSetRow> actualRows)
    {
        Iterator<ResultSetRow> actual = actualRows.iterator();
        NavigableMap<Long, PartitionState.RowState> expectedRows = partitionState.rows();

        Iterator<PartitionState.RowState> expected = expectedRows.values().iterator();

        // It is possible that we only get a single row in response, and it is equal to static row
        if (partitionState.isEmpty() && partitionState.staticRow() != null && actual.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            // TODO: it is possible to start distinguising between unknown and null values
            if (actualRowState.cd != UNSET_DESCR && actualRowState.cd != partitionState.staticRow().cd)
            {
                throw new ValidationException(partitionState.toString(),
                                              toString(valueGenerators, actualRows),
                                              "Found a row while model predicts statics only:" +
                                              "\nExpected: %s" +
                                              "\nActual: %s",
                                              partitionState.staticRow(),
                                              actualRowState);
            }

            for (int i = 0; i < actualRowState.vds.length; i++)
            {
                // If clustering is unset, all values should be equal to NIL: we have received a row with statics only.
                if (actualRowState.vds[i] != NIL_DESCR || (actualRowState.lts != LTS_UNKNOWN && actualRowState.lts[i] != NO_TIMESTAMP))
                    throw new ValidationException(partitionState.toString(),
                                                  toString(valueGenerators, actualRows),
                                                  "Found a row while model predicts statics only:" +
                                                  "\nActual: %s",
                                                  actualRowState);
            }

            assertStaticRow(partitionState,
                            actualRows,
                            partitionState.staticRow(),
                            actualRowState,
                            valueGenerators);
        }

        while (actual.hasNext() && expected.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            PartitionState.RowState expectedRowState = expected.next();

            // TODO: this is not necessarily true. It can also be that ordering is incorrect.
            if (actualRowState.cd != UNKNOWN_DESCR && actualRowState.cd != expectedRowState.cd)
            {
                throw new ValidationException(partitionState.toString(),
                                              toString(valueGenerators, actualRows),
                                              "Found a row in the model that is not present in the resultset:" +
                                              "\nExpected: %s" +
                                              "\nActual: %s",
                                              expectedRowState.toString(valueGenerators),
                                              actualRowState);
            }

            if (!vdsEqual(expectedRowState.vds, actualRowState.vds))
                throw new ValidationException(partitionState.toString(),
                                              toString(valueGenerators, actualRows),
                                              "Returned row state doesn't match the one predicted by the model:" +
                                              "\nExpected: %s" +
                                              "\nActual:   %s.",
                                              expectedRowState.toString(valueGenerators),
                                              actualRowState.toString(valueGenerators));

            if (!ltsEqual(expectedRowState.lts, actualRowState.lts))
                throw new ValidationException(partitionState.toString(),
                                              toString(valueGenerators, actualRows),
                                              "Timestamps in the row state don't match ones predicted by the model:" +
                                              "\nExpected: %s" +
                                              "\nActual:   %s.",
                                              expectedRowState.toString(valueGenerators),
                                              actualRowState.toString(valueGenerators));

            if (partitionState.staticRow() != null || actualRowState.hasStaticColumns())
            {
                PartitionState.RowState expectedStaticRowState = partitionState.staticRow();
                assertStaticRow(partitionState, actualRows, expectedStaticRowState, actualRowState, valueGenerators);
            }
        }

        if (actual.hasNext() || expected.hasNext())
        {
            throw new ValidationException(partitionState.toString(),
                                          toString(valueGenerators, actualRows),
                                          "Expected results to have the same number of results, but %s result iterator has more results." +
                                          "\nExpected: %s" +
                                          "\nActual:   %s",
                                          actual.hasNext() ? "actual" : "expected",
                                          expectedRows,
                                          actualRows);
        }
    }

    public static boolean vdsEqual(long[] expected, long[] actual)
    {
        Invariants.checkState(expected.length == actual.length);
        for (int i = 0; i < actual.length; i++)
        {
            long expectedD = expected[i];
            long actualD = actual[i];
            // An unset column will show as NIL
            if (expectedD == UNSET_DESCR && actualD == NIL_DESCR)
                continue;
            if (actualD != expectedD)
                return false;
        }
        return true;
    }

    public static boolean ltsEqual(long[] expected, long[] actual)
    {
        if (actual == LTS_UNKNOWN)
            return true;

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
                                       PartitionState.RowState staticRow,
                                       ResultSetRow actualRowState,
                                       ValueGenerators valueGenerators)
    {
        if (!vdsEqual(staticRow.vds, actualRowState.sds))
            throw new ValidationException(partitionState.toString(),
                                          toString(valueGenerators, actualRows),
                                          "Returned static row state doesn't match the one predicted by the model:" +
                                          "\nExpected: %s (%s)" +
                                          "\nActual:   %s (%s).",
                                          descriptorsToString(staticRow.vds), staticRow.toString(valueGenerators),
                                          descriptorsToString(actualRowState.sds), actualRowState);

        if (!ltsEqual(staticRow.lts, actualRowState.slts))
            throw new ValidationException(partitionState.toString(),
                                          toString(valueGenerators, actualRows),
                                          "Timestamps in the static row state don't match ones predicted by the model:" +
                                          "\nExpected: %s (%s)" +
                                          "\nActual:   %s (%s).",
                                          Arrays.toString(staticRow.lts), staticRow.toString(valueGenerators),
                                          Arrays.toString(actualRowState.slts), actualRowState);
    }

    public static String descriptorsToString(long[] descriptors)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < descriptors.length; i++)
        {
            if (i > 0)
                sb.append(", ");
            if (descriptors[i] == NIL_DESCR)
                sb.append("NIL");
            else if (descriptors[i] == UNSET_DESCR)
                sb.append("UNSET");
            else
                sb.append(descriptors[i]);
        }
        return sb.toString();
    }

    public static String toString(Collection<PartitionState.RowState> collection, ValueGenerators valueGenerators)
    {
        StringBuilder builder = new StringBuilder();

        for (PartitionState.RowState rowState : collection)
            builder.append(rowState.toString(valueGenerators)).append("\n");
        return builder.toString();
    }

    public static String toString(ValueGenerators valueGenerators, List<ResultSetRow> collection)
    {
        StringBuilder builder = new StringBuilder();

        for (ResultSetRow rowState : collection)
            builder.append(rowState.toString(valueGenerators)).append("\n");
        return builder.toString();
    }


    public static class ValidationException extends RuntimeException
    {
        public ValidationException(String partitionState, String observedState, String format, Object... objects)
        {
            super(String.format(format, objects) +
                  "\nPartition state:\n" + partitionState +
                  "\nObserved state:\n" + observedState);
        }
    }
}