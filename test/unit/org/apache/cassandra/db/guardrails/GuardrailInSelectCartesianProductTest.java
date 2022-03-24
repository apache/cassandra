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

package org.apache.cassandra.db.guardrails;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;

/**
 * Tests the guardrail for the max number of restrictions produced by the cartesian product of the {@code IN}
 * restrictions of a query, {@link Guardrails#inSelectCartesianProduct}.
 */
public class GuardrailInSelectCartesianProductTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 16;
    private static final int FAIL_THRESHOLD = 25;

    private static final String WARN_MESSAGE = "The cartesian product of the IN restrictions on %s produces %d " +
                                               "values, this exceeds warning threshold of " + WARN_THRESHOLD;
    private static final String FAIL_MESSAGE = "Aborting query because the cartesian product of the IN restrictions " +
                                               "on %s produces %d values, this exceeds fail threshold of " + FAIL_THRESHOLD;

    public GuardrailInSelectCartesianProductTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.inSelectCartesianProduct,
              Guardrails::setInSelectCartesianProductThreshold,
              Guardrails::getInSelectCartesianProductWarnThreshold,
              Guardrails::getInSelectCartesianProductFailThreshold);
    }

    @Override
    protected long currentValue()
    {
        throw new UnsupportedOperationException();
    }

    @Before
    public void initSchema()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, PRIMARY KEY((pk1, pk2), ck1, ck2))");
    }

    @Test
    public void testPkCartesianProduct() throws Throwable
    {
        // below both thresholds
        testPkCartesianProduct(1, 1);
        testPkCartesianProduct(1, 4);
        testPkCartesianProduct(4, 4);

        // above warn threshold
        testPkCartesianProduct(5, 5);
        testPkCartesianProduct(2, 12);
        testPkCartesianProduct(8, 3);

        // above cartesian product limit
        testPkCartesianProduct(1, 26);
        testPkCartesianProduct(5, 6);
        testPkCartesianProduct(26, 1);
    }

    @Test
    public void testCkCartesianProduct() throws Throwable
    {
        // below both thresholds
        testCkCartesianProduct(3, 8);
        testCkCartesianProduct(5, 5);

        // above cartesian product limit
        testCkCartesianProduct(1, 26);
        testCkCartesianProduct(5, 6);
        testCkCartesianProduct(6, 5);
        testCkCartesianProduct(26, 1);
    }

    @Test
    public void testPkCkCartesianProduct() throws Throwable
    {
        // below both thresholds
        testCartesianProduct(1, 10, 1, 10);
        testCartesianProduct(10, 1, 10, 1);
        testCartesianProduct(5, 5, 5, 5);

        // above cartesian product limit
        testCartesianProduct(5, 6, 5, 5);
        testCartesianProduct(6, 5, 5, 5);
        testCartesianProduct(5, 5, 6, 5);
        testCartesianProduct(5, 5, 5, 6);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        testExcludedUsers(() -> String.format("SELECT * FROM %%s WHERE pk1 in (%s) AND pk2 in (%s)",
                                              terms(5), terms(5)),
                          () -> String.format("SELECT * FROM %%s WHERE pk1 in (%s) AND pk2 in (%s) AND ck1 in (%s) AND ck2 in (%s)",
                                              terms(5), terms(5), terms(5), terms(6)));
    }

    @Test
    public void testPkCartesianProductMultiColumnBelowThreshold() throws Throwable
    {
        String inTerms = IntStream.range(0, 5).mapToObj(i -> String.format("(%d, %d)", i, i + 1)).collect(Collectors.joining(", "));
        String query = String.format("SELECT * FROM %%s WHERE (pk1, pk2) in (%s)", inTerms);
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: pk1", query);
    }

    private void testPkCartesianProduct(int pk1Terms, int pk2Terms) throws Throwable
    {
        testCartesianProduct(pk1Terms, pk2Terms, 1, 1);
    }

    private void testCkCartesianProduct(int ck1Terms, int ck2Terms) throws Throwable
    {
        testCartesianProduct(1, 1, ck1Terms, ck2Terms);
    }

    private void testCartesianProduct(int pk1, int pk2, int ck1, int ck2) throws Throwable
    {
        int keys = pk1 * pk2;
        int clusterings = ck1 * ck2;

        String query = String.format("SELECT * FROM %%s WHERE pk1 in (%s) AND pk2 in (%s) AND ck1 in (%s) AND ck2 in (%s)",
                                     terms(pk1), terms(pk2), terms(ck1), terms(ck2));
        testCartesianProduct(() -> execute(userClientState, query), keys, clusterings);

        String queryWithBindVariables = String.format("SELECT * FROM %%s WHERE pk1 in (%s) AND pk2 in (%s) AND ck1 in (%s) AND ck2 in (%s)",
                                                      markers(pk1), markers(pk2), markers(ck1), markers(ck2));
        testCartesianProduct(() -> execute(userClientState, queryWithBindVariables, bindValues(pk1, pk2, ck1, ck2)), keys, clusterings);
    }

    private void testCartesianProduct(CheckedFunction function, int keys, int clusterings) throws Throwable
    {
        String keysFailMessage = String.format(FAIL_MESSAGE, "partition key", keys);
        String keysWarnMessage = String.format(WARN_MESSAGE, "partition key", keys);
        String clusteringsFailMessage = String.format(FAIL_MESSAGE, "clustering key", clusterings);
        String clusteringsWarnMessage = String.format(WARN_MESSAGE, "clustering key", clusterings);

        if (keys > FAIL_THRESHOLD)
        {
            assertFails(function, keysFailMessage);
        }
        else if (keys > WARN_THRESHOLD)
        {
            if (clusterings > FAIL_THRESHOLD)
                assertFails(function, Arrays.asList(keysWarnMessage, clusteringsFailMessage));
            else if (clusterings > WARN_THRESHOLD)
                assertWarns(function, Arrays.asList(keysWarnMessage, clusteringsWarnMessage));
            else
                assertWarns(function, keysWarnMessage);
        }
        else if (clusterings > FAIL_THRESHOLD)
        {
            assertFails(function, clusteringsFailMessage);
        }
        else if (clusterings > WARN_THRESHOLD)
        {
            assertWarns(function, clusteringsWarnMessage);
        }
        else
        {
            assertValid(function);
        }
    }

    private static String terms(int terms)
    {
        assert terms > 0;
        return IntStream.range(0, terms).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    }

    private static String markers(int terms)
    {
        assert terms > 0;
        return IntStream.range(0, terms).mapToObj(i -> "?").collect(Collectors.joining(", "));
    }

    private static List<ByteBuffer> bindValues(int... termCounts)
    {
        return IntStream.of(termCounts)
                        .boxed()
                        .flatMap(terms -> IntStream.range(0, terms).boxed().map(Int32Type.instance::decompose))
                        .collect(Collectors.toList());
    }
}
