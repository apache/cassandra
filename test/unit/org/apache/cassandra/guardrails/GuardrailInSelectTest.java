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

package org.apache.cassandra.guardrails;


import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.ProtocolVersion;
import org.assertj.core.api.Assertions;

public class GuardrailInSelectTest extends GuardrailTester
{
    private static int defaultInSelectCartesianProduct;
    private static int defaultPartitionKeysInSelectQuery;
    private static final int inSelectCartesianProduct = 25;
    private static final int partitionKeysInSelectQuery = 500;
    private static final String cartesianProductErrorMessage = "The query cannot be completed because cartesian product of all values in IN conditions is greater than " + inSelectCartesianProduct;

    @BeforeClass
    public static void setup()
    {
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();
        defaultInSelectCartesianProduct = config.in_select_cartesian_product_failure_threshold;
        defaultPartitionKeysInSelectQuery = config.partition_keys_in_select_failure_threshold;
        config.in_select_cartesian_product_failure_threshold = inSelectCartesianProduct;
        config.partition_keys_in_select_failure_threshold = partitionKeysInSelectQuery;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().in_select_cartesian_product_failure_threshold = defaultInSelectCartesianProduct;
        DatabaseDescriptor.getGuardrailsConfig().partition_keys_in_select_failure_threshold = defaultPartitionKeysInSelectQuery;
    }

    @Before
    public void initSchema()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, PRIMARY KEY((pk1, pk2), ck1, ck2))");
    }

    @Test
    public void testPkCartesianProduct() throws Throwable
    {
        // below threshold
        testPkCartesianProduct(5, 5);
        testPkCartesianProduct(2, 12);
        testPkCartesianProduct(8, 3);

        // above cartesian product limit
        testPkCartesianProduct(1, 26);
        testPkCartesianProduct(5, 6);
        testPkCartesianProduct(26, 1);

        // above cartesian product limit as super user
        useSuperUser();
        testPkCartesianProduct(26, 1);
    }

    @Test
    public void testCkCartesianProduct() throws Throwable
    {
        // below threshold
        testCkCartesianProduct(3, 8);
        testCkCartesianProduct(5, 5);

        // above cartesian product limit
        testCkCartesianProduct(1, 26);
        testCkCartesianProduct(5, 6);
        testCkCartesianProduct(6, 5);
        testCkCartesianProduct(26, 1);

        // above cartesian product limit as super user
        useSuperUser();
        testCkCartesianProduct(26, 1);
    }

    @Test
    public void testPkCkCartesianProduct() throws Throwable
    {
        // below threshold
        testCartesianProduct(1, 10, 1, 10);
        testCartesianProduct(10, 1, 10, 1);
        testCartesianProduct(5, 5, 5, 5);

        // above cartesian product limit
        testCartesianProduct(5, 6, 5, 5);
        testCartesianProduct(6, 5, 5, 5);
        testCartesianProduct(5, 5, 6, 5);
        testCartesianProduct(5, 5, 5, 6);

        // above cartesian product limit as super user
        useSuperUser();
        testCartesianProduct(5, 5, 5, 6);
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
        String query = String.format("SELECT * FROM %%s WHERE pk1 in (%s) AND pk2 in (%s) AND ck1 in (%s) AND ck2 in (%s);",
                                     terms(pk1), terms(pk2), terms(ck1), terms(ck2));

        String queryWithBindVariables = String.format("SELECT * FROM %%s WHERE pk1 in (%s) AND pk2 in (%s) AND ck1 in (%s) AND ck2 in (%s);",
                                                      markers(pk1), markers(pk2), markers(ck1), markers(ck2));

        boolean exceedCartesianProductLimit = Math.max(pk1 * pk2, ck1 * ck2) > inSelectCartesianProduct;
        boolean failed = exceedCartesianProductLimit && !isSuperUser();

        if (failed)
        {
            String errorMessage = cartesianProductErrorMessage;
            Assertions.assertThatThrownBy(() -> executeNet(query))
                      .hasMessage(errorMessage);
            Assertions.assertThatThrownBy(() -> executeNet(queryWithBindVariables, bindValues(pk1, pk2, ck1, ck2)))
                      .hasMessage(errorMessage);
        }
        else
        {
            executeNet(query);
            executeNet(queryWithBindVariables, bindValues(pk1, pk2, ck1, ck2));
        }
    }

    @Test
    public void testPkCartesianProductMultiColumnBelowThreshold() throws Throwable
    {
        String inTerms = IntStream.range(0, 5).mapToObj(i -> String.format("(%d, %d)", i, i + 1)).collect(Collectors.joining(", "));
        String query = String.format("SELECT * FROM %%s WHERE (pk1, pk2) in (%s)", inTerms);
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: pk1", query);
    }

    private static String terms(int terms)
    {
        assert terms > 0;
        return IntStream.range(0, terms).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    }

    private static Object[] bindValues(int... termCounts)
    {
        Object[] values = new Object[Arrays.stream(termCounts).sum()];
        int idx = 0;

        for (int count : termCounts)
        {
            for (int i = 0; i < count; i++, idx++)
            {
                values[idx] = i;
            }
        }

        return values;
    }

    private static String markers(int terms)
    {
        assert terms > 0;
        return IntStream.range(0, terms).mapToObj(i -> "?").collect(Collectors.joining(", "));
    }
}