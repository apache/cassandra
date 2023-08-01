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

package org.apache.cassandra.cql3.functions.masking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.runners.Parameterized;

/**
 * {@link ColumnMaskQueryTester} for user-defined functions.
 */
public class ColumnMaskQueryWithUDFTest extends ColumnMaskQueryTester
{
    private static final String TEXT_REPLACE_FUNCTION = KEYSPACE + ".mask_text_replace";
    private static final String INT_REPLACE_FUNCTION = KEYSPACE + ".mask_int_replace";

    @Before
    @Override
    public void setupSchema() throws Throwable
    {
        createFunction(KEYSPACE,
                       "text, text",
                       "CREATE FUNCTION IF NOT EXISTS " + TEXT_REPLACE_FUNCTION + " (column text, replacement text) " +
                       "CALLED ON NULL INPUT " +
                       "RETURNS text " +
                       "LANGUAGE java " +
                       "AS 'return replacement;'");
        createFunction(KEYSPACE,
                       "int, int",
                       "CREATE FUNCTION IF NOT EXISTS " + INT_REPLACE_FUNCTION + " (column int, replacement int) " +
                       "CALLED ON NULL INPUT " +
                       "RETURNS int " +
                       "LANGUAGE java " +
                       "AS 'return replacement;'");
        super.setupSchema();
    }

    @Parameterized.Parameters(name = "order={0}, mask={1}, type={2}, value={3}")
    public static Collection<Object[]> options()
    {
        List<Object[]> options = new ArrayList<>();
        for (String order : Arrays.asList("ASC", "DESC"))
        {
            options.add(new Object[]{ order, TEXT_REPLACE_FUNCTION + "('redacted')", "text", "abc", "redacted" });
            options.add(new Object[]{ order, TEXT_REPLACE_FUNCTION + "('secret')", "text", "abc", "secret" });
            options.add(new Object[]{ order, INT_REPLACE_FUNCTION + "(0)", "int", 123, 0 });
            options.add(new Object[]{ order, INT_REPLACE_FUNCTION + "(-1)", "int", 123, -1 });
        }
        return options;
    }
}
