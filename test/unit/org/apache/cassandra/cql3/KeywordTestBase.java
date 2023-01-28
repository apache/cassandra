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

package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.SyntaxException;

/**
 * This class tests all keywords which took a long time. Hence it was split into multiple
 * KeywordTestSplitN to prevent CI timing out. If timeouts reappear split it further
 */
public abstract class KeywordTestBase extends CQLTester
{
    public static List<Object[]> keywords = Arrays.stream(CqlParser.tokenNames)
                                                  .filter(k -> k.startsWith("K_"))
                                                  .map(k -> {
                                                      String keyword = k.substring(2);
                                                      return new Object[] { keyword,ReservedKeywords.isReserved(keyword) };
                                                  })
                                                  .collect(Collectors.toList());

    public static Collection<Object[]> getKeywordsForSplit(int split, int totalSplits)
    {
        return Sets.newHashSet(Lists.partition(KeywordTestBase.keywords, KeywordTestBase.keywords.size() / totalSplits)
                                    .get(split - 1));
    }

    String keyword;
    boolean isReserved;
    public KeywordTestBase(String keyword, boolean isReserved)
    {
        this.keyword = keyword;
        this.isReserved = isReserved;
    }

    @Test
    public void test() throws Throwable
    {
        String createStatement = String.format("CREATE TABLE %s.%s (c text, %s text, PRIMARY KEY (c, %s))",
                                               KEYSPACE, keyword, keyword, keyword, keyword);
        logger.info(createStatement);
        if (isReserved)
        {
            try
            {
                schemaChange(createStatement);
                Assert.fail(String.format("Reserved keyword %s should not have parsed", keyword));
            }
            catch (RuntimeException ignore) // SyntaxException is wrapped by CQLTester
            {
                Assert.assertEquals(SyntaxException.class, ignore.getCause().getClass());
            }
        }
        else // not a reserved word
        {
            /* Create the table using the keyword as a tablename and column name, then call describe and re-create it
             * using the create_statement result.
             */
            schemaChange(createStatement);
            String describeStatement = String.format("DESCRIBE TABLE %s.%s", KEYSPACE, keyword);
            UntypedResultSet rs = execute(describeStatement);
            UntypedResultSet.Row row = rs.one();
            String describedCreateStatement = row.getString("create_statement");

            String recreateStatement = describedCreateStatement.replace(KEYSPACE, KEYSPACE_PER_TEST);
            logger.info(recreateStatement);
            schemaChange(recreateStatement);

            /* Check it is possible to insert and select from the table/column, with the keyword used in the
             * where clause
             */
            String insertStatement = String.format("INSERT INTO %s.%s(c,%s) VALUES ('x', '%s')",
                                                   KEYSPACE, keyword, keyword, keyword);
            logger.info(insertStatement);
            execute(insertStatement);

            String selectStatement = String.format("SELECT c, %s FROM %s.%s WHERE c = 'x' AND %s >= ''",
                                                   keyword, KEYSPACE, keyword, keyword);
            logger.info(selectStatement);
            rs = execute(selectStatement);
            row = rs.one();
            String value = row.getString(keyword.toLowerCase());
            Assert.assertEquals(keyword, value);

            /* Make a materialized view using the fields (cannot re-use the name as MV must be in same keyspace).
             * Added as CASSANDRA-11803 motivated adding the reserved/unreserved distinction
            */
            String createMV = String.format("CREATE MATERIALIZED VIEW %s.mv_%s AS %s PRIMARY KEY (c, %s)",
                                            KEYSPACE, keyword, selectStatement, keyword);
            logger.info(createMV);
            schemaChange(createMV);
        }
    }
}
