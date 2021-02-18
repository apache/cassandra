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
package org.apache.cassandra.index.sai.cql;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IndexOperatorSupportTest extends SAITester
{
    @Test
    public void shouldRejectAllQueries() throws Throwable
    {
        requireNetwork();

        DataModel model = new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA);
        model.createTables(this);
        model.createIndexes(this);

        for (String[] scenario : scenarios())
        {
            assertThatThrownBy(() -> model.executeIndexed(this, String.format(scenario[2], scenario[1]))).isInstanceOf(InvalidQueryException.class).as(scenario[0]);
        }
    }

    private List<String[]> scenarios()
    {
        List<String[]> scenarios = new LinkedList<>();

        for (Pair<String, String> column : DataModel.NORMAL_COLUMNS)
        {
            scenarios.add(new String[]{ "Should reject LIKE query for " + column.left, column.left, "SELECT * FROM %%s WHERE %s LIKE 'foo%%%%'" });
        }

        scenarios.add(new String[] { "Should reject range query for " + DataModel.ASCII_COLUMN, DataModel.ASCII_COLUMN, "SELECT * FROM %%s WHERE %s > 'foo'" });
        scenarios.add(new String[] { "Should reject range query for " + DataModel.ASCII_COLUMN, DataModel.ASCII_COLUMN, "SELECT * FROM %%s WHERE %s != 'foo'" });

        scenarios.add(new String[] { "Should reject range query for " + DataModel.TEXT_COLUMN, DataModel.TEXT_COLUMN, "SELECT * FROM %%s WHERE %s > 'foo'" });
        scenarios.add(new String[] { "Should reject range query for " + DataModel.TEXT_COLUMN, DataModel.TEXT_COLUMN, "SELECT * FROM %%s WHERE %s != 'foo'" });

        scenarios.add(new String[] { "Should reject NEQ query for " + DataModel.TINYINT_COLUMN, DataModel.TINYINT_COLUMN, "SELECT * FROM %%s WHERE %s != 10" });
        scenarios.add(new String[] { "Should reject NEQ query for " + DataModel.SMALLINT_COLUMN, DataModel.SMALLINT_COLUMN, "SELECT * FROM %%s WHERE %s != 10" });
        scenarios.add(new String[] { "Should reject NEQ query for " + DataModel.INT_COLUMN, DataModel.INT_COLUMN, "SELECT * FROM %%s WHERE %s != 10" });
        scenarios.add(new String[] { "Should reject NEQ query for " + DataModel.BIGINT_COLUMN, DataModel.BIGINT_COLUMN, "SELECT * FROM %%s WHERE %s != 10" });

        scenarios.add(new String[] { "Should reject range query for " + DataModel.BOOLEAN_COLUMN, DataModel.BOOLEAN_COLUMN, "SELECT * FROM %%s WHERE %s > true" });

        scenarios.add(new String[] { "Should reject range query for " + DataModel.UUID_COLUMN, DataModel.UUID_COLUMN, "SELECT * FROM %%s WHERE %s > e37394dc-d17b-11e8-a8d5-f2801f1b9fd1" });

        return scenarios;
    }
}
