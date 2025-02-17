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

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;

import static java.lang.String.format;

public class ConstraintsSystemTablesTest extends CqlConstraintValidationTester
{

    @Test
    public void createTableWithConstraints()
    {
        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 0);

        String table = createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 100 and ck1 > 10, ck2 int, v text, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");


        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 1);

        assertRowCount(execute(format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS),
                               KEYSPACE, table, "ck1"), 1);

        execute("ALTER TABLE %s ALTER ck2 CHECK ck2 > 100");

        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 2);

        execute("ALTER TABLE %s ALTER v CHECK LENGTH(v) > 100");

        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 3);

        execute("ALTER TABLE %s ALTER v CHECK NOT_NULL(v)");

        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 3);

        execute("ALTER TABLE %s ALTER ck1 DROP CHECK");

        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 2);

        execute("ALTER TABLE %s ALTER ck2 DROP CHECK");

        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 1);

        execute("ALTER TABLE %s ALTER v DROP CHECK");

        assertRowCount(execute(format("SELECT * FROM %s.%s",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_CONSTRAINTS)), 0);
    }

}
