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

package org.apache.cassandra.repair.autorepair;

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests that verifies "auto_repair" is not included in Schema mutation
 * {@link org.apache.cassandra.schema.SchemaKeyspace} if AutoRepair is disabled
 */
public class AutoRepairTablePropertyTest extends CQLTester
{
    @Test
    public void testSchedulerDisabledNoColumnReturned()
    {
        helperTestTableProperty(false);
    }

    @Test
    public void testSchedulerEnabledShouldReturnColumnReturned()
    {
        helperTestTableProperty(true);
    }

    public void helperTestTableProperty(boolean autoRepairOn)
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(autoRepairOn);
        DatabaseDescriptor.setAccordTransactionsEnabled(false);

        Map<String, String> systemSchemaTables = Map.of(SchemaKeyspaceTables.TABLES, "table_name", SchemaKeyspaceTables.VIEWS, "view_name");
        for (Map.Entry<String, String> systemSchema : systemSchemaTables.entrySet())
        {
            ColumnFamilyStore tables = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME).getColumnFamilyStore(systemSchema.getKey());
            SimpleBuilders.RowBuilder builder = new SimpleBuilders.RowBuilder(tables.metadata(), systemSchema.getValue());
            SchemaKeyspace.addTableParamsToRowBuilder(tables.metadata().params, builder, false);
            Row row = builder.build();
            ColumnMetadata autoRepair = tables.metadata().getColumn(ByteBufferUtil.bytes("auto_repair"));
            ColumnData data = row.getCell(autoRepair);
            if (autoRepairOn)
            {
                assertNotNull(data);
            }
            else
            {
                // if AutoRepair is not enabled, the column should not be returned
                // as part of the system_schema.tables mutation
                assertNull(data);
            }
        }
    }
}
