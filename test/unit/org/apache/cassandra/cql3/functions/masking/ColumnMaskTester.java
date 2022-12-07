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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.UserFunctions;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests table columns with attached dynamic data masking functions.
 */
public class ColumnMaskTester extends CQLTester
{
    protected static final String USERNAME = "ddm_user";
    protected static final String PASSWORD = "ddm_password";

    @BeforeClass
    public static void beforeClass()
    {
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
    }

    @Before
    public void before() throws Throwable
    {
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        useSuperUser();
        executeNet(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", USERNAME, PASSWORD));
        executeNet(format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE, USERNAME));
        executeNet(format("REVOKE UNMASK ON KEYSPACE %s FROM %s", KEYSPACE, USERNAME));
        useUser(USERNAME, PASSWORD);
    }

    protected void assertTableColumnsAreNotMasked(String... columns) throws Throwable
    {
        for (String column : columns)
        {
            assertColumnIsNotMasked(currentTable(), column);
        }
    }

    protected void assertViewColumnsAreNotMasked(String... columns) throws Throwable
    {
        for (String column : columns)
        {
            assertColumnIsNotMasked(currentView(), column);
        }
    }

    protected void assertColumnIsNotMasked(String table, String column) throws Throwable
    {
        ColumnMask mask = getColumnMask(table, column);
        assertNull(format("Mask for column '%s'", column), mask);

        assertRows(execute(format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMN_MASKS),
                           KEYSPACE, table, column));
    }

    protected void assertColumnIsMasked(String table,
                                        String column,
                                        String functionName,
                                        List<AbstractType<?>> partialArgumentTypes,
                                        List<ByteBuffer> partialArgumentValues) throws Throwable
    {
        KeyspaceMetadata keyspaceMetadata = Keyspace.open(KEYSPACE).getMetadata();
        TableMetadata tableMetadata = keyspaceMetadata.getTableOrViewNullable(table);
        assertNotNull(tableMetadata);
        ColumnMetadata columnMetadata = tableMetadata.getColumn(ColumnIdentifier.getInterned(column, false));
        assertNotNull(columnMetadata);
        AbstractType<?> columnType = columnMetadata.type;

        // Verify the column mask in the in-memory schema
        ColumnMask mask = getColumnMask(table, column);
        assertNotNull(mask);
        assertThat(mask.partialArgumentTypes()).isEqualTo(columnType.isReversed() && functionName.equals("mask_replace")
                                                          ? Collections.singletonList(ReversedType.getInstance(partialArgumentTypes.get(0)))
                                                          : partialArgumentTypes);
        assertThat(mask.partialArgumentValues()).isEqualTo(partialArgumentValues);

        // Verify the function in the column mask
        ScalarFunction function = mask.function;
        assertNotNull(function);
        assertThat(function.name().name).isEqualTo(functionName);
        assertThat(function.argTypes().get(0).asCQL3Type()).isEqualTo(columnMetadata.type.asCQL3Type());
        assertThat(function.argTypes().size()).isEqualTo(partialArgumentTypes.size() + 1);

        // Retrieve the persisted column metadata
        UntypedResultSet columnRows = execute("SELECT * FROM system_schema.columns " +
                                              "WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                                              KEYSPACE, table, column);
        ColumnMetadata persistedColumn = SchemaKeyspace.createColumnFromRow(columnRows.one(), keyspaceMetadata.types, UserFunctions.none());

        // Verify the column mask in the persisted schema
        ColumnMask savedMask = persistedColumn.getMask();
        assertNotNull(savedMask);
        assertThat(mask).isEqualTo(savedMask);
        assertThat(mask.function.argTypes()).isEqualTo(savedMask.function.argTypes());
    }

    @Nullable
    protected ColumnMask getColumnMask(String table, String column)
    {
        TableMetadata tableMetadata = Schema.instance.getTableMetadata(KEYSPACE, table);
        assertNotNull(tableMetadata);
        ColumnMetadata columnMetadata = tableMetadata.getColumn(ColumnIdentifier.getInterned(column, false));

        if (columnMetadata == null)
            fail(format("Unknown column '%s'", column));

        return columnMetadata.getMask();
    }
}
