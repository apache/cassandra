/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.utils;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrimaryKeyWithSortKeyTest
{
    private static final int NOW_IN_SEC = 0;

    private ColumnMetadata column;
    private PrimaryKeyWithSortKey key;
    private Row row;

    @Before
    public void setUp()
    {
        column = ColumnMetadata.regularColumn("ks", "table", "value", Int32Type.instance);
        IndexContext context = mock(IndexContext.class);
        when(context.getDefinition()).thenReturn(column);
        SSTableId<?> source = mock(SSTableId.class);
        key = new PrimaryKeyWithScore(context, source, mock(PrimaryKey.class), 1.0f, false);
        row = mock(Row.class);
    }

    @Test
    public void testMissingCellIsInvalid()
    {
        when(row.getCell(column)).thenReturn(null);

        assertFalse(key.isIndexDataValid(row, NOW_IN_SEC));
    }

    @Test
    public void testNonLiveCellIsInvalid()
    {
        Cell<?> cell = mock(Cell.class);
        when(cell.isLive(NOW_IN_SEC)).thenReturn(false);
        doReturn(cell).when(row).getCell(column);

        assertFalse(key.isIndexDataValid(row, NOW_IN_SEC));
    }
}
