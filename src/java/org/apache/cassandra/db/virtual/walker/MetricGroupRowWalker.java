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

package org.apache.cassandra.db.virtual.walker;

import org.apache.cassandra.db.virtual.model.Column;
import org.apache.cassandra.db.virtual.model.MetricGroupRow;

/**
 * The {@link org.apache.cassandra.db.virtual.model.MetricGroupRow} row metadata and data walker.
 *
 * @see org.apache.cassandra.db.virtual.model.MetricGroupRow
 */
public class MetricGroupRowWalker implements RowWalker<MetricGroupRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "group_name", String.class);
        visitor.accept(Column.Type.REGULAR, "virtual_table", String.class);
    }

    @Override
    public void visitRow(MetricGroupRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "group_name", String.class, row::groupName);
        visitor.accept(Column.Type.REGULAR, "virtual_table", String.class, row::virtualTable);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case PARTITION_KEY:
            case REGULAR:
                return 1;
            case CLUSTERING:
                return 0;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}
