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
import org.apache.cassandra.db.virtual.model.MetricRow;

/**
 * The {@link org.apache.cassandra.db.virtual.model.MetricRow} row metadata and data walker.
 *
 * @see org.apache.cassandra.db.virtual.model.MetricRow
 */
public class MetricRowWalker implements RowWalker<MetricRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "name", String.class);
        visitor.accept(Column.Type.REGULAR, "scope", String.class);
        visitor.accept(Column.Type.REGULAR, "type", String.class);
        visitor.accept(Column.Type.REGULAR, "value", String.class);
    }

    @Override
    public void visitRow(MetricRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "name", String.class, row::name);
        visitor.accept(Column.Type.REGULAR, "scope", String.class, row::scope);
        visitor.accept(Column.Type.REGULAR, "type", String.class, row::type);
        visitor.accept(Column.Type.REGULAR, "value", String.class, row::value);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case PARTITION_KEY:
                return 1;
            case CLUSTERING:
                return 0;
            case REGULAR:
                return 3;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}
