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
import org.apache.cassandra.db.virtual.model.HistogramMetricRow;

/**
 * The {@link org.apache.cassandra.db.virtual.model.HistogramMetricRow} row metadata and data walker.
 *
 * @see org.apache.cassandra.db.virtual.model.HistogramMetricRow
 */
public class HistogramMetricRowWalker implements RowWalker<HistogramMetricRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "name", String.class);
        visitor.accept(Column.Type.REGULAR, "max", Long.TYPE);
        visitor.accept(Column.Type.REGULAR, "mean", Double.TYPE);
        visitor.accept(Column.Type.REGULAR, "min", Long.TYPE);
        visitor.accept(Column.Type.REGULAR, "p75th", Double.TYPE);
        visitor.accept(Column.Type.REGULAR, "p95th", Double.TYPE);
        visitor.accept(Column.Type.REGULAR, "p98th", Double.TYPE);
        visitor.accept(Column.Type.REGULAR, "p999th", Double.TYPE);
        visitor.accept(Column.Type.REGULAR, "p99th", Double.TYPE);
        visitor.accept(Column.Type.REGULAR, "scope", String.class);
    }

    @Override
    public void visitRow(HistogramMetricRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "name", String.class, row::name);
        visitor.accept(Column.Type.REGULAR, "max", Long.TYPE, row::max);
        visitor.accept(Column.Type.REGULAR, "mean", Double.TYPE, row::mean);
        visitor.accept(Column.Type.REGULAR, "min", Long.TYPE, row::min);
        visitor.accept(Column.Type.REGULAR, "p75th", Double.TYPE, row::p75th);
        visitor.accept(Column.Type.REGULAR, "p95th", Double.TYPE, row::p95th);
        visitor.accept(Column.Type.REGULAR, "p98th", Double.TYPE, row::p98th);
        visitor.accept(Column.Type.REGULAR, "p999th", Double.TYPE, row::p999th);
        visitor.accept(Column.Type.REGULAR, "p99th", Double.TYPE, row::p99th);
        visitor.accept(Column.Type.REGULAR, "scope", String.class, row::scope);
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
                return 9;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}
