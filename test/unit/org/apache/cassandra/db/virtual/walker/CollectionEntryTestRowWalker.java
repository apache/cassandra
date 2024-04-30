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

import org.apache.cassandra.db.virtual.model.CollectionEntryTestRow;
import org.apache.cassandra.db.virtual.model.Column;

/**
 * aThe {@link org.apache.cassandra.db.virtual.model.CollectionEntryTestRow} row metadata and data walker.
 *
 * @see org.apache.cassandra.db.virtual.model.CollectionEntryTestRow
 */
public class CollectionEntryTestRowWalker implements RowWalker<CollectionEntryTestRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "primary_key", String.class);
        visitor.accept(Column.Type.PARTITION_KEY, "secondary_key", String.class);
        visitor.accept(Column.Type.CLUSTERING, "ordered_key", Long.TYPE);
        visitor.accept(Column.Type.REGULAR, "boolean_value", Boolean.TYPE);
        visitor.accept(Column.Type.REGULAR, "byte_value", Byte.TYPE);
        visitor.accept(Column.Type.REGULAR, "double_value", Double.TYPE);
        visitor.accept(Column.Type.REGULAR, "int_value", Integer.TYPE);
        visitor.accept(Column.Type.REGULAR, "long_value", Long.TYPE);
        visitor.accept(Column.Type.REGULAR, "short_value", Short.TYPE);
        visitor.accept(Column.Type.REGULAR, "value", String.class);
    }

    @Override
    public void visitRow(CollectionEntryTestRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "primary_key", String.class, row::primaryKey);
        visitor.accept(Column.Type.PARTITION_KEY, "secondary_key", String.class, row::secondaryKey);
        visitor.accept(Column.Type.CLUSTERING, "ordered_key", Long.TYPE, row::orderedKey);
        visitor.accept(Column.Type.REGULAR, "boolean_value", Boolean.TYPE, row::booleanValue);
        visitor.accept(Column.Type.REGULAR, "byte_value", Byte.TYPE, row::byteValue);
        visitor.accept(Column.Type.REGULAR, "double_value", Double.TYPE, row::doubleValue);
        visitor.accept(Column.Type.REGULAR, "int_value", Integer.TYPE, row::intValue);
        visitor.accept(Column.Type.REGULAR, "long_value", Long.TYPE, row::longValue);
        visitor.accept(Column.Type.REGULAR, "short_value", Short.TYPE, row::shortValue);
        visitor.accept(Column.Type.REGULAR, "value", String.class, row::value);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case CLUSTERING:
                return 1;
            case PARTITION_KEY:
                return 2;
            case REGULAR:
                return 7;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}
