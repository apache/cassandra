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
package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public final class DroppedColumn
{
    public final ColumnMetadata column;
    public final long droppedTime; // drop timestamp, in microseconds, yet with millisecond granularity

    /**
     * Creates a new dropped column record.
     *
     * @param column the metadata for the dropped column. This <b>must</b> be a dropped metadata, that is we should
     * have {@code column.isDropped() == true}.
     * @param droppedTime the time at which the column was dropped, in microseconds.
     */
    public DroppedColumn(ColumnMetadata column, long droppedTime)
    {
        this.column = column;
        this.droppedTime = droppedTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof DroppedColumn))
            return false;

        DroppedColumn dc = (DroppedColumn) o;

        return column.equals(dc.column) && droppedTime == dc.droppedTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(column, droppedTime);
    }

    public String toCQLString()
    {
        return String.format("DROPPED COLUMN RECORD %s %s%s USING TIMESTAMP %d",
                             column.name,
                             column.type.asCQL3Type(),
                             column.isStatic() ? " static" : "",
                             droppedTime);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("column", column).add("droppedTime", droppedTime).toString();
    }

    /**
     * A parsed dropped column record (from CREATE TABLE ... WITH DROPPED COLUMN RECORD ...).
     */
    public static final class Raw
    {
        private final ColumnIdentifier name;
        private final CQL3Type.Raw type;
        private final boolean isStatic;
        private final long timestamp;

        public Raw(ColumnIdentifier name, CQL3Type.Raw type, boolean isStatic, long timestamp)
        {
            this.name = name;
            this.type = type;
            this.isStatic = isStatic;
            this.timestamp = timestamp;
        }

        public DroppedColumn prepare(String keyspace, String table)
        {
            ColumnMetadata.Kind kind = isStatic ? ColumnMetadata.Kind.STATIC : ColumnMetadata.Kind.REGULAR;
            AbstractType<?> parsedType = type.prepare(keyspace).getType();
            if (parsedType.referencesUserTypes())
                throw invalidRequest("Invalid type %s for DROPPED COLUMN RECORD on %s: dropped column types should "
                                     + "not have user types", type, name);

            ColumnMetadata droppedColumn = ColumnMetadata.droppedColumn(keyspace,
                                                                        table,
                                                                        name,
                                                                        parsedType,
                                                                        kind);
            return new DroppedColumn(droppedColumn, timestamp);
        }
    }
}
