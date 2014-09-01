/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file * to you under the Apache License, Version 2.0 (the
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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Base abstract class for {@code Row} implementations.
 *
 * Unless you have a very good reason not to, every row implementation
 * should probably extend this class.
 */
public abstract class AbstractRow implements Row
{
    public Unfiltered.Kind kind()
    {
        return Unfiltered.Kind.ROW;
    }

    public boolean hasLiveData(int nowInSec)
    {
        if (primaryKeyLivenessInfo().isLive(nowInSec))
            return true;

        for (Cell cell : this)
            if (cell.isLive(nowInSec))
                return true;

        return false;
    }

    public boolean isEmpty()
    {
        return !primaryKeyLivenessInfo().hasTimestamp()
            && deletion().isLive()
            && !iterator().hasNext()
            && !hasComplexDeletion();
    }

    public boolean isStatic()
    {
        return clustering() == Clustering.STATIC_CLUSTERING;
    }

    public void digest(MessageDigest digest)
    {
        FBUtilities.updateWithByte(digest, kind().ordinal());
        clustering().digest(digest);

        deletion().digest(digest);
        primaryKeyLivenessInfo().digest(digest);

        Iterator<ColumnDefinition> iter = columns().complexColumns();
        while (iter.hasNext())
            getDeletion(iter.next()).digest(digest);

        for (Cell cell : this)
            cell.digest(digest);
    }

    /**
     * Copy this row to the provided writer.
     *
     * @param writer the row writer to write this row to.
     */
    public void copyTo(Row.Writer writer)
    {
        Rows.writeClustering(clustering(), writer);
        writer.writePartitionKeyLivenessInfo(primaryKeyLivenessInfo());
        writer.writeRowDeletion(deletion());

        for (Cell cell : this)
            cell.writeTo(writer);

        for (int i = 0; i < columns().complexColumnCount(); i++)
        {
            ColumnDefinition c = columns().getComplex(i);
            DeletionTime dt = getDeletion(c);
            if (!dt.isLive())
                writer.writeComplexDeletion(c, dt);
        }
        writer.endOfRow();
    }

    public void validateData(CFMetaData metadata)
    {
        Clustering clustering = clustering();
        for (int i = 0; i < clustering.size(); i++)
        {
            ByteBuffer value = clustering.get(i);
            if (value != null)
                metadata.comparator.subtype(i).validate(value);
        }

        primaryKeyLivenessInfo().validate();
        if (deletion().localDeletionTime() < 0)
            throw new MarshalException("A local deletion time should not be negative");

        for (Cell cell : this)
            cell.validate();
    }

    public String toString(CFMetaData metadata)
    {
        return toString(metadata, false);
    }

    public String toString(CFMetaData metadata, boolean fullDetails)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Row");
        if (fullDetails)
        {
            sb.append("[info=").append(primaryKeyLivenessInfo());
            if (!deletion().isLive())
                sb.append(" del=").append(deletion());
            sb.append(" ]");
        }
        sb.append(": ").append(clustering().toString(metadata)).append(" | ");
        boolean isFirst = true;
        ColumnDefinition prevColumn = null;
        for (Cell cell : this)
        {
            if (isFirst) isFirst = false; else sb.append(", ");
            if (fullDetails)
            {
                if (cell.column().isComplex() && !cell.column().equals(prevColumn))
                {
                    DeletionTime complexDel = getDeletion(cell.column());
                    if (!complexDel.isLive())
                        sb.append("del(").append(cell.column().name).append(")=").append(complexDel).append(", ");
                }
                sb.append(cell);
                prevColumn = cell.column();
            }
            else
            {
                sb.append(cell.column().name);
                if (cell.column().type instanceof CollectionType)
                {
                    CollectionType ct = (CollectionType)cell.column().type;
                    sb.append("[").append(ct.nameComparator().getString(cell.path().get(0))).append("]");
                    sb.append("=").append(ct.valueComparator().getString(cell.value()));
                }
                else
                {
                    sb.append("=").append(cell.column().type.getString(cell.value()));
                }
            }
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Row))
            return false;

        Row that = (Row)other;
        if (!this.clustering().equals(that.clustering())
             || !this.columns().equals(that.columns())
             || !this.primaryKeyLivenessInfo().equals(that.primaryKeyLivenessInfo())
             || !this.deletion().equals(that.deletion()))
            return false;

        Iterator<Cell> thisCells = this.iterator();
        Iterator<Cell> thatCells = that.iterator();
        while (thisCells.hasNext())
        {
            if (!thatCells.hasNext() || !thisCells.next().equals(thatCells.next()))
                return false;
        }
        return !thatCells.hasNext();
    }

    @Override
    public int hashCode()
    {
        int hash = Objects.hash(clustering(), columns(), primaryKeyLivenessInfo(), deletion());
        for (Cell cell : this)
            hash += 31 * cell.hashCode();
        return hash;
    }
}
