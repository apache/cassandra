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
package org.apache.cassandra.db.rows;

import java.security.MessageDigest;
import java.util.Objects;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Base abstract class for {@code Cell} implementations.
 *
 * Unless you have a very good reason not to, every cell implementation
 * should probably extend this class.
 */
public abstract class AbstractCell implements Cell
{
    public boolean isLive(int nowInSec)
    {
        return livenessInfo().isLive(nowInSec);
    }

    public boolean isTombstone()
    {
        return livenessInfo().hasLocalDeletionTime() && !livenessInfo().hasTTL();
    }

    public boolean isExpiring()
    {
        return livenessInfo().hasTTL();
    }

    public void writeTo(Row.Writer writer)
    {
        writer.writeCell(column(), isCounterCell(), value(), livenessInfo(), path());
    }

    public void digest(MessageDigest digest)
    {
        digest.update(value().duplicate());
        livenessInfo().digest(digest);
        FBUtilities.updateWithBoolean(digest, isCounterCell());
        if (path() != null)
            path().digest(digest);
    }

    public void validate()
    {
        column().validateCellValue(value());

        livenessInfo().validate();

        // If cell is a tombstone, it shouldn't have a value.
        if (isTombstone() && value().hasRemaining())
            throw new MarshalException("A tombstone should not have a value");

        if (path() != null)
            column().validateCellPath(path());
    }

    public int dataSize()
    {
        int size = value().remaining() + livenessInfo().dataSize();
        if (path() != null)
            size += path().dataSize();
        return size;

    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Cell))
            return false;

        Cell that = (Cell)other;
        return this.column().equals(that.column())
            && this.isCounterCell() == that.isCounterCell()
            && Objects.equals(this.value(), that.value())
            && Objects.equals(this.livenessInfo(), that.livenessInfo())
            && Objects.equals(this.path(), that.path());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column(), isCounterCell(), value(), livenessInfo(), path());
    }

    @Override
    public String toString()
    {
        if (isCounterCell())
            return String.format("[%s=%d ts=%d]", column().name, CounterContext.instance().total(value()), livenessInfo().timestamp());

        AbstractType<?> type = column().type;
        if (type instanceof CollectionType && type.isMultiCell())
        {
            CollectionType ct = (CollectionType)type;
            return String.format("[%s[%s]=%s info=%s]",
                                 column().name,
                                 ct.nameComparator().getString(path().get(0)),
                                 ct.valueComparator().getString(value()),
                                 livenessInfo());
        }
        return String.format("[%s=%s info=%s]", column().name, type.getString(value()), livenessInfo());
    }

    public Cell takeAlias()
    {
        // Cell is always used as an Aliasable object but as the code currently
        // never need to alias a cell outside of its valid scope, we don't yet
        // need that.
        throw new UnsupportedOperationException();
    }
}
