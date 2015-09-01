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

import org.apache.cassandra.config.ColumnDefinition;
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
public abstract class AbstractCell extends Cell
{
    protected AbstractCell(ColumnDefinition column)
    {
        super(column);
    }

    public void digest(MessageDigest digest)
    {
        digest.update(value().duplicate());
        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithInt(digest, ttl());
        FBUtilities.updateWithBoolean(digest, isCounterCell());
        if (path() != null)
            path().digest(digest);
    }

    public void validate()
    {
        column().validateCellValue(value());

        if (ttl() < 0)
            throw new MarshalException("A TTL should not be negative");
        if (localDeletionTime() < 0)
            throw new MarshalException("A local deletion time should not be negative");
        if (isExpiring() && localDeletionTime() == NO_DELETION_TIME)
            throw new MarshalException("Shoud not have a TTL without an associated local deletion time");

        // If cell is a tombstone, it shouldn't have a value.
        if (isTombstone() && value().hasRemaining())
            throw new MarshalException("A tombstone should not have a value");

        if (path() != null)
            column().validateCellPath(path());
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if(!(other instanceof Cell))
            return false;

        Cell that = (Cell)other;
        return this.column().equals(that.column())
            && this.isCounterCell() == that.isCounterCell()
            && this.timestamp() == that.timestamp()
            && this.ttl() == that.ttl()
            && this.localDeletionTime() == that.localDeletionTime()
            && Objects.equals(this.value(), that.value())
            && Objects.equals(this.path(), that.path());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column(), isCounterCell(), timestamp(), ttl(), localDeletionTime(), value(), path());
    }

    @Override
    public String toString()
    {
        if (isCounterCell())
            return String.format("[%s=%d ts=%d]", column().name, CounterContext.instance().total(value()), timestamp());

        AbstractType<?> type = column().type;
        if (type instanceof CollectionType && type.isMultiCell())
        {
            CollectionType ct = (CollectionType)type;
            return String.format("[%s[%s]=%s %s]",
                                 column().name,
                                 ct.nameComparator().getString(path().get(0)),
                                 ct.valueComparator().getString(value()),
                                 livenessInfoString());
        }
        return String.format("[%s=%s %s]", column().name, type.getString(value()), livenessInfoString());
    }

    private String livenessInfoString()
    {
        if (isExpiring())
            return String.format("ts=%d ttl=%d ldt=%d", timestamp(), ttl(), localDeletionTime());
        else if (isTombstone())
            return String.format("ts=%d ldt=%d", timestamp(), localDeletionTime());
        else
            return String.format("ts=%d", timestamp());
    }

}
