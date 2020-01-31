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
import java.util.AbstractCollection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
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

    @Override
    public boolean hasLiveData(int nowInSec, boolean enforceStrictLiveness)
    {
        if (primaryKeyLivenessInfo().isLive(nowInSec))
            return true;
        else if (enforceStrictLiveness)
            return false;
        return Iterables.any(cells(), cell -> cell.isLive(nowInSec));
    }

    public boolean isStatic()
    {
        return clustering() == Clustering.STATIC_CLUSTERING;
    }

    public void digest(MessageDigest digest)
    {
        digest(digest, Collections.emptySet());
    }

    public void digest(MessageDigest digest, Set<ByteBuffer> columnsToExclude)
    {
        FBUtilities.updateWithByte(digest, kind().ordinal());
        clustering().digest(digest);

        deletion().digest(digest);
        primaryKeyLivenessInfo().digest(digest);

        for (ColumnData cd : this)
            if (!columnsToExclude.contains(cd.column.name.bytes))
                cd.digest(digest);
    }

    public void validateData(CFMetaData metadata)
    {
        Clustering clustering = clustering();
        for (int i = 0; i < clustering.size(); i++)
        {
            ByteBuffer value = clustering.get(i);
            if (value != null)
            {
                try
                {
                    metadata.comparator.subtype(i).validate(value);
                }
                catch (Exception e)
                {
                    throw new MarshalException("comparator #" + i + " '" + metadata.comparator.subtype(i) + "' in '" + metadata + "' didn't validate", e);
                }
            }
        }

        primaryKeyLivenessInfo().validate();
        if (deletion().time().localDeletionTime() < 0)
            throw new MarshalException("A local deletion time should not be negative in '" + metadata + "'");

        for (ColumnData cd : this)
            try
            {
                cd.validate();
            }
            catch (Exception e)
            {
                throw new MarshalException("data for '" + cd.column.debugString() + "', " + cd + " in '" + metadata + "' didn't validate", e);
            }
    }

    public String toString()
    {
        return columnData().toString();
    }

    public String toString(CFMetaData metadata)
    {
        return toString(metadata, false);
    }

    public String toString(CFMetaData metadata, boolean fullDetails)
    {
        return toString(metadata, true, fullDetails);
    }

    public String toString(CFMetaData metadata, boolean includeClusterKeys, boolean fullDetails)
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
        sb.append(": ");
        if(includeClusterKeys)
            sb.append(clustering().toString(metadata));
        else
            sb.append(clustering().toCQLString(metadata));
        sb.append(" | ");
        boolean isFirst = true;
        for (ColumnData cd : this)
        {
            if (isFirst) isFirst = false; else sb.append(", ");
            if (fullDetails)
            {
                if (cd.column().isSimple())
                {
                    sb.append(cd);
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    if (!complexData.complexDeletion().isLive())
                        sb.append("del(").append(cd.column().name).append(")=").append(complexData.complexDeletion());
                    for (Cell cell : complexData)
                        sb.append(", ").append(cell);
                }
            }
            else
            {
                if (cd.column().isSimple())
                {
                    Cell cell = (Cell)cd;
                    sb.append(cell.column().name).append('=');
                    if (cell.isTombstone())
                        sb.append("<tombstone>");
                    else
                        sb.append(cell.column().type.getString(cell.value()));
                }
                else
                {
                    sb.append(cd.column().name).append('=');
                    ComplexColumnData complexData = (ComplexColumnData) cd;
                    Function<Cell, String> transform = null;
                    if (cd.column().type.isCollection())
                    {
                        CollectionType ct = (CollectionType) cd.column().type;
                        transform = cell -> String.format("%s -> %s",
                                                  ct.nameComparator().getString(cell.path().get(0)),
                                                  ct.valueComparator().getString(cell.value()));

                    }
                    else if (cd.column().type.isUDT())
                    {
                        UserType ut = (UserType)cd.column().type;
                        transform = cell -> {
                            Short fId = ut.nameComparator().getSerializer().deserialize(cell.path().get(0));
                            return String.format("%s -> %s",
                                                 ut.fieldNameAsString(fId),
                                                 ut.fieldType(fId).getString(cell.value()));
                        };
                    }
                    transform = transform != null ? transform : cell -> "";
                    sb.append(StreamSupport.stream(complexData.spliterator(), false)
                                           .map(transform)
                                           .collect(Collectors.joining(", ", "{", "}")));
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
             || !this.primaryKeyLivenessInfo().equals(that.primaryKeyLivenessInfo())
             || !this.deletion().equals(that.deletion()))
            return false;

        return Iterables.elementsEqual(this, that);
    }

    @Override
    public int hashCode()
    {
        int hash = Objects.hash(clustering(), primaryKeyLivenessInfo(), deletion());
        for (ColumnData cd : this)
            hash += 31 * cd.hashCode();
        return hash;
    }
}
