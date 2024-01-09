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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Base class for all types that have the capacity of being multi-cell (when not frozen).
 *
 * <p>A multi-cell type is one whose value is composed of multiple sub-values that are layout on multiple {@link Cell}
 * (one for each sub-value), typically collections. Being layout on multiple cell allows partial update (of only some
 * of the sub-values) without read-before write.
 *
 * <p>All multi-cell capable types can either be used as truly multi-cell types, or can be used frozen. In the later
 * case, the values are not layout in multiple cells, but instead the whole value (with all its sub-values) is packed
 * within a single cell value (which imply no partial update without read-before-write in particular).
 * {@link #isMultiCell()} allows to know if a given type is a multi-cell variant or a frozen one (both are technically
 * 2 different types, that have the same values from a user point of view, just with different capability).
 *
 * @param <T> the type of the values of this type.
 */
public abstract class MultiCellCapableType<T> extends AbstractType<T>
{

    protected MultiCellCapableType(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, isMultiCell, subTypes);
    }

    // Overriding as abstract to prevent subclasses from relying on the default implementation, as it's not appropriate.
    @Override
    public abstract AbstractType<?> with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell);

    /**
     * The subtype/comparator to use for the {@link CellPath} part of cells forming values for this type when used in
     * its multi-cell variant.
     *
     * <p>Note: in theory, we shouldn't have to access this on frozen instances (where {@code isMultiCell() == false}),
     * but for convenience, it is expected that this method always returns a proper value "as if" the type was a
     * multi-cell variant, even if it is not.
     *
     * @return the comparator for the {@link CellPath} component of cells of this type.
     */
    public abstract AbstractType<?> nameComparator();

    public abstract ByteBuffer serializeForNativeProtocol(Iterator<Cell<?>> cells, ProtocolVersion version);

    /**
     * Whether {@code this} type is compatible (including for sorting) with {@code previous}, assuming both are
     * of the same class (so {@code previous} can be safely cast to whichever class implements this) and both are
     * frozen.
     */
    protected abstract boolean isCompatibleWhenFrozenWith(AbstractType<?> previous);

    /**
     * Whether {@code this} type is compatible (including for sorting) with {@code previous}, assuming both are
     * of the same class (so {@code previous} can be safely cast to whichever class implements this) but neither
     * are frozen.
     */
    protected abstract boolean isCompatibleWhenNonFrozenWith(AbstractType<?> previous);

    /**
     * Whether {@code this} type is value-compatible with {@code previous}, assuming both are of the same class (so
     * {@code previous} can be safely cast to whichever class implements this) and both are are frozen.
     */
    protected abstract boolean isValueCompatibleWhenFrozenWith(AbstractType<?> previous);

    /**
     *  Whether {@code this} type is equal to {@code that} type, assuming both are of the same class (so
     * {@code that} can be safely cast to whichever class implements this), that
     * {@code this.isMultiCell() == that.isMultiCell()} and that {@code this.subTypes().equals(that.subTypes())}.
     */
    protected abstract boolean equalsNoFrozenNoSubtypes(AbstractType<?> that);

    @Override
    public final boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!getClass().equals(previous.getClass()))
            return false;

        if (isMultiCell() != previous.isMultiCell())
            return false;

        return isMultiCell() ? isCompatibleWhenNonFrozenWith(previous) : isCompatibleWhenFrozenWith(previous);
    }

    @Override
    protected final boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!getClass().equals(previous.getClass()))
            return false;

        if (isMultiCell() != previous.isMultiCell())
            return false;

        // For multi-cell, there is no difference between compatible and value-compatible: multi-cell types can only be
        // used as non-primary key column values, and so in a way are always checked for "value compatibility". Though
        // even when compared as values, they have sub-parts that end up in a CellPath, which are sorted, and may thus
        // require sorted compatibility. It would almost make sense to reject isCompatibleWith when types are
        // multi-cell, but that could be a tad error prone/inconvenient, so instead we just make both versions do the
        // same thing (hence the call to isCompatibleWhenNonFrozenWith like in isCompatibleWith).
        return isMultiCell() ? isCompatibleWhenNonFrozenWith(previous) : isValueCompatibleWhenFrozenWith(previous);
    }

    @Override
    public final boolean equals(Object that)
    {
        if (this == that)
            return true;

        if (!getClass().equals(that.getClass()))
            return false;

        AbstractType<?> other = (AbstractType<?>) that;
        return isMultiCell() == other.isMultiCell()
               && subTypes.equals(other.subTypes)
               && equalsNoFrozenNoSubtypes(other);
    }
}
