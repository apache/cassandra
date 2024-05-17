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

import java.util.Objects;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.rows.CellPath;

public abstract class MultiCellCapableType<T> extends AbstractType<T>
{
    protected MultiCellCapableType(ComparisonType comparisonType, boolean isMultiCell, ImmutableList<AbstractType<?>> subTypes)
    {
        super(comparisonType, isMultiCell, subTypes);
    }

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

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (equals(previous))
            return true;

        if (!(previous instanceof MultiCellCapableType))
            return false;

        if (this.isMultiCell() != previous.isMultiCell())
            return false;

        MultiCellCapableType<?> tprev = (MultiCellCapableType<?>) previous;
        if (!this.isMultiCell())
            return isCompatibleWithFrozen(tprev);

        if (!this.nameComparator().isCompatibleWith(tprev.nameComparator()))
            return false;

        // the value comparator is only used for Cell values, so sorting doesn't matter
        return isSubTypesCompatibleWith(tprev, AbstractType::isSerializationCompatibleWith);
    }

    /** A version of isCompatibleWith() to deal with non-multicell (frozen) types */
    protected boolean isCompatibleWithFrozen(MultiCellCapableType<?> previous)
    {
        return isSubTypesCompatibleWith(previous, AbstractType::isCompatibleWith);
    }

    @Override
    protected boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        if (Objects.equals(this, previous))
            return true;

        // for multi-cell collections, compatibility and value-compatibility are the same
        if (isMultiCell())
            return isCompatibleWith(previous);

        if (previous.isMultiCell())
            return false;

        if (!(previous instanceof MultiCellCapableType))
            return false;

        MultiCellCapableType<?> tprev = (MultiCellCapableType<?>) previous;
        return isValueCompatibleWithFrozen(tprev);
    }

    /** A version of isValueCompatibleWith() to deal with non-multicell (frozen) types */
    protected boolean isValueCompatibleWithFrozen(MultiCellCapableType<?> previous)
    {
        return isSubTypesCompatibleWith(previous, AbstractType::isValueCompatibleWith);
    }

}
