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

}
