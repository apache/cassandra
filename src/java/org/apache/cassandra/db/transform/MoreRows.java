/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

import static org.apache.cassandra.db.transform.Transformation.add;
import static org.apache.cassandra.db.transform.Transformation.mutable;

/**
 * An interface for providing new row contents for a partition.
 *
 * The new contents are produced as a normal arbitrary RowIterator or UnfilteredRowIterator (as appropriate),
 * with matching staticRow, partitionKey and partitionLevelDeletion.
 *
 * The transforming iterator invokes this method when any current source is exhausted, then then inserts the
 * new contents as the new source.
 *
 * If the new source is itself a product of any transformations, the two transforming iterators are merged
 * so that control flow always occurs at the outermost point
 */
public interface MoreRows<I extends BaseRowIterator<?>> extends MoreContents<I>
{

    public static UnfilteredRowIterator extend(UnfilteredRowIterator iterator, MoreRows<? super UnfilteredRowIterator> more)
    {
        return add(mutable(iterator), more);
    }

    public static UnfilteredRowIterator extend(UnfilteredRowIterator iterator, MoreRows<? super UnfilteredRowIterator> more, RegularAndStaticColumns columns)
    {
        return add(Transformation.wrapIterator(iterator, columns), more);
    }

    public static RowIterator extend(RowIterator iterator, MoreRows<? super RowIterator> more)
    {
        return add(mutable(iterator), more);
    }

}

