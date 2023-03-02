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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Abstract class to make writing unfiltered iterators that wrap another iterator
 * easier. By default, the wrapping iterator simply delegate every call to
 * the wrapped iterator so concrete implementations will have to override some methods.
 * <p>
 * Note that if most of what you want to do is modifying/filtering the returned
 * {@code Unfiltered}, {@link org.apache.cassandra.db.transform.Transformation#apply(UnfilteredRowIterator, Transformation)}
 * can be a simpler option.
 */
public interface WrappingUnfilteredRowIterator extends UnfilteredRowIterator
{
    UnfilteredRowIterator wrapped();

    default TableMetadata metadata()
    {
        return wrapped().metadata();
    }

    default RegularAndStaticColumns columns()
    {
        return wrapped().columns();
    }

    default boolean isReverseOrder()
    {
        return wrapped().isReverseOrder();
    }

    default DecoratedKey partitionKey()
    {
        return wrapped().partitionKey();
    }

    default DeletionTime partitionLevelDeletion()
    {
        return wrapped().partitionLevelDeletion();
    }

    default Row staticRow()
    {
        return wrapped().staticRow();
    }

    default EncodingStats stats()
    {
        return wrapped().stats();
    }

    default boolean hasNext()
    {
        return wrapped().hasNext();
    }

    default Unfiltered next()
    {
        return wrapped().next();
    }

    default void close()
    {
        wrapped().close();
    }
}