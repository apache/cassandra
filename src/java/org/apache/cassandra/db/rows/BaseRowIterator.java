/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.rows;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * A common interface for Row and Unfiltered, that permits sharing of the (majority) common
 * methods and functionality
 */
public interface BaseRowIterator<U extends Unfiltered> extends CloseableIterator<U>
{
    /**
     * The metadata for the table this iterator on.
     */
    public TableMetadata metadata();

    /**
     * Whether or not the rows returned by this iterator are in reversed
     * clustering order.
     */
    public boolean isReverseOrder();

    /**
     * A subset of the columns for the (static and regular) rows returned by this iterator.
     * Every row returned by this iterator must guarantee that it has only those columns.
     */
    public RegularAndStaticColumns columns();

    /**
     * The partition key of the partition this in an iterator over.
     */
    public DecoratedKey partitionKey();

    /**
     * The static part corresponding to this partition (this can be an empty
     * row but cannot be {@code null}).
     */
    public Row staticRow();

    /**
     * Returns whether the provided iterator has no data.
     */
    public boolean isEmpty();
}
