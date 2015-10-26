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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * An iterator over a number of unfiltered partitions (i.e. partitions containing deletion informations).
 *
 * The object returned by a call to next() is only guaranteed to be
 * valid until the next call to hasNext() or next(). If a consumer wants to keep a
 * reference on the returned objects for longer than the iteration, it must
 * make a copy of it explicitely.
 */
public interface UnfilteredPartitionIterator extends BasePartitionIterator<UnfilteredRowIterator>
{
    /**
     * Whether that partition iterator is for a thrift queries.
     * <p>
     * If this is true, the partition iterator may return some empty UnfilteredRowIterator and those
     * should be preserved as thrift include partitions that "exists" (have some cells even
     * if this are actually deleted) but have nothing matching the query.
     *
     * @return whether the iterator is for a thrift query.
     */
    public boolean isForThrift();

    public CFMetaData metadata();
}
