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

import org.apache.cassandra.db.rows.*;

/**
 * An iterator over a number of (filtered) partition.
 *
 * PartitionIterator is to RowIterator what UnfilteredPartitionIterator is to UnfilteredRowIterator
 * though unlike UnfilteredPartitionIterator, it is not guaranteed that the RowIterator
 * returned are in partitioner order.
 *
 * The object returned by a call to next() is only guaranteed to be
 * valid until the next call to hasNext() or next(). If a consumer wants to keep a
 * reference on the returned objects for longer than the iteration, it must
 * make a copy of it explicitely.
 */
public interface PartitionIterator extends BasePartitionIterator<RowIterator>
{
}
