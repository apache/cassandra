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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

final class UnfilteredPartitions extends BasePartitions<UnfilteredRowIterator, UnfilteredPartitionIterator> implements UnfilteredPartitionIterator
{
    final boolean isForThrift;

    // wrap an iterator for transformation
    public UnfilteredPartitions(UnfilteredPartitionIterator input)
    {
        super(input);
        this.isForThrift = input.isForThrift();
    }

    public boolean isForThrift()
    {
        return isForThrift;
    }

    public CFMetaData metadata()
    {
        return input.metadata();
    }
}
