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

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.db.rows.BaseRowIterator;

// A Transformation that can stop an iterator earlier than its natural exhaustion
public abstract class StoppingTransformation<I extends BaseRowIterator<?>> extends Transformation<I>
{
    private BaseIterator rows;
    private BaseIterator partitions;

    /**
     * If invoked by a subclass, any partitions iterator this transformation has been applied to will terminate
     * after any currently-processing item is returned, as will any row/unfiltered iterator
     */
    @DontInline
    protected void stop()
    {
        if (partitions != null)
        {
            partitions.stop.isSignalled = true;
            partitions.stopChild.isSignalled = true;
        }

        stopInPartition();
    }

    /**
     * If invoked by a subclass, any rows/unfiltered iterator this transformation has been applied to will terminate
     * after any currently-processing item is returned
     */
    @DontInline
    protected void stopInPartition()
    {
        if (rows != null)
        {
            rows.stop.isSignalled = true;
            rows.stopChild.isSignalled = true;
        }
    }

    @Override
    protected void attachTo(BasePartitions partitions)
    {
        assert this.partitions == null;
        this.partitions = partitions;
    }

    @Override
    protected void attachTo(BaseRows rows)
    {
        assert this.rows == null;
        this.rows = rows;
    }

    @Override
    protected void onClose()
    {
        partitions = null;
    }

    @Override
    protected void onPartitionClose()
    {
        rows = null;
    }
}
