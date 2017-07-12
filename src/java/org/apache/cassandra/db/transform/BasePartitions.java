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

import java.util.Collections;

import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Throwables.merge;

public abstract class BasePartitions<R extends BaseRowIterator<?>, I extends BasePartitionIterator<? extends BaseRowIterator<?>>>
extends BaseIterator<BaseRowIterator<?>, I, R>
implements BasePartitionIterator<R>
{

    public BasePartitions(I input)
    {
        super(input);
    }

    BasePartitions(BasePartitions<?, ? extends I> copyFrom)
    {
        super(copyFrom);
    }


    // *********************************


    protected BaseRowIterator<?> applyOne(BaseRowIterator<?> value, Transformation transformation)
    {
        return value == null ? null : transformation.applyToPartition(value);
    }

    void add(Transformation transformation)
    {
        transformation.attachTo(this);
        super.add(transformation);
        next = applyOne(next, transformation);
    }

    protected Throwable runOnClose(int length)
    {
        Throwable fail = null;
        Transformation[] fs = stack;
        for (int i = 0 ; i < length ; i++)
        {
            try
            {
                fs[i].onClose();
            }
            catch (Throwable t)
            {
                fail = merge(fail, t);
            }
        }
        return fail;
    }

    public final boolean hasNext()
    {
        BaseRowIterator<?> next = null;
        try
        {

            Stop stop = this.stop;
            while (this.next == null)
            {
                Transformation[] fs = stack;
                int len = length;

                while (!stop.isSignalled && input.hasNext())
                {
                    next = input.next();
                    for (int i = 0 ; next != null & i < len ; i++)
                        next = fs[i].applyToPartition(next);

                    if (next != null)
                    {
                        this.next = next;
                        return true;
                    }
                }

                if (stop.isSignalled || stopChild.isSignalled || !hasMoreContents())
                    return false;
            }
            return true;

        }
        catch (Throwable t)
        {
            if (next != null)
                Throwables.close(t, Collections.singleton(next));
            throw t;
        }
    }

}

