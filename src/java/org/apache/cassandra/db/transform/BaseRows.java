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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.*;

import static org.apache.cassandra.utils.Throwables.merge;

public abstract class BaseRows<R extends Unfiltered, I extends BaseRowIterator<? extends Unfiltered>>
extends BaseIterator<Unfiltered, I, R>
implements BaseRowIterator<R>
{

    private Row staticRow;
    private DecoratedKey partitionKey;

    public BaseRows(I input)
    {
        super(input);
        staticRow = input.staticRow();
        partitionKey = input.partitionKey();
    }

    // swap parameter order to avoid casting errors
    BaseRows(BaseRows<?, ? extends I> copyFrom)
    {
        super(copyFrom);
        staticRow = copyFrom.staticRow;
        partitionKey = copyFrom.partitionKey();
    }

    public CFMetaData metadata()
    {
        return input.metadata();
    }

    public boolean isReverseOrder()
    {
        return input.isReverseOrder();
    }

    public PartitionColumns columns()
    {
        return input.columns();
    }

    public DecoratedKey partitionKey()
    {
        return input.partitionKey();
    }

    public Row staticRow()
    {
        return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
    }


    // **************************


    @Override
    protected Throwable runOnClose(int length)
    {
        Throwable fail = null;
        Transformation[] fs = stack;
        for (int i = 0 ; i < length ; i++)
        {
            try
            {
                fs[i].onPartitionClose();
            }
            catch (Throwable t)
            {
                fail = merge(fail, t);
            }
        }
        return fail;
    }

    @Override
    void add(Transformation transformation)
    {
        transformation.attachTo(this);
        super.add(transformation);

        // transform any existing data
        if (staticRow != null)
            staticRow = transformation.applyToStatic(staticRow);
        next = applyOne(next, transformation);
        partitionKey = transformation.applyToPartitionKey(partitionKey);
    }

    @Override
    protected Unfiltered applyOne(Unfiltered value, Transformation transformation)
    {
        return value == null
               ? null
               : value instanceof Row
                 ? transformation.applyToRow((Row) value)
                 : transformation.applyToMarker((RangeTombstoneMarker) value);
    }

    @Override
    public final boolean hasNext()
    {
        Stop stop = this.stop;
        while (this.next == null)
        {
            Transformation[] fs = stack;
            int len = length;

            while (!stop.isSignalled && !stopChild.isSignalled && input.hasNext())
            {
                Unfiltered next = input.next();

                if (next.isRow())
                {
                    Row row = (Row) next;
                    for (int i = 0 ; row != null && i < len ; i++)
                        row = fs[i].applyToRow(row);
                    next = row;
                }
                else
                {
                    RangeTombstoneMarker rtm = (RangeTombstoneMarker) next;
                    for (int i = 0 ; rtm != null && i < len ; i++)
                        rtm = fs[i].applyToMarker(rtm);
                    next = rtm;
                }

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
}
