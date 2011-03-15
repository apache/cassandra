package org.apache.cassandra.utils;
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


import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

/**
 * reduces equal values from the source iterator to a single (optionally transformed) instance.
 */
public abstract class ReducingIterator<T1, T2> extends AbstractIterator<T2> implements Iterator<T2>, Iterable<T2>
{
    protected Iterator<T1> source;
    protected T1 last;

    public ReducingIterator(Iterator<T1> source)
    {
        this.source = source;
    }

    /** combine this object with the previous ones.  intermediate state is up to your implementation. */
    public abstract void reduce(T1 current);

    /** return the last object computed by reduce */
    protected abstract T2 getReduced();

    /** override this if the keys you want to base the reduce on are not the same as the object itself (but can be generated from it) */
    protected boolean isEqual(T1 o1, T1 o2)
    {
        return o1.equals(o2);
    }

    protected T2 computeNext()
    {
        if (last == null && !source.hasNext())
            return endOfData();

        onKeyChange();
        boolean keyChanged = false;
        while (!keyChanged)
        {
            if (last != null)
                reduce(last);
            if (!source.hasNext())
            {
                last = null;
                break;
            }
            T1 current = source.next();
            if (last != null && !isEqual(current, last))
                keyChanged = true;
            last = current;
        }
        return getReduced();
    }

    /**
     * Called at the begining of each new key, before any reduce is called.
     * To be overriden by implementing classes.
     */
    protected void onKeyChange() {}

    public Iterator<T2> iterator()
    {
        return this;
    }
}
