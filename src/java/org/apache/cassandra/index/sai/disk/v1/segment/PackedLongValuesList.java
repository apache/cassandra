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
package org.apache.cassandra.index.sai.disk.v1.segment;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.apache.lucene.util.Accountable;

public class PackedLongValuesList implements Accountable
{
    private final List<PackedLongValues> values;
    private int FILTER_TYPES = 2;

    public PackedLongValuesList()
    {
        // make it 3 when suffix is implemented
        this.values = new ArrayList<>(FILTER_TYPES);
    }

    public final long size() {
        long size = 0;
        for (PackedLongValues value : values) 
        {
            size += value.size();
        }
        return size;
    }

    @Override
    public long ramBytesUsed()
    {
        long ramUsed = 0;
        for (PackedLongValues builder : values) 
        {
            ramUsed += builder.ramBytesUsed();
        }
        return ramUsed;
    }

    public void add(PackedLongValues value) {
        values.add(value);
    }

    /** Return an iterator over the values of this array. */
    public Iterator iterator() {
        return new Iterator();
    }

    public final class Iterator {

        PackedLongValues.Iterator[] iterators;
        long[] valueOffsets;
        int curIndexForValueOffsets = 0;
        public Iterator() {
            // initialize the iterator for each PackedLongValues in values list
            iterators = new PackedLongValues.Iterator[values.size()];
            valueOffsets = new long[FILTER_TYPES];
            int curOffset = 0;
            for (int i = 0; i < values.size(); i++) {
                iterators[i] = values.get(i).iterator();
                valueOffsets[i] = curOffset;
                curOffset += values.get(i).size();
            }
        }

        public final boolean hasNext() {

            if(curIndexForValueOffsets < FILTER_TYPES)
            {
                return true;
            }
            for (PackedLongValues.Iterator iterator : iterators) {
                if (iterator.hasNext())
                    return true;
            }
            return false;
        }

        /** Return the next long in the buffer. */
        public final long next() {
            assert hasNext();
            //This is used to first send all the offsets before sending the data. This could be uneven.
            if(curIndexForValueOffsets < FILTER_TYPES)
            {
                return valueOffsets[curIndexForValueOffsets++];
            }
            for (PackedLongValues.Iterator iterator : iterators) {
                if (iterator.hasNext())
                    return iterator.next();
            }
            throw new IllegalStateException("No more values left to iterate");
        }
    }
    
    public static class Builder implements Accountable 
    {
        long ramBytesUsed = 0;
        private List<PackedLongValues.Builder> builderList;
        int FILTER_TYPES = 2; 
        Builder() 
        {
            builderList = new ArrayList<>();
            for(int i=0;i<FILTER_TYPES;i++)
            {
                builderList.add(PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT));
            }
        }

        public PackedLongValuesList build()
        {
            PackedLongValuesList list = new PackedLongValuesList();
            for (PackedLongValues.Builder builder : builderList)
            {
                list.add(builder.build());
            }
            ramBytesUsed += list.ramBytesUsed();
            return list;
        }

        //type - 0: exactMatch, 1: prefix
        public Builder add(long l, int type)
        {
            builderList.get(type).add(l);
            return this;
        }

        public long ramBytesUsed()
        {
            return ramBytesUsed;
        }
    }

}
