package org.apache.cassandra.utils.IntervalTree;
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


import com.google.common.collect.Ordering;

public class Interval<T>
{
    public final Comparable min;
    public final Comparable max;
    public final T Data;

    public Interval(Comparable min, Comparable max)
    {
        this.min = min;
        this.max = max;
        this.Data = null;
    }

    public Interval(Comparable min, Comparable max, T data)
    {
        this.min = min;
        this.max = max;
        this.Data = data;
    }

    public boolean encloses(Interval interval)
    {
        return (this.min.compareTo(interval.min) <= 0
                && this.max.compareTo(interval.max) >= 0);
    }

    public boolean contains(Comparable point)
    {
        return (this.min.compareTo(point) <= 0
                && this.max.compareTo(point) >= 0);
    }

    public boolean intersects(Interval interval)
    {
        return this.contains(interval.min) || this.contains(interval.max);
    }


    public static final Ordering<Interval> minOrdering = new Ordering<Interval>()
    {
        public int compare(Interval interval, Interval interval1)
        {
            return interval.min.compareTo(interval1.min);
        }
    };

    public static final Ordering<Interval> maxOrdering = new Ordering<Interval>()
    {
        public int compare(Interval interval, Interval interval1)
        {
            return interval.max.compareTo(interval1.max);
        }
    };

    public String toString()
    {
        return String.format("Interval(%s, %s)", min, max);
    }
}
