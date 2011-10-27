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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntervalNode
{
    private static final Logger logger = LoggerFactory.getLogger(IntervalNode.class);

    Comparable v_pt;
    Comparable v_min;
    Comparable v_max;
    List<Interval> intersects_left;
    List<Interval> intersects_right;
    IntervalNode left = null;
    IntervalNode right = null;

    public IntervalNode(List<Interval> toBisect)
    {
        logger.debug("Creating IntervalNode from {}", toBisect);

        if (toBisect.size() > 0)
        {
            findMinMedianMax(toBisect);
            List<Interval> intersects = getIntersectingIntervals(toBisect);
            intersects_left = Interval.minOrdering.sortedCopy(intersects);
            intersects_right = Interval.maxOrdering.reverse().sortedCopy(intersects);
            //if i.max < v_pt then it goes to the left subtree
            List<Interval> leftSegment = getLeftIntervals(toBisect);
            List<Interval> rightSegment = getRightIntervals(toBisect);
            if (leftSegment.size() > 0)
                this.left = new IntervalNode(leftSegment);
            if (rightSegment.size() > 0)
                this.right = new IntervalNode(rightSegment);
        }
    }

    public List<Interval> getLeftIntervals(List<Interval> candidates)
    {
        List<Interval> retval = new ArrayList<Interval>();
        for (Interval candidate : candidates)
        {
            if (candidate.max.compareTo(v_pt) < 0)
                retval.add(candidate);
        }
        return retval;
    }

    public List<Interval> getRightIntervals(List<Interval> candidates)
    {
        List<Interval> retval = new ArrayList<Interval>();
        for (Interval candidate : candidates)
        {
            if (candidate.min.compareTo(v_pt) > 0)
                retval.add(candidate);
        }
        return retval;
    }

    public List<Interval> getIntersectingIntervals(List<Interval> candidates)
    {
        List<Interval> retval = new ArrayList<Interval>();
        for (Interval candidate : candidates)
        {
            if (candidate.min.compareTo(v_pt) <= 0
                && candidate.max.compareTo(v_pt) >= 0)
                retval.add(candidate);
        }
        return retval;
    }

    public void findMinMedianMax(List<Interval> intervals)
    {
        if (intervals.size() > 0)
        {
            List<Comparable> allEndpoints = new ArrayList<Comparable>(intervals.size() * 2);

            for (Interval interval : intervals)
            {
                allEndpoints.add(interval.min);
                allEndpoints.add(interval.max);
            }
            Collections.sort(allEndpoints);
            v_pt = allEndpoints.get(intervals.size());
            v_min = allEndpoints.get(0);
            v_max = allEndpoints.get(allEndpoints.size() - 1);
        }
    }
}
