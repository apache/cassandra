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
package org.apache.cassandra.db.rows;

import java.util.Iterator;

import org.apache.cassandra.db.Slice;

public interface SliceableUnfilteredRowIterator extends UnfilteredRowIterator
{
    /**
     * Move forward (resp. backward if isReverseOrder() is true for the iterator) in
     * the iterator and return an iterator over the Unfiltered selected by the provided
     * {@code slice}.
     * <p>
     * Please note that successive calls to {@code slice} are allowed provided the
     * slice are non overlapping and are passed in clustering (resp. reverse clustering) order.
     * However, {@code slice} is allowed to leave the iterator in an unknown state and there
     * is no guarantee over what a call to {@code hasNext} or {@code next} will yield after
     * a call to {@code slice}. In other words, for a given iterator, you should either use
     * {@code slice} or {@code hasNext/next} but not both.
     */
    public Iterator<Unfiltered> slice(Slice slice);
}
