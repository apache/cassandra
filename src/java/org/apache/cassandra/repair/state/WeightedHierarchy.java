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

package org.apache.cassandra.repair.state;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.cache.IMeasurableMemory;

/**
 * A pattern for tracking the weight of a hierarchy of references. Registered objects are treated as though they are
 * immutable, and are only measured on registration.
 *
 * Internal nodes must maintain a reference to the root, in order to notify it of any new registrations that may impact
 * the total weight of the hierarchy.
 */
class WeightedHierarchy
{
    interface Node
    {
        // How much does this object retain without any of its nested state
        long independentRetainedSize();
    }

    interface InternalNode extends Node
    {
        Root root();

        default void onNestedStateRegistration(Node nested)
        {
            Root root = root();
            root.totalNestedRetainedSize().addAndGet(nested.independentRetainedSize());
            root.onRetainedSizeUpdate();
        }
    }

    interface Root extends InternalNode, IMeasurableMemory
    {
        AtomicLong totalNestedRetainedSize();

        default long unsharedHeapSize()
        {
            return totalNestedRetainedSize().get() + independentRetainedSize();
        }

        void onRetainedSizeUpdate();
    }
}
