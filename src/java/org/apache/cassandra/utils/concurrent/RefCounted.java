/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;

/**
 * An object that needs ref counting does the following:
 *   - defines a Tidy object that will cleanup once it's gone,
 *     (this must retain no references to the object we're tracking (only its resources and how to clean up))
 *   - implements RefCounted
 *   - encapsulates a RefCounted.Impl, to which it proxies all calls to RefCounted behaviours
 *   - ensures no external access to the encapsulated Impl, and permits no references to it to leak
 *   - users must ensure no references to the sharedRef leak, or are retained outside of a method scope either.
 *     (to ensure the sharedRef is collected with the object, so that leaks may be detected and corrected)
 *
 * This class' functionality is achieved by what may look at first glance like a complex web of references,
 * but boils down to:
 *
 * Target --> Impl --> sharedRef --> [RefState] <--> RefCountedState --> Tidy
 *                                        ^                ^
 *                                        |                |
 * Ref -----------------------------------                 |
 *                                                         |
 * Global -------------------------------------------------
 *
 * So that, if Target is collected, Impl is collected and, hence, so is sharedRef.
 *
 * Once ref or sharedRef are collected, the paired RefState's release method is called, which if it had
 * not already been called will update RefCountedState and log an error.
 *
 * Once the RefCountedState has been completely released, the Tidy method is called and it removes the global reference
 * to itself so it may also be collected.
 */
public interface RefCounted
{

    /**
     * @return the a new Ref() to the managed object, incrementing its refcount, or null if it is already released
     */
    public Ref tryRef();

    /**
     * @return the shared Ref that is created at instantiation of the RefCounted instance.
     * Once released, if no other refs are extant the object will be tidied; references to
     * this object should never be retained outside of a method's scope
     */
    public Ref sharedRef();

    public static interface Tidy
    {
        void tidy();
        String name();
    }

    public static class Impl
    {
        public static RefCounted get(Tidy tidy)
        {
            return new RefCountedImpl(tidy);
        }
    }
}
