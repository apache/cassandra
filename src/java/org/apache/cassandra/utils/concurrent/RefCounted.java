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
 * An object that needs ref counting does the two following:
 *   - defines a Tidy object that will cleanup once it's gone,
 *     (this must retain no references to the object we're tracking (only its resources and how to clean up))
 * Then, one of two options:
 * 1) Construct a Ref directly pointing to it, and always use this Ref; or
 * 2)
 *   - implements RefCounted
 *   - encapsulates a Ref, we'll call selfRef, to which it proxies all calls to RefCounted behaviours
 *   - users must ensure no references to the selfRef leak, or are retained outside of a method scope.
 *     (to ensure the selfRef is collected with the object, so that leaks may be detected and corrected)
 */
public interface RefCounted<T>
{
    /**
     * @return the a new Ref() to the managed object, incrementing its refcount, or null if it is already released
     */
    public Ref<T> tryRef();

    public Ref<T> ref();

    public static interface Tidy
    {
        void tidy() throws Exception;
        String name();
    }
}
