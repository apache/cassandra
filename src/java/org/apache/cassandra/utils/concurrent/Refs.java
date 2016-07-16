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
package org.apache.cassandra.utils.concurrent;

import java.util.*;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * A collection of managed Ref references to RefCounted objects, and the objects they are referencing.
 * Care MUST be taken when using this collection, as if a permanent reference to it leaks we will not
 * be alerted to a lack of reference release.
 *
 * All of the java.util.Collection operations that modify the collection are unsupported.
 */
public final class Refs<T extends RefCounted<T>> extends AbstractCollection<T> implements AutoCloseable
{
    private final Map<T, Ref<T>> references;

    public Refs()
    {
        this.references = new HashMap<>();
    }

    public Refs(Map<T, Ref<T>> references)
    {
        this.references = new HashMap<>(references);
    }

    /**
     * Release ALL of the references held by this Refs collection
     */
    public void release()
    {
        try
        {
            release(references.values());
        }
        finally
        {
            references.clear();
        }
    }

    /**
     * See {@link Refs#release()}
     */
    public void close()
    {
        release();
    }

    /**
     * @param referenced the object we have a Ref to
     * @return the Ref to said object
     */
    public Ref<T> get(T referenced)
    {
        return references.get(referenced);
    }

    /**
     * @param referenced the object we have a Ref to
     */
    public void release(T referenced)
    {
        Ref ref = references.remove(referenced);
        if (ref == null)
            throw new IllegalStateException("This Refs collection does not hold a reference to " + referenced);
        ref.release();
    }

    /**
     * Release the retained Ref to the provided object, if held, return false otherwise
     * @param referenced the object we retain a Ref to
     * @return return true if we held a reference to the object, and false otherwise
     */
    public boolean releaseIfHolds(T referenced)
    {
        Ref ref = references.remove(referenced);
        if (ref != null)
            ref.release();
        return ref != null;
    }

    public void relaseAllExcept(Collection<T> keep)
    {
        Collection<T> release = new ArrayList<>(references.keySet());
        release.retainAll(keep);
        release(release);
    }
    /**
     * Release a retained Ref to all of the provided objects; if any is not held, an exception will be thrown
     * @param release
     */
    public void release(Collection<T> release)
    {
        List<Ref<T>> refs = new ArrayList<>();
        List<T> notPresent = null;
        for (T obj : release)
        {
            Ref<T> ref = references.remove(obj);
            if (ref == null)
            {
                if (notPresent == null)
                    notPresent = new ArrayList<>();
                notPresent.add(obj);
            }
            else
            {
                refs.add(ref);
            }
        }

        IllegalStateException notPresentFail = null;
        if (notPresent != null)
        {
            notPresentFail = new IllegalStateException("Could not release references to " + notPresent
                                                       + " as references to these objects were not held");
            notPresentFail.fillInStackTrace();
        }
        try
        {
            release(refs);
        }
        catch (Throwable t)
        {
            if (notPresentFail != null)
                t.addSuppressed(notPresentFail);
        }
        if (notPresentFail != null)
            throw notPresentFail;
    }

    /**
     * Attempt to take a reference to the provided object; if it has already been released, null will be returned
     * @param t object to acquire a reference to
     * @return true iff success
     */
    public boolean tryRef(T t)
    {
        Ref<T> ref = t.tryRef();
        if (ref == null)
            return false;
        ref = references.put(t, ref);
        if (ref != null)
            ref.release(); // release dup
        return true;
    }

    public Iterator<T> iterator()
    {
        return Iterators.unmodifiableIterator(references.keySet().iterator());
    }

    public int size()
    {
        return references.size();
    }

    /**
     * Merge two sets of references, ensuring only one reference is retained between the two sets
     */
    public Refs<T> addAll(Refs<T> add)
    {
        List<Ref<T>> overlap = new ArrayList<>();
        for (Map.Entry<T, Ref<T>> e : add.references.entrySet())
        {
            if (this.references.containsKey(e.getKey()))
                overlap.add(e.getValue());
            else
                this.references.put(e.getKey(), e.getValue());
        }
        add.references.clear();
        release(overlap);
        return this;
    }

    /**
     * Acquire a reference to all of the provided objects, or none
     */
    public static <T extends RefCounted<T>> Refs<T> tryRef(Iterable<T> reference)
    {
        HashMap<T, Ref<T>> refs = new HashMap<>();
        for (T rc : reference)
        {
            Ref<T> ref = rc.tryRef();
            if (ref == null)
            {
                release(refs.values());
                return null;
            }
            refs.put(rc, ref);
        }
        return new Refs<T>(refs);
    }

    public static <T extends RefCounted<T>> Refs<T> ref(Iterable<T> reference)
    {
        Refs<T> refs = tryRef(reference);
        if (refs != null)
            return refs;
        throw new IllegalStateException();
    }

    public static void release(Iterable<? extends Ref<?>> refs)
    {
        maybeFail(release(refs, null));
    }
    public static Throwable release(Iterable<? extends Ref<?>> refs, Throwable accumulate)
    {
        for (Ref ref : refs)
        {
            try
            {
                ref.release();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    public static <T extends SelfRefCounted<T>> Iterable<Ref<T>> selfRefs(Iterable<T> refs)
    {
        return Iterables.transform(refs, new Function<T, Ref<T>>()
        {
            @Nullable
            public Ref<T> apply(T t)
            {
                return t.selfRef();
            }
        });
    }
}
