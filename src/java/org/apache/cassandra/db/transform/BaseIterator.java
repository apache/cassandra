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

import java.util.Iterator;
import java.util.NoSuchElementException;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

abstract class BaseIterator<V, I extends CloseableIterator<? extends V>, O extends V> extends Stack implements AutoCloseable, Iterator<O>
{
    I input;
    V next;

    // We require two stop signals for correctness, since the `stop` reference of the base iterator can "leak"
    // into the transformations stack. Using a single `stop` signal may result into the inconsistent state,
    // since stopping transformation would stop only the child iterator.

    // Signals that the base iterator has been signalled to stop. Applies at the end of the current next().
    Stop stop;
    // Signals that the current child iterator has been signalled to stop.
    Stop stopChild;

    static class Stop
    {
        // TODO: consider moving "next" into here, so that a stop() when signalled outside of a function call (e.g. in attach)
        // can take effect immediately; this doesn't seem to be necessary at the moment, but it might cause least surprise in future
        boolean isSignalled;
    }

    // responsibility for initialising next lies with the subclass
    BaseIterator(BaseIterator<? extends V, ? extends I, ?> copyFrom)
    {
        super(copyFrom);
        this.input = copyFrom.input;
        this.next = copyFrom.next;
        this.stop = copyFrom.stop;
        this.stopChild = copyFrom.stopChild;
    }

    BaseIterator(I input)
    {
        this.input = input;
        this.stop = new Stop();
        this.stopChild = this.stop;
    }

    /**
     * run the corresponding runOnClose method for the first length transformations.
     *
     * used in hasMoreContents to close the methods preceding the MoreContents
     */
    protected abstract Throwable runOnClose(int length);

    /**
     * apply the relevant method from the transformation to the value.
     *
     * used in hasMoreContents to apply the functions that follow the MoreContents
     */
    protected abstract V applyOne(V value, Transformation transformation);

    public final void close()
    {
        Throwable fail = runOnClose(length);
        if (next instanceof AutoCloseable)
        {
            try { ((AutoCloseable) next).close(); }
            catch (Throwable t) { fail = merge(fail, t); }
        }
        try { input.close(); }
        catch (Throwable t) { fail = merge(fail, t); }
        maybeFail(fail);
    }

    public final O next()
    {
        if (next == null && !hasNext())
            throw new NoSuchElementException();

        O next = (O) this.next;
        this.next = null;
        return next;
    }

    // may set next != null if the next contents are a transforming iterator that already has data to return,
    // in which case we immediately have more contents to yield
    protected final boolean hasMoreContents()
    {
        return moreContents.length > 0 && tryGetMoreContents();
    }

    @DontInline
    private boolean tryGetMoreContents()
    {
        for (int i = 0 ; i < moreContents.length ; i++)
        {
            MoreContentsHolder holder = moreContents[i];
            MoreContents provider = holder.moreContents;
            I newContents = (I) provider.moreContents();
            if (newContents == null)
                continue;

            input.close();
            input = newContents;
            Stack prefix = EMPTY;
            if (newContents instanceof BaseIterator)
            {
                // we're refilling with transformed contents, so swap in its internals directly
                // TODO: ensure that top-level data is consistent. i.e. staticRow, partitionlevelDeletion etc are same?
                BaseIterator abstr = (BaseIterator) newContents;
                prefix = abstr;
                input = (I) abstr.input;
                stopChild = abstr.stop;
                next = apply((V) abstr.next, holder.length); // must apply all remaining functions to the next, if any
            }

            // since we're truncating our transformation stack to only those occurring after the extend transformation
            // we have to run any prior runOnClose methods
            maybeFail(runOnClose(holder.length));
            refill(prefix, holder, i);

            if (next != null || input.hasNext())
                return true;

            i = -1;
        }
        return false;
    }

    // apply the functions [from..length)
    private V apply(V next, int from)
    {
        while (next != null & from < length)
            next = applyOne(next, stack[from++]);
        return next;
    }
}

