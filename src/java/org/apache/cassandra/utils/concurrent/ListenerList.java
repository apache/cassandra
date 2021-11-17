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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GenericFutureListener;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.concurrent.ExecutionFailure;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ImmediateExecutor;

import static org.apache.cassandra.utils.concurrent.ListenerList.Notifying.NOTIFYING;

/**
 * Encapsulate one or more items in a linked-list that is immutable whilst shared, forming a prepend-only list (or stack).
 * Once the list is ready to consume, exclusive ownership is taken by clearing the shared variable containing it, after
 * which the list may be invoked using {@link #notifyExclusive(ListenerList, Future)}, which reverses the list before invoking the work it contains.
 */
abstract class ListenerList<V> extends IntrusiveStack<ListenerList<V>>
{
    abstract void notifySelf(Executor notifyExecutor, Future<V> future);

    static ListenerList pushHead(ListenerList prev, ListenerList next)
    {
        if (prev instanceof Notifying<?>)
        {
            Notifying result = new Notifying();
            result.next = next;
            next.next = prev == NOTIFYING ? null : prev;
            return result;
        }
        next.next = prev;
        return next;
    }

    /**
     * Logically append {@code newListener} to {@link #listeners}
     * (at this stage it is a stack, so we actually prepend)
     *
     * @param newListener must be either a {@link ListenerList} or {@link GenericFutureListener}
     */
    @Inline
    static <T> void push(AtomicReferenceFieldUpdater<? super T, ListenerList> updater, T in, ListenerList newListener)
    {
        IntrusiveStack.push(updater, in, newListener, ListenerList::pushHead);
    }

    /**
     * Logically append {@code newListener} to {@link #listeners}
     * (at this stage it is a stack, so we actually prepend)
     *
     * @param newListener must be either a {@link ListenerList} or {@link GenericFutureListener}
     */
    @Inline
    static <T> void pushExclusive(AtomicReferenceFieldUpdater<? super T, ListenerList> updater, T in, ListenerList newListener)
    {
        IntrusiveStack.pushExclusive(updater, in, newListener, ListenerList::pushHead);
    }

    static <V, T extends Future<V>> void notify(AtomicReferenceFieldUpdater<? super T, ListenerList> updater, T in)
    {
        while (true)
        {
            ListenerList<V> listeners = updater.get(in);
            if (listeners == null || listeners instanceof Notifying)
                return; // either no listeners, or we are already notifying listeners, so we'll get to the new one when ready

            if (updater.compareAndSet(in, listeners, NOTIFYING))
            {
                while (true)
                {
                    notifyExclusive(listeners, in);
                    if (updater.compareAndSet(in, NOTIFYING, null))
                        return;

                    listeners = updater.getAndSet(in, NOTIFYING);
                }
            }
        }
    }

    /**
     * Requires exclusive ownership of {@code head}.
     *
     * Task submission occurs in the order the operations were submitted; if all of the executors
     * are immediate or unspecified this guarantees execution order.
     * Tasks are submitted to the executor individually, as this simplifies semantics and
     * we anticipate few listeners in practice, and even fewer with indirect executors.
     *
     * @param head must be either a {@link ListenerList} or {@link GenericFutureListener}
     */
    static <T> void notifyExclusive(ListenerList<T> head, Future<T> future)
    {
        Executor notifyExecutor; {
            Executor exec = future.notifyExecutor();
            notifyExecutor = inExecutor(exec) ? null : exec;
        }

        head = reverse(head);
        forEach(head, i -> i.notifySelf(notifyExecutor, future));
    }

    /**
     * Notify {@code listener} on the invoking thread, handling any exceptions
     */
    static <F extends io.netty.util.concurrent.Future<?>> void notifyListener(GenericFutureListener<F> listener, F future)
    {
        try
        {
            listener.operationComplete(future);
        }
        catch (Throwable t)
        {
            // TODO: suboptimal package interdependency - move FutureTask etc here?
            ExecutionFailure.handle(t);
        }
    }

    /**
     * Notify {@code listener} using {@code notifyExecutor} if set, and handling exceptions otherwise
     */
    static <F extends io.netty.util.concurrent.Future<?>> void notifyListener(Executor notifyExecutor, GenericFutureListener<F> listener, F future)
    {
        if (notifyExecutor == null) notifyListener(listener, future);
        else safeExecute(notifyExecutor, () -> notifyListener(listener, future));
    }

    /**
     * Notify {@code listener} using {@code notifyExecutor} if set, and handling exceptions otherwise
     */
    static void notifyListener(@Nullable Executor notifyExecutor, Runnable listener)
    {
        safeExecute(notifyExecutor, listener);
    }

    private static void safeExecute(@Nullable Executor notifyExecutor, Runnable runnable)
    {
        if (notifyExecutor == null)
            notifyExecutor = ImmediateExecutor.INSTANCE;
        try
        {
            notifyExecutor.execute(runnable);
        }
        catch (Exception | Error e)
        {
            // TODO: suboptimal package interdependency - move FutureTask etc here?
            ExecutionFailure.handle(e);
        }
    }

    /**
     * Encapsulate a regular listener in a linked list
     */
    static class GenericFutureListenerList<V> extends ListenerList<V>
    {
        final GenericFutureListener listener;

        GenericFutureListenerList(GenericFutureListener listener)
        {
            this.listener = listener;
        }

        @Override
        void notifySelf(Executor notifyExecutor, Future<V> future)
        {
            notifyListener(notifyExecutor, listener, future);
        }
    }

    /**
     * Encapsulates the invocation of a callback with everything needed to submit for execution
     * without incurring significant further overhead as a list
     */
    static class CallbackListener<V> extends ListenerList<V> implements Runnable
    {
        final Future<V> future;
        final FutureCallback<? super V> callback;

        CallbackListener(Future<V> future, FutureCallback<? super V> callback)
        {
            this.future = future;
            this.callback = callback;
        }

        @Override
        public void run()
        {
            if (future.isSuccess()) callback.onSuccess(future.getNow());
            else callback.onFailure(future.cause());
        }

        @Override
        void notifySelf(Executor notifyExecutor, Future<V> future)
        {
            notifyListener(notifyExecutor, this);
        }
    }

    /**
     * Encapsulates the invocation of a callback with everything needed to submit for execution
     * without incurring significant further overhead as a list
     */
    static class CallbackBiConsumerListener<V> extends ListenerList<V> implements Runnable
    {
        final Future<V> future;
        final BiConsumer<? super V, Throwable> callback;
        final Executor executor;

        CallbackBiConsumerListener(Future<V> future, BiConsumer<? super V, Throwable> callback, Executor executor)
        {
            this.future = future;
            this.callback = callback;
            this.executor = executor;
        }

        @Override
        public void run()
        {
            if (future.isSuccess()) callback.accept(future.getNow(), null);
            else callback.accept(null, future.cause());
        }

        @Override
        void notifySelf(Executor notifyExecutor, Future<V> future)
        {
            notifyListener(executor == null ? notifyExecutor : executor, this);
        }
    }

    /**
     * Encapsulates the invocation of a callback with everything needed to submit for execution
     * without incurring significant further overhead as a list
     */
    static class CallbackListenerWithExecutor<V> extends CallbackListener<V>
    {
        final Executor executor;
        CallbackListenerWithExecutor(Future<V> future, FutureCallback<? super V> callback, Executor executor)
        {
            super(future, callback);
            this.executor = executor;
        }

        @Override
        void notifySelf(Executor notifyExecutor, Future<V> future)
        {
            notifyListener(executor, this);
        }
    }

    /**
     * Encapsulates the invocation of a callback with everything needed to submit for execution
     * without incurring significant further overhead as a list
     */
    static class CallbackLambdaListener<V> extends ListenerList<V> implements Runnable
    {
        final Future<V> future;
        final Consumer<? super V> onSuccess;
        final Consumer<? super Throwable> onFailure;
        final Executor executor;

        CallbackLambdaListener(Future<V> future, Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure, Executor executor)
        {
            this.future = future;
            this.onSuccess = onSuccess;
            this.onFailure = onFailure;
            this.executor = executor;
        }

        @Override
        public void run()
        {
            if (future.isSuccess()) onSuccess.accept(future.getNow());
            else onFailure.accept(future.cause());
        }

        @Override
        void notifySelf(Executor notifyExecutor, Future future)
        {
            notifyListener(executor == null ? notifyExecutor : executor, this);
        }
    }

    /**
     * Encapsulate a task, executable on completion by {@link Future#notifyExecutor}, in a linked list for storing in
     * {@link #listeners}, either as a listener on its own (since we need to encapsulate it anyway), or alongside
     * other listeners in a list
     */
    static class RunnableWithNotifyExecutor<V> extends ListenerList<V>
    {
        final Runnable task;
        RunnableWithNotifyExecutor(Runnable task)
        {
            this.task = task;
        }

        @Override
        void notifySelf(Executor notifyExecutor, Future<V> future)
        {
            notifyListener(notifyExecutor, task);
        }
    }

    /**
     * Encapsulate a task executable on completion in a linked list for storing in {@link #listeners},
     * either as a listener on its own (since we need to encapsulate it anyway), or alongside other listeners
     * in a list
     */
    static class RunnableWithExecutor<V> extends ListenerList<V>
    {
        final Runnable task;
        @Nullable final Executor executor;
        RunnableWithExecutor(Runnable task, @Nullable Executor executor)
        {
            this.task = task;
            this.executor = executor;
        }

        @Override
        void notifySelf(Executor notifyExecutor, Future<V> future)
        {
            notifyListener(inExecutor(executor) ? null : executor, task);
        }
    }

    /**
     * Dummy that indicates listeners are already being notified after the future was completed,
     * so we cannot notify them ourselves whilst maintaining the guaranteed invocation order.
     * The invocation of the list can be left to the thread already notifying listeners.
     */
    static class Notifying<V> extends ListenerList<V>
    {
        static final Notifying NOTIFYING = new Notifying();

        @Override
        void notifySelf(Executor notifyExecutor, Future<V> future)
        {
        }
    }

    /**
     * @return true iff the invoking thread is executing {@code executor}
     */
    static boolean inExecutor(Executor executor)
    {
        return (executor instanceof EventExecutor && ((EventExecutor) executor).inEventLoop())
               || (executor instanceof ExecutorPlus && ((ExecutorPlus) executor).inExecutor());
    }
}

