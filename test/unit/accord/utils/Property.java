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

package accord.utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class Property
{
    public static abstract class Common<T extends Common<T>>
    {
        protected long seed = ThreadLocalRandom.current().nextLong();
        protected int examples = 1000;

        protected boolean pure = true;
        @Nullable
        protected Duration timeout = null;

        protected Common() {
        }

        protected Common(Common<?> other) {
            this.seed = other.seed;
            this.examples = other.examples;
            this.pure = other.pure;
            this.timeout = other.timeout;
        }

        public T withSeed(long seed)
        {
            this.seed = seed;
            return (T) this;
        }

        public T withExamples(int examples)
        {
            if (examples <= 0)
                throw new IllegalArgumentException("Examples must be positive");
            this.examples = examples;
            return (T) this;
        }

        public T withPure(boolean pure)
        {
            this.pure = pure;
            return (T) this;
        }

        public T withTimeout(Duration timeout)
        {
            this.timeout = timeout;
            return (T) this;
        }

        protected void checkWithTimeout(Runnable fn)
        {
            AsyncPromise<?> promise = new AsyncPromise<>();
            Thread t = new Thread(() -> {
                try
                {
                    fn.run();
                    promise.setSuccess(null);
                }
                catch (Throwable e)
                {
                    promise.setFailure(e);
                }
            });
            t.setName("property with timeout");
            t.setDaemon(true);
            try
            {
                t.start();
                promise.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
            catch (ExecutionException e)
            {
                throw new ProtocolException(propertyError(this, e.getCause()));
            }
            catch (InterruptedException e)
            {
                t.interrupt();
                throw new PropertyError(propertyError(this, e));
            }
            catch (TimeoutException e)
            {
                t.interrupt();
                TimeoutException override = new TimeoutException("property test did not complete within " + this.timeout);
                override.setStackTrace(new StackTraceElement[0]);
                throw new PropertyError(propertyError(this, override));
            }
        }
    }

    public static class ForBuilder extends Common<ForBuilder>
    {
        public void check(FailingConsumer<RandomSource> fn)
        {
            forAll(Gens.random()).check(fn);
        }

        public <T> SingleBuilder<T> forAll(Gen<T> gen)
        {
            return new SingleBuilder<>(gen, this);
        }

        public <A, B> DoubleBuilder<A, B> forAll(Gen<A> a, Gen<B> b)
        {
            return new DoubleBuilder<>(a, b, this);
        }

        public <A, B, C> TrippleBuilder<A, B, C> forAll(Gen<A> a, Gen<B> b, Gen<C> c)
        {
            return new TrippleBuilder<>(a, b, c, this);
        }
    }

    private static Object normalizeValue(Object value)
    {
        if (value == null)
            return null;
        // one day java arrays will have a useful toString... one day...
        if (value.getClass().isArray())
        {
            Class<?> subType = value.getClass().getComponentType();
            if (!subType.isPrimitive())
                return Arrays.asList((Object[]) value);
            if (Byte.TYPE == subType)
                return Arrays.toString((byte[]) value);
            if (Character.TYPE == subType)
                return Arrays.toString((char[]) value);
            if (Short.TYPE == subType)
                return Arrays.toString((short[]) value);
            if (Integer.TYPE == subType)
                return Arrays.toString((int[]) value);
            if (Long.TYPE == subType)
                return Arrays.toString((long[]) value);
            if (Float.TYPE == subType)
                return Arrays.toString((float[]) value);
            if (Double.TYPE == subType)
                return Arrays.toString((double[]) value);
        }
        try
        {
            return value.toString();
        }
        catch (Throwable t)
        {
            return "Object.toString failed: " + t.getClass().getCanonicalName() + ": " + t.getMessage();
        }
    }

    private static String propertyError(Common<?> input, Throwable cause, Object... values)
    {
        StringBuilder sb = new StringBuilder();
        // return "Seed=" + seed + "\nExamples=" + examples;
        sb.append("Property error detected:\nSeed = ").append(input.seed).append('\n');
        sb.append("Examples = ").append(input.examples).append('\n');
        sb.append("Pure = ").append(input.pure).append('\n');
        if (cause != null)
        {
            String msg = cause.getMessage();
            sb.append("Error: ");
            // to improve readability, if a newline is detected move the error msg to the next line
            if (msg != null && msg.contains("\n"))
                msg = "\n\t" + msg.replace("\n", "\n\t");
            if (msg == null)
                msg = cause.getClass().getCanonicalName();
            sb.append(msg).append('\n');
        }
        if (values != null)
        {
            sb.append("Values:\n");
            for (int i = 0; i < values.length; i++)
                sb.append('\t').append(i).append(" = ").append(normalizeValue(values[i])).append('\n');
        }
        return sb.toString();
    }

    public interface FailingConsumer<A>
    {
        void accept(A value) throws Exception;
    }

    public static class SingleBuilder<T> extends Common<SingleBuilder<T>>
    {
        private final Gen<T> gen;

        private SingleBuilder(Gen<T> gen, Common<?> other) {
            super(other);
            this.gen = Objects.requireNonNull(gen);
        }

        public void check(FailingConsumer<T> fn)
        {
            if (timeout != null)
            {
                checkWithTimeout(() -> checkInternal(fn));
                return;
            }
            checkInternal(fn);
        }

        private void checkInternal(FailingConsumer<T> fn)
        {
            RandomSource random = new DefaultRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                T value = null;
                try
                {
                    checkInterrupted();
                    fn.accept(value = gen.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, value), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    public interface FailingBiConsumer<A, B>
    {
        void accept(A a, B b) throws Exception;
    }

    public static class DoubleBuilder<A, B> extends Common<DoubleBuilder<A, B>>
    {
        private final Gen<A> aGen;
        private final Gen<B> bGen;

        private DoubleBuilder(Gen<A> aGen, Gen<B> bGen, Common<?> other) {
            super(other);
            this.aGen = Objects.requireNonNull(aGen);
            this.bGen = Objects.requireNonNull(bGen);
        }

        public void check(FailingBiConsumer<A, B> fn)
        {
            if (timeout != null)
            {
                checkWithTimeout(() -> checkInternal(fn));
                return;
            }
            checkInternal(fn);
        }

        private void checkInternal(FailingBiConsumer<A, B> fn)
        {
            RandomSource random = new DefaultRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                A a = null;
                B b = null;
                try
                {
                    checkInterrupted();
                    fn.accept(a = aGen.next(random), b = bGen.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, a, b), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    public interface FailingTriConsumer<A, B, C>
    {
        void accept(A a, B b, C c) throws Exception;
    }

    public static class TrippleBuilder<A, B, C> extends Common<TrippleBuilder<A, B, C>>
    {
        private final Gen<A> as;
        private final Gen<B> bs;
        private final Gen<C> cs;

        public TrippleBuilder(Gen<A> as, Gen<B> bs, Gen<C> cs, Common<?> other)
        {
            super(other);
            this.as = as;
            this.bs = bs;
            this.cs = cs;
        }

        public void check(FailingTriConsumer<A, B, C> fn)
        {
            if (timeout != null)
            {
                checkWithTimeout(() -> checkInternal(fn));
                return;
            }
            checkInternal(fn);
        }

        private void checkInternal(FailingTriConsumer<A, B, C> fn)
        {
            RandomSource random = new DefaultRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                A a = null;
                B b = null;
                C c = null;
                try
                {
                    checkInterrupted();
                    fn.accept(a = as.next(random), b = bs.next(random), c = cs.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, a, b, c), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    private static void checkInterrupted() throws InterruptedException
    {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();
    }

    public static class PropertyError extends AssertionError
    {
        public PropertyError(String message, Throwable cause)
        {
            super(message, cause);
        }

        public PropertyError(String message)
        {
            super(message);
        }
    }

    public static ForBuilder qt()
    {
        return new ForBuilder();
    }
}
