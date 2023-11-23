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

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public interface Gen<A> {
    /**
     * For cases where method handles isn't able to detect the proper type, this method acts as a cast
     * to inform the compiler of the desired type.
     */
    static <A> Gen<A> of(Gen<A> fn)
    {
        return fn;
    }

    A next(RandomSource random);

    default <B> Gen<B> map(Function<? super A, ? extends B> fn)
    {
        return r -> fn.apply(this.next(r));
    }

    default <B> Gen<B> map(BiFunction<RandomSource, ? super A, ? extends B> fn)
    {
        return r -> fn.apply(r, this.next(r));
    }

    default IntGen mapToInt(ToIntFunction<A> fn)
    {
        return r -> fn.applyAsInt(next(r));
    }

    default LongGen mapToLong(ToLongFunction<A> fn)
    {
        return r -> fn.applyAsLong(next(r));
    }

    default <B> Gen<B> flatMap(Function<? super A, Gen<? extends B>> mapper)
    {
        return rs -> mapper.apply(this.next(rs)).next(rs);
    }

    default <B> Gen<B> flatMap(BiFunction<RandomSource, ? super A, Gen<? extends B>> mapper)
    {
        return rs -> mapper.apply(rs, this.next(rs)).next(rs);
    }

    default Gen<A> filter(Predicate<A> fn)
    {
        Gen<A> self = this;
        return r -> {
            A value;
            do {
                value = self.next(r);
            }
            while (!fn.test(value));
            return value;
        };
    }

    default Supplier<A> asSupplier(RandomSource rs)
    {
        return () -> next(rs);
    }

    default Stream<A> asStream(RandomSource rs)
    {
        return Stream.generate(() -> next(rs));
    }

    interface IntGen extends Gen<Integer>
    {
        int nextInt(RandomSource random);

        @Override
        default Integer next(RandomSource random)
        {
            return nextInt(random);
        }

        default Gen.IntGen filterInt(IntPredicate fn)
        {
            return rs -> {
                int value;
                do
                {
                    value = nextInt(rs);
                }
                while (!fn.test(value));
                return value;
            };
        }

        @Override
        default Gen.IntGen filter(Predicate<Integer> fn)
        {
            return filterInt(i -> fn.test(i));
        }

        default IntSupplier asIntSupplier(RandomSource rs)
        {
            return () -> nextInt(rs);
        }

        default IntStream asIntStream(RandomSource rs)
        {
            return IntStream.generate(() -> nextInt(rs));
        }
    }

    interface LongGen extends Gen<Long>
    {
        long nextLong(RandomSource random);

        @Override
        default Long next(RandomSource random)
        {
            return nextLong(random);
        }

        default Gen.LongGen filterLong(LongPredicate fn)
        {
            return rs -> {
                long value;
                do
                {
                    value = nextLong(rs);
                }
                while (!fn.test(value));
                return value;
            };
        }

        @Override
        default Gen.LongGen filter(Predicate<Long> fn)
        {
            return filterLong(i -> fn.test(i));
        }

        default LongSupplier asLongSupplier(RandomSource rs)
        {
            return () -> nextLong(rs);
        }

        default LongStream asLongStream(RandomSource rs)
        {
            return LongStream.generate(() -> nextLong(rs));
        }
    }
}
