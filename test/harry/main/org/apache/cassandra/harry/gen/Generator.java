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

package org.apache.cassandra.harry.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.harry.gen.rng.PcgRSUFast;

public interface Generator<T>
{
    // It might be better if every generator has its own independent rng (or even implementation-dependent rng).
    // This way we can have simpler interface: generate a value from the long and invertible iterator does the opposite.
    // More things can be invertible this way. For example, entire primary keys can be invertible.
    T generate(EntropySource rng);

    default List<T> generate(EntropySource rng, int n)
    {
        List<T> res = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
            res.add(generate(rng));
        return res;
    }

    default Surjections.Surjection<T> toSurjection(long streamId)
    {
        return (current) -> {
            EntropySource rng = new PcgRSUFast(current, streamId);
            return generate(rng);
        };
    }

    // TODO: this is only applicable to surjections, it seems
    public static class Value<T>
    {
        public final long descriptor;
        public final T value;

        public Value(long descriptor, T value)
        {
            this.descriptor = descriptor;
            this.value = value;
        }

        public String toString()
        {
            return "Value{" +
                   "descriptor=" + descriptor +
                   ", value=" + (value.getClass().isArray() ? Arrays.toString((Object[]) value) : value) + '}';
        }
    }

    default Supplier<T> bind(EntropySource rand)
    {
        return () -> generate(rand);
    }

    default <T1> Generator<T1> map(Function<T, T1> map)
    {
        return (rand) -> map.apply(generate(rand));
    }

    default <T1> Generator<T1> flatMap(Function<T, Generator<T1>> fmap)
    {
        return (rand) -> fmap.apply(generate(rand)).generate(rand);
    }

    default <T2, T3> Generator<T3> zip(Generator<T2> g1, BiFunction<T, T2, T3> map)
    {
        return (rand) -> map.apply(Generator.this.generate(rand), g1.generate(rand));
    }

    default <T2, T3, T4> Generator<T4> zip(Generator<T2> g1, Generator<T3> g2, TriFunction<T, T2, T3, T4> map)
    {
        return (rand) -> map.apply(Generator.this.generate(rand), g1.generate(rand), g2.generate(rand));
    }

    default <T2, T3, T4, T5> Generator<T5> zip(Generator<T2> g1, Generator<T3> g2, Generator<T4> g3, QuatFunction<T, T2, T3, T4, T5> map)
    {
        return (rand) -> map.apply(Generator.this.generate(rand), g1.generate(rand), g2.generate(rand), g3.generate(rand));
    }

    default <T2, T3, T4, T5, T6> Generator<T6> zip(Generator<T2> g1, Generator<T3> g2, Generator<T4> g3, Generator<T5> g4, QuinFunction<T, T2, T3, T4, T5, T6> map)
    {
        return (rand) -> map.apply(Generator.this.generate(rand), g1.generate(rand), g2.generate(rand), g3.generate(rand), g4.generate(rand));
    }

    default <T2, T3, T4, T5, T6, T7> Generator<T7> zip(Generator<T2> g1, Generator<T3> g2, Generator<T4> g3, Generator<T5> g4, Generator<T6> g5, SechFunction<T, T2, T3, T4, T5, T6, T7> map)
    {
        return (rand) -> map.apply(Generator.this.generate(rand), g1.generate(rand), g2.generate(rand), g3.generate(rand), g4.generate(rand), g5.generate(rand));
    }


    public interface TriFunction<T1, T2, T3, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3);
    }

    public interface QuatFunction<T1, T2, T3, T4, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3, T4 t4);
    }

    public interface QuinFunction<T1, T2, T3, T4, T5, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5);
    }

    public interface SechFunction<T1, T2, T3, T4, T5, T6, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6);
    }
}