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

package org.apache.cassandra.harry.util;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.EntropySource;

public class TestRunner
{
    private static final int CYCLES = 100;

    protected static final EntropySource rand = EntropySource.forTests(6371747244598697093L);

    public static <T1, T2> void test(Generator<T1> gen1,
                                     Generator<T2> gen2,
                                     BiConsumer<T1, T2> validate)
    {
        for (int i = 0; i < CYCLES; i++)
        {
            validate.accept(gen1.generate(rand),
                            gen2.generate(rand));
        }
    }

    public static <T1, T2> void test(Generator<T1> gen1,
                                     Function<T1, Generator<T2>> gen2,
                                     ThrowingConsumer<T2> validate) throws Throwable
    {
        test(gen1,
             (v1) -> test(gen2.apply(v1), validate));
    }

    public static <T1> void test(Generator<T1> gen1,
                                 ThrowingConsumer<T1> validate) throws Throwable
    {
        for (int i = 0; i < CYCLES; i++)
        {
            validate.accept(gen1.generate(rand));
        }
    }

    public static <VISIT, MODEL, SUT> void test(Generator<VISIT> visitGenerator,
                                                Supplier<MODEL> initializeModel,
                                                Supplier<SUT> initializeSUT,
                                                BiFunction<MODEL, VISIT, MODEL> applyToModel,
                                                BiFunction<SUT, VISIT, SUT> applyToSut,
                                                ThrowingBiConsumer<MODEL, SUT> afterAll) throws Throwable
    {
        MODEL model = initializeModel.get();
        SUT sut = initializeSUT.get();
        for (int i = 0; i < CYCLES; i++)
        {
            VISIT v = visitGenerator.generate(rand);
            model = applyToModel.apply(model, v);
            sut = applyToSut.apply(sut, v);
        }
        afterAll.accept(model, sut);
    }

    public static <VISIT, SUT> void test(Generator<VISIT> visitGenerator,
                                         Supplier<SUT> initializeSUT,
                                         BiFunction<SUT, VISIT, SUT> applyToSut,
                                         Consumer<SUT> afterAll) throws Throwable
    {
        SUT sut = initializeSUT.get();
        for (int i = 0; i < CYCLES; i++)
        {
            VISIT v = visitGenerator.generate(rand);
            sut = applyToSut.apply(sut, v);
        }
        afterAll.accept(sut);
    }

    public static interface ThrowingConsumer<T>
    {
        void accept(T t) throws Throwable;
    }

    public static interface ThrowingBiConsumer<T1, T2>
    {
        void accept(T1 t1, T2 t2) throws Throwable;
    }
}

