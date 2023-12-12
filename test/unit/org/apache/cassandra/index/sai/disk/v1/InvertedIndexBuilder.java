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
package org.apache.cassandra.index.sai.disk.v1;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.stream.Collectors.toList;

public class InvertedIndexBuilder
{
    public static List<Pair<ByteComparable, LongArrayList>> buildStringTermsEnum(int terms, int postings, Supplier<String> termsGenerator, IntSupplier postingsGenerator)
    {
        final List<ByteComparable> sortedTerms = Stream.generate(termsGenerator)
                                                       .distinct()
                                                       .limit(terms)
                                                       .sorted()
                                                       .map(ByteComparable::of)
                                                       .collect(toList());

        final List<Pair<ByteComparable, LongArrayList>> termsEnum = new ArrayList<>();
        for (ByteComparable term : sortedTerms)
        {
            final LongArrayList postingsList = new LongArrayList();

            IntStream.generate(postingsGenerator)
                     .distinct()
                     .limit(postings)
                     .sorted()
                     .forEach(postingsList::add);

            termsEnum.add(Pair.create(term, postingsList));
        }
        return termsEnum;
    }
}
