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

package org.apache.cassandra.cql3.ast;

import java.util.stream.Stream;

public interface Element
{
    void toCQL(StringBuilder sb, CQLFormatter formatter);

    default void toCQL(StringBuilder sb)
    {
        toCQL(sb, CQLFormatter.None.instance);
    }

    default String toCQL()
    {
        StringBuilder sb = new StringBuilder();
        toCQL(sb, CQLFormatter.None.instance);
        return sb.toString();
    }

    default String toCQL(CQLFormatter formatter)
    {
        StringBuilder sb = new StringBuilder();
        toCQL(sb, formatter);
        return sb.toString();
    }

    default Stream<? extends Element> stream()
    {
        return Stream.empty();
    }

    default Stream<? extends Element> streamRecursive()
    {
        return streamRecursive(false);
    }

    default Stream<? extends Element> streamRecursive(boolean includeSelf)
    {
        Stream<Element> stream = stream().flatMap(e -> Stream.concat(Stream.of(e), e.streamRecursive()));
        if (includeSelf)
            stream = Stream.concat(Stream.of(this), stream);
        return stream;
    }
}
