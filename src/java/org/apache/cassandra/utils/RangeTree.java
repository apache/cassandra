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

package org.apache.cassandra.utils;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface RangeTree<Token, Range, Value> extends Iterable<Map.Entry<Range, Value>>
{
    void searchToken(Token token, Consumer<Map.Entry<Range, Value>> onMatch);

    boolean add(Range key, Value value);

    List<Value> get(Range range);

    void get(Range range, Consumer<Map.Entry<Range, Value>> onMatch);

    List<Map.Entry<Range, Value>> search(Range range);

    void search(Range range, Consumer<Map.Entry<Range, Value>> onMatch);

    List<Map.Entry<Range, Value>> searchToken(Token token);

    int remove(Range key);

    void clear();

    int size();

    boolean isEmpty();

    default Stream<Map.Entry<Range, Value>> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }

    interface Accessor<Token, Range>
    {
        Token start(Range range);
        Token end(Range range);
        boolean contains(Token start, Token end, Token token);
        default boolean contains(Range range, Token token)
        {
            return contains(start(range), end(range), token);
        }
        boolean intersects(Range range, Token start, Token end);
        default boolean intersects(Range left, Range right)
        {
            return intersects(left, start(right), end(right));
        }
    }
}
