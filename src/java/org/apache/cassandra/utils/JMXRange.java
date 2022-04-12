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

import java.util.function.Function;

import com.google.common.base.Strings;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;

/**
 * {@link Range} can not be safely sent over JMX as the client may not have it in the classpath, so this class offers
 * a common way to convert to/from JMX compatable type (aka String).
 */
public final class JMXRange
{
    public static final String TOKEN_DELIMITER = ":";

    public static <T extends RingPosition<T>> Range<T> parse(String str, Function<String, T> fn)
    {
        String[] splits = str.split(TOKEN_DELIMITER);
        assert splits.length == 2 : String.format("Unable to parse token range %s; needs to have two tokens separated by %s", str, TOKEN_DELIMITER);
        String lhsStr = splits[0];
        assert !Strings.isNullOrEmpty(lhsStr) : String.format("Unable to parse token range %s; left hand side of the token separater is empty", str);
        String rhsStr = splits[1];
        assert !Strings.isNullOrEmpty(rhsStr) : String.format("Unable to parse token range %s; right hand side of the token separater is empty", str);
        T lhs = fn.apply(lhsStr);
        T rhs = fn.apply(rhsStr);
        return new Range<>(lhs, rhs);
    }

    public static String toString(Range<?> range)
    {
        return range.left + TOKEN_DELIMITER + range.right;
    }
}
