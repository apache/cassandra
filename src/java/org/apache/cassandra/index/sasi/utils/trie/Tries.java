/*
 * Copyright 2005-2010 Roger Kapsi
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/**
 * This class is taken from https://github.com/rkapsi/patricia-trie (v0.6), and slightly modified
 * to correspond to Cassandra code style, as the only Patricia Trie implementation,
 * which supports pluggable key comparators (e.g. commons-collections PatriciaTrie (which is based
 * on rkapsi/patricia-trie project) only supports String keys)
 * but unfortunately is not deployed to the maven central as a downloadable artifact.
 */

package org.apache.cassandra.index.sasi.utils.trie;

/**
 * A collection of {@link Trie} utilities
 */
public class Tries
{
    /** 
     * Returns true if bitIndex is a {@link KeyAnalyzer#OUT_OF_BOUNDS_BIT_KEY}
     */
    static boolean isOutOfBoundsIndex(int bitIndex)
    {
        return bitIndex == KeyAnalyzer.OUT_OF_BOUNDS_BIT_KEY;
    }

    /** 
     * Returns true if bitIndex is a {@link KeyAnalyzer#EQUAL_BIT_KEY}
     */
    static boolean isEqualBitKey(int bitIndex)
    {
        return bitIndex == KeyAnalyzer.EQUAL_BIT_KEY;
    }

    /** 
     * Returns true if bitIndex is a {@link KeyAnalyzer#NULL_BIT_KEY} 
     */
    static boolean isNullBitKey(int bitIndex)
    {
        return bitIndex == KeyAnalyzer.NULL_BIT_KEY;
    }

    /** 
     * Returns true if the given bitIndex is valid. Indices 
     * are considered valid if they're between 0 and 
     * {@link Integer#MAX_VALUE}
     */
    static boolean isValidBitIndex(int bitIndex)
    {
        return 0 <= bitIndex;
    }

    /**
     * Returns true if both values are either null or equal
     */
    static boolean areEqual(Object a, Object b)
    {
        return (a == null ? b == null : a.equals(b));
    }

    /**
     * Throws a {@link NullPointerException} with the given message if 
     * the argument is null.
     */
    static <T> T notNull(T o, String message)
    {
        if (o == null)
            throw new NullPointerException(message);

        return o;
    }

    /**
     * A utility method to cast keys. It actually doesn't
     * cast anything. It's just fooling the compiler!
     */
    @SuppressWarnings("unchecked")
    static <K> K cast(Object key)
    {
        return (K)key;
    }
}
