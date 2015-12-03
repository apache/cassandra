/*
 * Copyright 2010 Roger Kapsi
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

package org.apache.cassandra.index.sasi.utils.trie;

import java.util.Comparator;

/**
 * This class is taken from https://github.com/rkapsi/patricia-trie (v0.6), and slightly modified
 * to correspond to Cassandra code style, as the only Patricia Trie implementation,
 * which supports pluggable key comparators (e.g. commons-collections PatriciaTrie (which is based
 * on rkapsi/patricia-trie project) only supports String keys)
 * but unfortunately is not deployed to the maven central as a downloadable artifact.
 */

/**
 * The {@link KeyAnalyzer} provides bit-level access to keys
 * for the {@link PatriciaTrie}.
 */
public interface KeyAnalyzer<K> extends Comparator<K>
{
    /**
     * Returned by {@link #bitIndex(Object, Object)} if a key's
     * bits were all zero (0).
     */
    int NULL_BIT_KEY = -1;
    
    /** 
     * Returned by {@link #bitIndex(Object, Object)} if a the
     * bits of two keys were all equal.
     */
    int EQUAL_BIT_KEY = -2;
    
    /**
     * Returned by {@link #bitIndex(Object, Object)} if a keys 
     * indices are out of bounds.
     */
    int OUT_OF_BOUNDS_BIT_KEY = -3;
    
    /**
     * Returns the key's length in bits.
     */
    int lengthInBits(K key);
    
    /**
     * Returns {@code true} if a key's bit it set at the given index.
     */
    boolean isBitSet(K key, int bitIndex);
    
    /**
     * Returns the index of the first bit that is different in the two keys.
     */
    int bitIndex(K key, K otherKey);
    
    /**
     * Returns {@code true} if the second argument is a 
     * prefix of the first argument.
     */
    boolean isPrefix(K key, K prefix);
}
