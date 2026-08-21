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
package org.apache.cassandra.db.compaction;

import java.util.Comparator;

/**
 * Use bubble sort to insert the <code>sortedFrom - 1</code> element into a pre-sorted array, and
 * track element equality to next element to help in finding merge ranges.
 * <p>
 * The comparator is held as an instance field rather than passed per call so that this class can
 * be duplicated under a new name via the {@code gen-java-copy} build step (see {@code build.xml}):
 * each generated copy is a separately compiled class whose {@code comparator.compare(...)} call
 * site is only ever exercised with the one comparator it was constructed with, keeping that call
 * site monomorphic/inlinable instead of megamorphic across every comparator sharing this class.
 */
public class PreSortedBubbleInsert<T>
{
    private final Comparator<T> comparator;

    public PreSortedBubbleInsert(Comparator<T> comparator)
    {
        this.comparator = comparator;
    }

    public void sortPerturbed(T[] preSortedArray, boolean[] equalsNext, int perturbedLimit, int sortedTo)
    {
        for (; perturbedLimit > 0; perturbedLimit--)
            bubbleInsertToPreSorted(preSortedArray, equalsNext, perturbedLimit, sortedTo);
    }

    /**
     * @param preSortedArray partially pre-sorted array of elements to be sorted in place
     * @param equalsNext tracking the equality between each element and the next in the sorted array
     * @param sortedFrom elements from <code>sortedFrom</code> are assumed sorted
     * @param sortedTo the limit of our sort effort
     */
    private void bubbleInsertToPreSorted(T[] preSortedArray, boolean[] equalsNext, int sortedFrom, int sortedTo)
    {
        int insertInto = sortedFrom - 1;
        T newElement = preSortedArray[insertInto];
        for (; insertInto < sortedTo - 1; insertInto++)
        {
            int cmp = comparator.compare(newElement, preSortedArray[insertInto + 1]);
            if (cmp < 0)
            {
                equalsNext[insertInto] = false;
                break;
            }
            else if (cmp == 0) {
                equalsNext[insertInto] = true;
                break;
            }
            else
            {
                // skip the section of equal elements who are smaller than the new element
                for (; insertInto < sortedTo - 1; insertInto++)
                {
                    if (!equalsNext[insertInto + 1])
                    {
                        break;
                    }
                    preSortedArray[insertInto] = preSortedArray[insertInto + 1];
                    equalsNext[insertInto] = true;
                }
                preSortedArray[insertInto] = preSortedArray[insertInto + 1];
                equalsNext[insertInto] = false;
            }
        }
        preSortedArray[insertInto] = newElement;
    }
}
