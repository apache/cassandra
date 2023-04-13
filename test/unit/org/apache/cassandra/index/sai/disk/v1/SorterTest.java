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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.Sorter;

import static org.junit.Assert.assertArrayEquals;

public class SorterTest
{
    @Test
    public void test()
    {
        final int[] array = new int[100];
        for (int x=0; x < array.length; x++)
        {
            array[x] = x;
        }

        int[] sortedArray = Arrays.copyOf(array, array.length);

        SAIRandomizedTester.shuffle(array);

        final Sorter sorter = new IntroSorter() {
            int pivotDoc;

            @Override
            protected void swap(int i, int j) {
                int o = array[i];
                array[i] = array[j];
                array[j] = o;
            }

            @Override
            protected void setPivot(int i)
            {
                pivotDoc = array[i];
            }

            @Override
            protected int comparePivot(int j) {
                return pivotDoc - array[j];
            }
        };

        sorter.sort(0, array.length);

        assertArrayEquals(sortedArray, array);
    }
}
