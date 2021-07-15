/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ExpMovingAverageTest
{
    private static final double epsilon = 0.0001;

    @Test
    public void testUpdates()
    {
        ExpMovingAverage average = new ExpMovingAverage(0.5);
        assertNotNull(average.toString());

        average.update(10);

        assertEquals(10, average.get(), epsilon);

        average.update(11);
        assertEquals(10.5, average.get(), epsilon);

        average.update(12);
        assertEquals(11.25, average.get(), epsilon);

        average.update(11.75);

        assertEquals(11.5, average.get(), epsilon);
    }

    @Test
    public void testDecay10()
    {
        testDecay(10, 0.01, ExpMovingAverage.decayBy10());
    }

    @Test
    public void testDecay100()
    {
        testDecay(100, 0.01, ExpMovingAverage.decayBy100());
    }

    @Test
    public void testDecay1000()
    {
        testDecay(1000, 0.01, ExpMovingAverage.decayBy1000());
    }

    @Test
    public void testDecay()
    {
        double ratio = 0.1;
        int count = 50;
        testDecay(count, ratio, ExpMovingAverage.withDecay(ratio, count));
    }

    public void testDecay(int count, double expectedBelow, MovingAverage average)
    {
        average.update(1.0);    // on initialization average takes the exact value
        for (int i = 0; i < count; ++i)
            average.update(0.0);

        assertTrue(average.get() <= expectedBelow + epsilon);
        assertTrue(average.get() >= expectedBelow / 2 - epsilon);
    }
}