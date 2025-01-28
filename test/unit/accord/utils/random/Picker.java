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

package accord.utils.random;

import java.util.Arrays;
import java.util.function.Supplier;

import accord.utils.Invariants;
import accord.utils.RandomSource;

public class Picker
{
    public static float[] randomWeights(RandomSource random, int length)
    {
        float[] weights = new float[length - 1];
        float sum = 0;
        for (int i = 0 ; i < weights.length ; ++i)
            weights[i] = sum += random.nextFloat();
        sum += random.nextFloat();
        for (int i = 0 ; i < weights.length ; ++i)
            weights[i] /= sum;
        return weights;
    }

    static abstract class Weighted
    {
        final RandomSource random;
        final float[] weights;

        public Weighted(RandomSource random, float[] weights)
        {
            this.random = random;
            this.weights = weights;
        }


        static float[] randomWeights(RandomSource random, float[] bias)
        {
            float[] weights = new float[bias.length - 1];
            float sum = 0;
            for (int i = 0 ; i < weights.length ; ++i)
                weights[i] = sum += random.nextFloat() * bias[i];
            sum += random.nextFloat() * bias[weights.length];
            for (int i = 0 ; i < weights.length ; ++i)
                weights[i] /= sum;
            return weights;
        }

        static float[] normaliseWeights(float[] input)
        {
            float[] output = new float[input.length - 1];
            float sum = 0;
            for (int i = 0 ; i < output.length ; ++i)
                output[i] = sum += input[i];
            sum += input[output.length];
            for (int i = 0 ; i < output.length ; ++i)
                output[i] /= sum;
            return output;
        }

        int pickIndex()
        {
            int i = Arrays.binarySearch(weights, random.nextFloat());
            if (i < 0) i = -1 - i;
            return i;
        }
    }

    public static class WeightedObjectPicker<T> extends Weighted implements Supplier<T>
    {
        final T[] values;

        private WeightedObjectPicker(RandomSource random, T[] values, float[] weights)
        {
            super(random, weights);
            this.values = values;
        }

        @Override
        public T get()
        {
            return values[pickIndex()];
        }

        public static <T> WeightedObjectPicker<T> randomWeighted(RandomSource random, T[] values)
        {
            return new WeightedObjectPicker<>(random, values, Picker.randomWeights(random, values.length));
        }

        public static <T> WeightedObjectPicker<T> randomWeighted(RandomSource random, T[] values, float[] bias)
        {
            Invariants.checkArgument(values.length == bias.length);
            return new WeightedObjectPicker<>(random, values, randomWeights(random, bias));
        }

        public static <T> WeightedObjectPicker<T> weighted(RandomSource random, T[] values, float[] proportionalWeights)
        {
            Invariants.checkArgument(values.length == proportionalWeights.length);
            return new WeightedObjectPicker<>(random, values, normaliseWeights(proportionalWeights));
        }
    }
}
