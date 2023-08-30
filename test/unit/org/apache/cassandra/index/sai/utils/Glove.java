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

package org.apache.cassandra.index.sai.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple wrapper of a <a href="https://nlp.stanford.edu/projects/glove/">glove</a> model that loads a set of
 * word -> embedding mappings into a {@link WordVector} object. The embeddings are represented a fixed dimension
 * float[].
 * <p>
 * The glove model provides a consistently valid embedding for each word that can be used for testing graph
 * implementations that use embeddings.
 */
public class Glove
{
    public static WordVector parse(InputStream inputStream) throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8), 1024 * 1024);

        Map<String, Vector> wordVectorMap = new HashMap<>();
        int dimension = -1;

        while (reader.ready())
        {
            String line = reader.readLine();

            String parts[] = line.split(" ");

            String word = parts[0];

            float[] vectorArray = new float[parts.length - 1];

            for (int index = 1; index < parts.length; index++)
                vectorArray[index - 1] = Float.parseFloat(parts[index]);

            Vector vector = new Vector(vectorArray);
            if (wordVectorMap.isEmpty())
                dimension = vector.dimension();
            else if (vector.dimension() != dimension)
                throw new IOException("Vectors must all be of the same dimension");
            wordVectorMap.put(word, vector);
        }
        return new WordVector(wordVectorMap, dimension);
    }

    public static class WordVector
    {
        private final Map<String, Vector> wordVectorMap;
        private final int dimension;
        private final List<String> words;

        public WordVector(Map<String, Vector> wordVectorMap, int dimension)
        {
            this.wordVectorMap = wordVectorMap;
            this.dimension = dimension;
            this.words = new ArrayList<>(wordVectorMap.size());
            this.words.addAll(wordVectorMap.keySet());
        }

        public int size()
        {
            return wordVectorMap.size();
        }

        public int dimension()
        {
            return dimension;
        }

        public String word(int index)
        {
            return words.get(index);
        }

        public float[] vector(String word)
        {
            return wordVectorMap.get(word).vector;
        }
    }

    public static class Vector
    {
        private final float[] vector;

        public Vector(float[] vector)
        {
            this.vector = vector;
        }

        public int dimension()
        {
            return vector.length;
        }
    }
}
