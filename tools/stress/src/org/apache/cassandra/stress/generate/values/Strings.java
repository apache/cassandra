/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.stress.generate.values;

import java.util.Random;

import org.apache.cassandra.db.marshal.UTF8Type;

public class Strings extends Generator<String>
{
    private final char[] chars;
    private final Random rnd = new Random();

    public Strings(String name, GeneratorConfig config)
    {
        super(UTF8Type.instance, config, name);
        chars = new char[(int) sizeDistribution.maxValue()];
    }

    @Override
    public String generate()
    {
        long seed = identityDistribution.next();
        sizeDistribution.setSeed(seed);
        rnd.setSeed(~seed);
        int size = (int) sizeDistribution.next();
        for (int i = 0 ; i < size ; i++)
            chars[i] = (char) (32 +rnd.nextInt(128-32));
        return new String(chars, 0, size);
    }
}
