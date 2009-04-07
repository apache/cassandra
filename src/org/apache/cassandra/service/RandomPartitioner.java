/**
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

package org.apache.cassandra.service;

import java.math.BigInteger;
import java.util.Comparator;

import org.apache.cassandra.utils.FBUtilities;

/**
 * This class generates a MD5 hash of the key. It uses the standard technique
 * used in all DHT's.
 * 
 * @author alakshman
 * 
 */
public class RandomPartitioner implements IPartitioner
{
    private static final Comparator<String> comparator = new Comparator<String>()
    {
        public int compare(String o1, String o2)
        {
            String[] split1 = o1.split(":", 2);
            String[] split2 = o2.split(":", 2);
            BigInteger i1 = new BigInteger(split1[0]);
            BigInteger i2 = new BigInteger(split2[0]);
            int v = i1.compareTo(i2);
            if (v != 0) {
                return v;
            }
            return split1[1].compareTo(split2[1]);
        }
    };
    private static final Comparator<String> rcomparator = new Comparator<String>()
    {
        public int compare(String o1, String o2)
        {
            return -comparator.compare(o1, o2);
        }
    };

    public BigInteger hash(String key)
	{
		return FBUtilities.hash(key);
	}

    public String decorateKey(String key)
    {
        return hash(key).toString() + ":" + key;
    }

    public String undecorateKey(String decoratedKey)
    {
        return decoratedKey.split(":", 2)[1];
    }

    public Comparator<String> getDecoratedKeyComparator()
    {
        return comparator;
    }

    public Comparator<String> getReverseDecoratedKeyComparator()
    {
        return rcomparator;
    }
}