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

public class OrderPreservingHashPartitioner implements IPartitioner
{
    private final static int maxKeyHashLength_ = 36;
    private final static BigInteger ONE = BigInteger.ONE;
    /* May be even 255L will work. But I need to verify that. */
    private static final BigInteger prime_ = BigInteger.valueOf(Character.MAX_VALUE);
    
    public BigInteger hash(String key)
    {
        BigInteger h = BigInteger.ZERO;
        char val[] = key.toCharArray();
       
        for (int i = 0; i < maxKeyHashLength_; i++)
        {
            if( i < val.length )
                h = prime_.multiply(h).add( BigInteger.valueOf(val[i]) );
            else
                h = prime_.multiply(h).add( ONE );
        }
        return h;
    }
}
