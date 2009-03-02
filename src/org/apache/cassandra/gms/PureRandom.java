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

package org.apache.cassandra.gms;

import java.util.Random;

import org.apache.cassandra.utils.BitSet;



/**
 * Implementation of a PureRandomNumber generator. Use this class cautiously. Not
 * for general purpose use. Currently this is used by the Gossiper to choose a random
 * endpoint to Gossip to.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class PureRandom extends Random
{
    private BitSet bs_ = new BitSet();
    private int lastUb_;

    PureRandom()
    {
        super();
    }

    public int nextInt(int ub)
    {
    	if (ub <= 0)
    		throw new IllegalArgumentException("ub must be positive");

        if ( lastUb_ !=  ub )
        {
            bs_.clear();
            lastUb_ = ub;
        }
        else if(bs_.cardinality() == ub)
        {
        	bs_.clear();
        }

        int value = super.nextInt(ub);
        while ( bs_.get(value) )
        {
            value = super.nextInt(ub);
        }
        bs_.set(value);
        return value;
    }

    public static void main(String[] args) throws Throwable
    {
    	Random pr = new PureRandom();
        int ubs[] = new int[] { 2, 3, 1, 10, 5, 0};

        for (int ub : ubs)
        {
            System.out.println("UB: " + String.valueOf(ub));
            for (int j = 0; j < 10; j++)
            {
                int junk = pr.nextInt(ub);
                // Do something with junk so JVM doesn't optimize away
                System.out.println(junk);
            }
        }
    }
}
