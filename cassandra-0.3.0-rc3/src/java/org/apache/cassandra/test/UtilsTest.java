/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FastHashMap;
import org.apache.cassandra.utils.GuidGenerator;

public class UtilsTest 
{
	private static void doHashPerf() throws Throwable
	{
		List<BigInteger> list = new ArrayList<BigInteger>();
		for ( int i = 0; i < 100; ++i )
		{
			String guid = GuidGenerator.guid();
			list.add( FBUtilities.hash(guid) );
		}
		Collections.sort(list);
		
		int startValue = 1000000;
		
		while ( true )
		{
			long start = System.currentTimeMillis();
			for ( int i = 0; i < 1024; ++i )
			{
				String key = Integer.toString(startValue + i);
				BigInteger hash = FBUtilities.hash(key);
				Collections.binarySearch(list, hash);
			}
			System.out.println("TIME TAKEN: " + (System.currentTimeMillis() - start));
			Thread.sleep(100);
		}
	}
	
	public static void main(String[] args) throws Throwable
	{
	}
}
