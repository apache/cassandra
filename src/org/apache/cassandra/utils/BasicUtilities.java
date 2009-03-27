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

package org.apache.cassandra.utils;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor; 
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */


public class BasicUtilities
{        
	public static byte[] longToByteArray(long arg)
	{      
        byte[] retVal = new byte[8];
        ByteBuffer bb= ByteBuffer.wrap(retVal);
        bb.putLong(arg);
        return retVal; 
	 }
	
	public static long byteArrayToLong(byte[] arg)
	{
		ByteBuffer bb= ByteBuffer.wrap(arg);
		return bb.getLong();
	}
	
	public static byte[] intToByteArray(int arg)
	{      
        byte[] retVal = new byte[4];
        ByteBuffer bb= ByteBuffer.wrap(retVal);
        bb.putInt(arg);
        return retVal; 
	 }
	
	public static int byteArrayToInt(byte[] arg)
	{
		ByteBuffer bb= ByteBuffer.wrap(arg);
		return bb.getInt();
	}
	
	public static byte[] shortToByteArray(short arg)
	{      
        byte[] retVal = new byte[2];
        ByteBuffer bb= ByteBuffer.wrap(retVal);
        bb.putShort(arg);
        return retVal; 
	 }
	
	public static short byteArrayToShort(byte[] arg)
	{
		ByteBuffer bb= ByteBuffer.wrap(arg);
		return bb.getShort();
    }
}
