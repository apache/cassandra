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

import java.nio.ByteBuffer;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */


public class BasicUtilities
{        
	public static byte[] longToByteArray(long arg)
	{      
        byte[] retVal = new byte[8];
        ByteBuffer.wrap(retVal).putLong(arg);
        return retVal; 
	 }
	
	public static long byteArrayToLong(byte[] arg)
	{
        return ByteBuffer.wrap(arg).getLong();
	}
	
	public static byte[] intToByteArray(int arg)
	{      
        byte[] retVal = new byte[4];
        ByteBuffer.wrap(retVal).putInt(arg);
        return retVal; 
	 }
	
	public static int byteArrayToInt(byte[] arg)
	{
        return ByteBuffer.wrap(arg).getInt();
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
        return ByteBuffer.wrap(arg).getShort();
    }
	
	public static byte[] booleanToByteArray(boolean b)
	{
	    return b ? shortToByteArray((short)1) : shortToByteArray((short)0);
	}
	
	public static boolean byteArrayToBoolean(byte[] arg)
	{
	    return (byteArrayToShort(arg) == (short) 1) ? true : false;
	}
}
