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
import java.net.*;
import java.security.*;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class GuidGenerator {
    private static Random myRand;
    private static SecureRandom mySecureRand;
    private static String s_id;
    private static SafeMessageDigest md5 = null;

    static {
        if (System.getProperty("java.security.egd") == null) {
            System.setProperty("java.security.egd", "file:/dev/urandom");
        }
        mySecureRand = new SecureRandom();
        long secureInitializer = mySecureRand.nextLong();
        myRand = new Random(secureInitializer);
        try {
            s_id = InetAddress.getLocalHost().toString();
        }
        catch (UnknownHostException e) {
            LogUtil.getLogger(GuidGenerator.class.getName()).debug(LogUtil.throwableToString(e));
        }

        try {
            MessageDigest myMd5 = MessageDigest.getInstance("MD5");
            md5 = new SafeMessageDigest(myMd5);
        }
        catch (NoSuchAlgorithmException e) {
            LogUtil.getLogger(GuidGenerator.class.getName()).debug(LogUtil.throwableToString(e));
        }
    }


    public static String guid() {
        byte[] array = guidAsBytes();
        
        StringBuffer sb = new StringBuffer();
        for (int j = 0; j < array.length; ++j) {
            int b = array[j] & 0xFF;
            if (b < 0x10) sb.append('0');
            sb.append(Integer.toHexString(b));
        }

        return convertToStandardFormat( sb.toString() );
    }
    
    public static String guidToString(byte[] bytes)
    {
        StringBuffer sb = new StringBuffer();
        for (int j = 0; j < bytes.length; ++j) {
            int b = bytes[j] & 0xFF;
            if (b < 0x10) sb.append('0');
            sb.append(Integer.toHexString(b));
        }

        return convertToStandardFormat( sb.toString() );
    }
    
    public static byte[] guidAsBytes()
    {
        StringBuffer sbValueBeforeMD5 = new StringBuffer();
        long time = System.currentTimeMillis();
        long rand = 0;
        rand = myRand.nextLong();
        sbValueBeforeMD5.append(s_id);
        sbValueBeforeMD5.append(":");
        sbValueBeforeMD5.append(Long.toString(time));
        sbValueBeforeMD5.append(":");
        sbValueBeforeMD5.append(Long.toString(rand));

        String valueBeforeMD5 = sbValueBeforeMD5.toString();
        return md5.digest(valueBeforeMD5.getBytes());
    }

    /*
        * Convert to the standard format for GUID
        * Example: C2FEEEAC-CFCD-11D1-8B05-00600806D9B6
    */

    private static String convertToStandardFormat(String valueAfterMD5) {
        String raw = valueAfterMD5.toUpperCase();
        StringBuffer sb = new StringBuffer();
        sb.append(raw.substring(0, 8));
        sb.append("-");
        sb.append(raw.substring(8, 12));
        sb.append("-");
        sb.append(raw.substring(12, 16));
        sb.append("-");
        sb.append(raw.substring(16, 20));
        sb.append("-");
        sb.append(raw.substring(20));
        return sb.toString();
    }
}






