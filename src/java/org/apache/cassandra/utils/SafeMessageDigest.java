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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SafeMessageDigest
{
    private MessageDigest md_ = null;

    public static SafeMessageDigest digest_;
    static
    {
        try
        {
            digest_ = new SafeMessageDigest(MessageDigest.getInstance("SHA-1"));
        }
        catch (NoSuchAlgorithmException e)
        {
            assert (false);
        }
    }

    public SafeMessageDigest(MessageDigest md)
    {
        md_ = md;
    }

    public synchronized void update(byte[] theBytes)
    {
        md_.update(theBytes);
    }

    //NOTE: This should be used instead of seperate update() and then digest()
    public synchronized byte[] digest(byte[] theBytes)
    {
        //this does an implicit update()
        return md_.digest(theBytes);
    }

    public synchronized byte[] digest()
    {
        return md_.digest();
    }

    public byte[] unprotectedDigest()
    {
        return md_.digest();
    }

    public void unprotectedUpdate(byte[] theBytes)
    {
        md_.update(theBytes);
    }

    public byte[] unprotectedDigest(byte[] theBytes)
    {
        return md_.digest(theBytes);
    }

    public int getDigestLength()
    {
        return md_.getDigestLength();
    }
}
