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

package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.PartitionerType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;


public class PrimaryKey implements Comparable<PrimaryKey>
{   
    public static List<PrimaryKey> create(Set<String> keys)
    {
        List<PrimaryKey> list = new ArrayList<PrimaryKey>();
        for ( String key : keys )
        {
            list.add( new PrimaryKey(key) );
        }
        Collections.sort(list);
        return list;
    }
    
    /* MD5 hash of the key_ */
    private BigInteger hash_;
    /* Key used by the application */
    private String key_;
    
    PrimaryKey(String key)
    {
        PartitionerType pType = StorageService.getPartitionerType();
        switch (pType)
        {
            case RANDOM:
                hash_ = FBUtilities.hash(key);
                break;
            
            case OPHF:
                break;
                
            default:
                hash_ = hash_ = FBUtilities.hash(key);
                break;
        }        
        key_ = key;
    }
    
    PrimaryKey(String key, BigInteger hash)
    {
        hash_ = hash;
        key_ = key;
    }
    
    public String key()
    {
        return key_;
    }
    
    public BigInteger hash()
    {
        return hash_;
    }
    
    /**
     * This performs semantic comparison of Primary Keys.
     * If the partition algorithm chosen is "Random" then
     * the hash of the key is used for comparison. If it 
     * is an OPHF then the key is used.
     * 
     * @param rhs primary against which we wish to compare.
     * @return
     */
    public int compareTo(PrimaryKey rhs)
    {
        int value = 0;
        PartitionerType pType = StorageService.getPartitionerType();
        switch (pType)
        {
            case RANDOM:
                value = hash_.compareTo(rhs.hash_);
                break;
            
            case OPHF:
                value = key_.compareTo(rhs.key_);
                break;
                
            default:
                value = hash_.compareTo(rhs.hash_);
                break;
        }        
        return value;
    }
    
    @Override
    public String toString()
    {
        return (hash_ != null) ? (key_ + ":" + hash_) : key_;
    }
}
