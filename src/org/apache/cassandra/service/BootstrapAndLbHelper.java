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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.io.SSTable;
import org.apache.log4j.Logger;

public final class BootstrapAndLbHelper
{
    private static final Logger logger_ = Logger.getLogger(BootstrapAndLbHelper.class);
    private static List<String> getIndexedPrimaryKeys()
    {
        List<String> indexedPrimaryKeys = SSTable.getIndexedKeys();
        Iterator<String> it = indexedPrimaryKeys.iterator();
        
        while ( it.hasNext() )
        {
            String key = it.next();
            if ( !StorageService.instance().isPrimary(key) )
            {
                it.remove();
            }
        }
        return indexedPrimaryKeys;
    }
    
    /**
     * Given the number of keys that need to be transferred say, 1000
     * and given the smallest key stored we need the hash of the 1000th
     * key greater than the smallest key in the sorted order in the primary
     * range.
     * 
     * @param keyCount number of keys after which token is required.
     * @return token.
    */
    public static BigInteger getTokenBasedOnPrimaryCount(int keyCount)
    {
        List<String> indexedPrimaryKeys = getIndexedPrimaryKeys();
        int index = keyCount / SSTable.indexInterval();
        String key = (index >= indexedPrimaryKeys.size()) ? indexedPrimaryKeys.get( indexedPrimaryKeys.size() - 1 ) : indexedPrimaryKeys.get(index);
        logger_.debug("Hashing key " + key + " ...");
        return StorageService.instance().hash(key);
    }
}
