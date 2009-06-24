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

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class DBManager
{
    private static DBManager dbMgr_;

    public static synchronized DBManager instance() throws IOException
    {
        if (dbMgr_ == null)
        {
           dbMgr_ = new DBManager();
        }
        return dbMgr_;
    }

    public static class StorageMetadata
    {
        private Token myToken;
        private int generation_;

        StorageMetadata(Token storageId, int generation)
        {
            myToken = storageId;
            generation_ = generation;
        }

        public Token getStorageId()
        {
            return myToken;
        }

        public void setStorageId(Token storageId)
        {
            myToken = storageId;
        }

        public int getGeneration()
        {
            return generation_;
        }
    }

    public DBManager() throws IOException
    {
        Set<String> tables = DatabaseDescriptor.getTableToColumnFamilyMap().keySet();
        
        for (String table : tables)
        {
            Table tbl = Table.open(table);
            tbl.onStart();
        }
        /* Do recovery if need be. */
        RecoveryManager recoveryMgr = RecoveryManager.instance();
        recoveryMgr.doRecovery();
    }
}
