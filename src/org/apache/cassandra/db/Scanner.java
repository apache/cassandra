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

import java.util.*;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;


/**
 * This class is used to loop through a retrieved column family
 * to get all columns in Iterator style. Usage is as follows:
 * Scanner scanner = new Scanner("table");
 * scanner.fetchColumnfamily(key, "column-family");
 * 
 * while ( scanner.hasNext() )
 * {
 *     Column column = scanner.next();
 *     // Do something with the column
 * }
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Scanner implements IScanner<IColumn>
{
    /* Table over which we are scanning. */
    private String table_; 
    /* Iterator when iterating over the columns of a given key in a column family */
    private Iterator<IColumn> columnIt_;
        
    public Scanner(String table)
    {
        table_ = table;
    }
    
    /**
     * Fetch the columns associated with this key for the specified column family.
     * This method basically sets up an iterator internally and then provides an 
     * iterator like interface to iterate over the columns.
     * @param key key we are interested in.
     * @param cf column family we are interested in.
     * @throws IOException
     * @throws ColumnFamilyNotDefinedException
     */
    public void fetch(String key, String cf) throws IOException, ColumnFamilyNotDefinedException
    {        
        if ( cf != null )
        {
            Table table = Table.open(table_);
            ColumnFamily columnFamily = table.get(key, cf);
            if ( columnFamily != null )
            {
                Collection<IColumn> columns = columnFamily.getAllColumns();            
                columnIt_ = columns.iterator();
            }
        }
    }        
    
    public boolean hasNext() throws IOException
    {
        return columnIt_.hasNext();
    }
    
    public IColumn next()
    {
        return columnIt_.next();
    }
    
    public void close() throws IOException
    {
        throw new UnsupportedOperationException("This operation is not supported in the Scanner");
    }
}
