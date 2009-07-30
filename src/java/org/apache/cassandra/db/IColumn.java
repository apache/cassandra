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

import java.util.Collection;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface IColumn
{
    public static short UtfPrefix_ = 2;
    public boolean isMarkedForDelete();
    public long getMarkedForDeleteAt();
    public byte[] name();
    public int size();
    public int serializedSize();
    public long timestamp();
    public long timestamp(byte[] columnName);
    public byte[] value();
    public byte[] value(byte[] columnName);
    public Collection<IColumn> getSubColumns();
    public IColumn getSubColumn(byte[] columnName);
    public void addColumn(IColumn column);
    public IColumn diff(IColumn column);
    public int getObjectCount();
    public byte[] digest();
    public int getLocalDeletionTime(); // for tombstone GC, so int is sufficient granularity
    public String getString(AbstractType comparator);
}
