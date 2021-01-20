/*
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

package org.apache.cassandra.db.memtable;

/**
 * This class exists solely to avoid initialization of the default memtable class.
 * Some tests want to setup table parameters before initializing DatabaseDescriptor -- this allows them to do so.
 */
public class DefaultMemtableFactory
{
    // We can't use TrieMemtable.FACTORY as that requires DatabaseDescriptor to have been initialized.
    public static final Memtable.Factory INSTANCE = TrieMemtable::new;
//    public static final Memtable.Factory INSTANCE = SkipListMemtable::new;
}
