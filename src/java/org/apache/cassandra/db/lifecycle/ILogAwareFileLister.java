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
package org.apache.cassandra.db.lifecycle;

import java.nio.file.Path;
import java.util.List;
import java.util.function.BiPredicate;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;

/**
 * An interface for listing files in a folder
 */
public interface ILogAwareFileLister
{
    /**
     * Listing files that are not removed by log transactions in a folder.
     *
     * @param folder The folder to scan
     * @param filter The filter determines which files the client wants returned
     * @param onTxnErr The behavior when we fail to list files
     * @return all files that are not removed by log transactions
     */
    List<File> list(Path folder, BiPredicate<File, Directories.FileType> filter, Directories.OnTxnErr onTxnErr);
}
