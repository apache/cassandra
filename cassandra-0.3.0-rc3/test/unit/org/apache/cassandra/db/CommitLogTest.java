/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;

public class CommitLogTest extends CleanupHelper
{
    @Test
    public void testMain() throws IOException {
        // TODO this is useless, since it assumes we have a working set of commit logs to parse
        /*
        File logDir = new File(DatabaseDescriptor.getLogFileLocation());
        File[] files = logDir.listFiles();
        Arrays.sort( files, new FileUtils.FileComparator() );

        byte[] bytes = new byte[CommitLogHeader.size(Integer.parseInt(args[0]))];
        for ( File file : files )
        {
            CommitLog clog = new CommitLog( file );
            clog.readCommitLogHeader(file.getAbsolutePath(), bytes);
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(bytes, 0, bytes.length);
            CommitLogHeader clHeader = CommitLogHeader.serializer().deserialize(bufIn);

            StringBuilder sb = new StringBuilder("");
            for ( byte b : bytes )
            {
                sb.append(b);
                sb.append(" ");
            }

            System.out.println("FILE:" + file);
            System.out.println(clHeader.toString());
        }
        */
    }
}
