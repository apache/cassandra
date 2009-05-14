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
package org.apache.cassandra;

import java.io.File;

import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.log4j.Logger;

public class CleanupHelper
{
    private static Logger logger = Logger.getLogger(CleanupHelper.class);

    @BeforeClass
    public static void cleanup()
    {
        // we clean the fs twice, once to start with (so old data files don't get stored by anything static if this is the first run)
        // and once after flushing stuff (to try to clean things out if it is not.)  part #2 seems to be less than perfect.
        String[] directoryNames = {
                DatabaseDescriptor.getBootstrapFileLocation(),
                DatabaseDescriptor.getLogFileLocation(),
                DatabaseDescriptor.getDataFileLocation(),
                DatabaseDescriptor.getMetadataDirectory(),
        };

        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
            {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            for (File f : dir.listFiles())
            {
                logger.debug("deleting " + f);
                f.delete();
            }
        }
    }
}