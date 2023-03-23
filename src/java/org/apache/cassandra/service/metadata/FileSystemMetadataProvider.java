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

package org.apache.cassandra.service.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

public class FileSystemMetadataProvider implements MetadataProvider
{
    private static final Logger logger = LoggerFactory.getLogger(FileSystemMetadataProvider.class);

    public static final String METADATA_FILE_NAME_KEY = "file";
    public static final String DEFAULT_METADATA_FILE_NAME = "cassandra.metadata";
    private final String metadataFile;

    public FileSystemMetadataProvider(Map<String, String> config)
    {
        if (config == null)
            metadataFile = DEFAULT_METADATA_FILE_NAME;
        else
            metadataFile = config.getOrDefault(METADATA_FILE_NAME_KEY, DEFAULT_METADATA_FILE_NAME);
    }

    @Override
    public Map<String, String> load()
    {
        File propertiesFile = new File(metadataFile);

        if (!propertiesFile.exists() || !propertiesFile.isFile() || !propertiesFile.isReadable())
        {
            logger.warn("Property file {} either does not exist or it is not a file or it is not readable. ", propertiesFile);
            return Collections.emptyMap();
        }

        try
        {
            Map props = FileUtils.readProperties(propertiesFile);
            return new HashMap<>((Map<String, String>) props);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Unable to load properties from %s", propertiesFile), ex);
        }
    }
}