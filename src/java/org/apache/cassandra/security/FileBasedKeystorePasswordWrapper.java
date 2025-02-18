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

package org.apache.cassandra.security;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;

/**
 * This class provides functionality to load the password for a keystore from the password configuration or a file storing
 * the password. If the password is specified in a file, that will take precedence and the value provided via the
 * password configuration will not be used.
 */
public class FileBasedKeystorePasswordWrapper
{
    private static final Logger logger = LoggerFactory.getLogger(FileBasedKeystorePasswordWrapper.class);
    final private String keystoreFilePath;
    final private String passwordFilePath;
    private String password;
    private boolean passwordFileProvided;
    private boolean passwordFileUsed;

    public FileBasedKeystorePasswordWrapper(String keystoreFilePath, String password, String passwordFilePath)
    {
        this.keystoreFilePath = keystoreFilePath;
        this.password = password;
        this.passwordFilePath = passwordFilePath;
        ensureSingleSourceOfPassword();
        maybeKeystorePasswordFile();
    }

    public String getPassword()
    {
        return password;
    }

    /**
     * Returns if a password file was used to read the keystore's password.
     *
     * @return {@code true} if the specified password file exists and could be read successfully; {@code false} otherwise
     */
    public boolean isPasswordFileUsed()
    {
        return passwordFileUsed;
    }

    private void ensureSingleSourceOfPassword()
    {
        final boolean passwordProvided = !StringUtils.isEmpty(password);
        passwordFileProvided = !StringUtils.isEmpty(passwordFilePath);
        if (passwordProvided && passwordFileProvided)
        {
            final String msg = String.format("For %s, password was specified via configuration and the password file both. The password file will take precedence.", keystoreFilePath);
            logger.warn(msg);
        }
    }

    private void maybeKeystorePasswordFile()
    {
        if (passwordFileProvided)
        {
            File keystorePasswordFile = new File(passwordFilePath);
            if (keystorePasswordFile.exists())
            {
                try
                {
                    this.password = Files.readString(keystorePasswordFile.toPath());
                    this.passwordFileUsed = true;
                }
                catch (IOException e)
                {
                    final String msg = String.format("'Failed to read keystore password from the %s for %s",
                                                     keystorePasswordFile, keystoreFilePath);
                    throw new ConfigurationException(msg, e);
                }
            }
            else
            {
                final String msg = String.format("keystore password file %s does not exist", keystorePasswordFile.path());
                throw new ConfigurationException(msg);
            }
        }
    }
}
