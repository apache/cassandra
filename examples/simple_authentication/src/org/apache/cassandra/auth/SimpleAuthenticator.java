package org.apache.cassandra.auth;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;

public class SimpleAuthenticator implements IAuthenticator
{
    public final static String PASSWD_FILENAME_PROPERTY        = "passwd.properties";
    public final static String PMODE_PROPERTY                  = "passwd.mode";
    public static final String USERNAME_KEY                    = "username";
    public static final String PASSWORD_KEY                    = "password";

    public enum PasswordMode
    {
        PLAIN, MD5,
    };

    public AuthenticatedUser defaultUser()
    {
        // users must log in
        return null;
    }

    public AuthenticatedUser authenticate(Map<? extends CharSequence,? extends CharSequence> credentials) throws AuthenticationException
    {
        String pmode_plain = System.getProperty(PMODE_PROPERTY);
        PasswordMode mode = PasswordMode.PLAIN;

        if (null != pmode_plain)
        {
            try
            {
                mode = PasswordMode.valueOf(pmode_plain);
            }
            catch (Exception e)
            {
                // this is not worth a StringBuffer
                String mode_values = "";
                for (PasswordMode pm : PasswordMode.values())
                    mode_values += "'" + pm + "', ";

                mode_values += "or leave it unspecified.";
                throw new AuthenticationException("The requested password check mode '" + pmode_plain + "' is not a valid mode.  Possible values are " + mode_values);
            }
        }

        String pfilename = System.getProperty(PASSWD_FILENAME_PROPERTY);

        String username = null;
        CharSequence user = credentials.get(USERNAME_KEY);
        if (null == user) 
            throw new AuthenticationException("Authentication request was missing the required key '" + USERNAME_KEY + "'");
        else
            username = user.toString();

        String password = null;
        CharSequence pass = credentials.get(PASSWORD_KEY);
        if (null == pass) 
            throw new AuthenticationException("Authentication request was missing the required key '" + PASSWORD_KEY + "'");
        else
            password = pass.toString();

        boolean authenticated = false;

        InputStream in = null;
        try
        {
            in = new BufferedInputStream(new FileInputStream(pfilename));
            Properties props = new Properties();
            props.load(in);

            // note we keep the message here and for the wrong password exactly the same to prevent attackers from guessing what users are valid
            if (null == props.getProperty(username)) throw new AuthenticationException(authenticationErrorMessage(mode, username));
            switch (mode)
            {
                case PLAIN:
                    authenticated = password.equals(props.getProperty(username));
                    break;
                case MD5:
                    authenticated = MessageDigest.isEqual(FBUtilities.threadLocalMD5Digest().digest(password.getBytes()), Hex.hexToBytes(props.getProperty(username)));
                    break;
                default:
                    throw new RuntimeException("Unknown PasswordMode " + mode);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Authentication table file given by property " + PASSWD_FILENAME_PROPERTY + " could not be opened: " + e.getMessage());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected authentication problem", e);
        }
        finally
        {
            FileUtils.closeQuietly(in);
        }

        if (!authenticated) throw new AuthenticationException(authenticationErrorMessage(mode, username));

        return new AuthenticatedUser(username);
    }

    public void validateConfiguration() throws ConfigurationException
    {
        String pfilename = System.getProperty(SimpleAuthenticator.PASSWD_FILENAME_PROPERTY);
        if (pfilename == null)
        {
            throw new ConfigurationException("When using " + this.getClass().getCanonicalName() + " " + 
                    SimpleAuthenticator.PASSWD_FILENAME_PROPERTY + " properties must be defined.");	
        }
    }

    static String authenticationErrorMessage(PasswordMode mode, String username)
    {
        return String.format("Given password in password mode %s could not be validated for user %s", mode, username);
    }
}
