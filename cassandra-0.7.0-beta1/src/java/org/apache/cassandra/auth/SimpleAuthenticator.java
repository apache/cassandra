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


import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.AccessLevel;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;

public class SimpleAuthenticator implements IAuthenticator
{
    public final static String PASSWD_FILENAME_PROPERTY        = "passwd.properties";
    public final static String ACCESS_FILENAME_PROPERTY        = "access.properties";
    public final static String PMODE_PROPERTY                  = "passwd.mode";
    public static final String USERNAME_KEY                    = "username";
    public static final String PASSWORD_KEY                    = "password";

    public enum PasswordMode
    {
        PLAIN, MD5,
    };

    @Override
    public AccessLevel login(String keyspace, AuthenticationRequest authRequest) throws AuthenticationException, AuthorizationException
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

        String username = authRequest.getCredentials().get(USERNAME_KEY);
        if (null == username) throw new AuthenticationException("Authentication request was missing the required key '" + USERNAME_KEY + "'");

        String password = authRequest.getCredentials().get(PASSWORD_KEY);
        if (null == password) throw new AuthenticationException("Authentication request was missing the required key '" + PASSWORD_KEY + "'");

        boolean authenticated = false;

        try
        {
            FileInputStream in = new FileInputStream(pfilename);
            Properties props = new Properties();
            props.load(in);
            in.close();

            // note we keep the message here and for the wrong password exactly the same to prevent attackers from guessing what users are valid
            if (null == props.getProperty(username)) throw new AuthenticationException(authenticationErrorMessage(mode, username));
            switch (mode)
            {
                case PLAIN:
                    authenticated = password.equals(props.getProperty(username));
                    break;
                case MD5:
                    authenticated = MessageDigest.isEqual(password.getBytes(), MessageDigest.getInstance("MD5").digest(props.getProperty(username).getBytes()));
                    break;
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AuthenticationException("You requested MD5 checking but the MD5 digest algorithm is not available: " + e.getMessage());
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException("Authentication table file given by property " + PASSWD_FILENAME_PROPERTY + " could not be found: " + e.getMessage());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Authentication table file given by property " + PASSWD_FILENAME_PROPERTY + " could not be opened: " + e.getMessage());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected authentication problem", e);
        }

        if (!authenticated) throw new AuthenticationException(authenticationErrorMessage(mode, username));

        // if we're here, the authentication succeeded. Now let's see if the user is authorized for this keyspace.

        String afilename = System.getProperty(ACCESS_FILENAME_PROPERTY);
        AccessLevel authorized = AccessLevel.NONE;
        try
        {
            FileInputStream in = new FileInputStream(afilename);
            Properties props = new Properties();
            props.load(in);
            in.close();

            // structure:
            // given keyspace X, users A B and C can be authorized like this (separate their names with spaces):
            // X = A B C
            
            // note we keep the message here and for other authorization problems exactly the same to prevent attackers
            // from guessing what keyspaces are valid
            if (null == props.getProperty(keyspace))
                throw new AuthorizationException(authorizationErrorMessage(keyspace, username));

            for (String allow : props.getProperty(keyspace).split(","))
            {
                if (allow.equals(username)) authorized = AccessLevel.FULL;
            }
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException("Authorization table file given by property " + ACCESS_FILENAME_PROPERTY + " could not be found: " + e.getMessage());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Authorization table file given by property " + ACCESS_FILENAME_PROPERTY + " could not be opened: " + e.getMessage());
        }

        if (authorized == AccessLevel.NONE) throw new AuthorizationException(authorizationErrorMessage(keyspace, username));
        
        return authorized;
    }

   @Override
    public void validateConfiguration() throws ConfigurationException 
    {
        String aFileName = System.getProperty(SimpleAuthenticator.ACCESS_FILENAME_PROPERTY);
        String pfilename = System.getProperty(SimpleAuthenticator.PASSWD_FILENAME_PROPERTY);
        if (aFileName == null || pfilename == null)
        {
            throw new ConfigurationException("When using " + this.getClass().getCanonicalName() + " " + 
                    SimpleAuthenticator.ACCESS_FILENAME_PROPERTY + " and " + 
                    SimpleAuthenticator.PASSWD_FILENAME_PROPERTY + " properties must be defined.");	
        }
    }

    static String authorizationErrorMessage(String keyspace, String username)
    {
        return String.format("User %s could not be authorized to use keyspace %s", username, keyspace);
    }

    static String authenticationErrorMessage(PasswordMode mode, String username)
    {
        return String.format("Given password in password mode %s could not be validated for user %s", mode, username);
    }
}
