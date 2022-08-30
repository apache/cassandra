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

package org.apache.cassandra.auth.jmx;

import java.lang.reflect.*;
import java.security.AccessControlContext;
import java.security.AccessController;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a proxy interface to the platform's MBeanServer instance to perform minimal authorization and prevent
 * certain known security issues. In Cassandra 3.11+, this goes even further to include resource-based authorization
 * controls.
 *
 * Certain operations are never allowed for users and these are recorded in a deny list so that we can short circuit
 * authorization process if one is attempted by a remote subject.
 */
public class AuthorizationProxy implements InvocationHandler
{
    private static final Logger logger = LoggerFactory.getLogger(AuthorizationProxy.class);

    private MBeanServer mbs;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        String methodName = method.getName();

        if ("getMBeanServer".equals(methodName))
            throw new SecurityException("Access denied");

        // Corresponds to MBeanServer.invoke
        if (methodName.equals("invoke") && args.length == 4)
            checkVulnerableMethods(args);

        // Retrieve Subject from current AccessControlContext
        AccessControlContext acc = AccessController.getContext();
        Subject subject = Subject.getSubject(acc);

        // Allow setMBeanServer iff performed on behalf of the connector server itself
        if (("setMBeanServer").equals(methodName))
        {
            if (subject != null)
                throw new SecurityException("Access denied");

            if (args[0] == null)
                throw new IllegalArgumentException("Null MBeanServer");

            if (mbs != null)
                throw new IllegalArgumentException("MBeanServer already initialized");

            mbs = (MBeanServer) args[0];
            return null;
        }

        return invoke(method, args);
    }

    /**
     * Invoke a method on the MBeanServer instance. This is called when authorization is not required (because
     * AllowAllAuthorizer is configured, or because the invocation is being performed by the JMXConnector
     * itself rather than by a connected client), and also when a call from an authenticated subject
     * has been successfully authorized
     *
     * @param method
     * @param args
     * @return
     * @throws Throwable
     */
    private Object invoke(Method method, Object[] args) throws Throwable
    {
        try
        {
            return method.invoke(mbs, args);
        }
        catch (InvocationTargetException e) //Catch any exception that might have been thrown by the mbeans
        {
            Throwable t = e.getCause(); //Throw the exception that nodetool etc expects
            throw t;
        }
    }

    private void checkVulnerableMethods(Object args[])
    {
        assert args.length == 4;
        ObjectName name;
        String operationName;
        Object[] params;
        String[] signature;
        try
        {
            name = (ObjectName) args[0];
            operationName = (String) args[1];
            params = (Object[]) args[2];
            signature = (String[]) args[3];
        }
        catch (ClassCastException cce)
        {
            logger.warn("Could not interpret arguments to check vulnerable MBean invocations; did the MBeanServer interface change?", cce);
            return;
        }

        // When adding compiler directives from a file, most JDKs will log the file contents if invalid, which
        // leads to an arbitrary file read vulnerability
        checkCompilerDirectiveAddMethods(name, operationName);

        // Loading arbitrary (JVM and native) libraries from remotes
        checkJvmtiLoad(name, operationName);
        checkMLetMethods(name, operationName);
    }

    private void checkCompilerDirectiveAddMethods(ObjectName name, String operation)
    {
        if (name.getCanonicalName().equals("com.sun.management:type=DiagnosticCommand")
                && operation.equals("compilerDirectivesAdd"))
            throw new SecurityException("Access is denied!");
    }

    private void checkJvmtiLoad(ObjectName name, String operation)
    {
        if (name.getCanonicalName().equals("com.sun.management:type=DiagnosticCommand")
                && operation.equals("jvmtiAgentLoad"))
            throw new SecurityException("Access is denied!");
    }

    private void checkMLetMethods(ObjectName name, String operation)
    {
        // Inspired by MBeanServerAccessController, but that class ignores check if a SecurityManager is installed,
        // which we don't want

        if (operation == null)
            return;

        try
        {
            if (!mbs.isInstanceOf(name, "javax.management.loading.MLet"))
                return;
        } catch (InstanceNotFoundException infe)
        {
            return;
        }

        if (operation.equals("addURL") || operation.equals("getMBeansFromURL"))
            throw new SecurityException("Access is denied!");
    }
}

