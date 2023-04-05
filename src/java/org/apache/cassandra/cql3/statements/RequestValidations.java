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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Utility methods use to perform request validation.
 *
 * <p>This class use overloaded methods to allow to specify different numbers of message arguments. While
 * this introduces some clutter in the API, it avoids array allocation, initialization, and garbage collection
 * overhead that is incurred by varargs calls. </p>
 *
 * <b>Warning about performance</b>
 *
 * <p>The goal of this class is to improve readability of code, but in some circumstances this may come at a
 * significant performance cost. Remember that argument values for message construction must all be computed eagerly,
 * and autoboxing may happen as well, even when the check succeeds. If the message arguments are expensive to create
 * you should use the customary form:
 *  <pre>
 *      if (value < 0.0)
 *          throw RequestValidations.invalidRequest("negative value: %s", toReadableText(value));
 *  </pre>
 * </p>
 */
public final class RequestValidations
{
    /**
     * Checks that the specified expression is {@code true}. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param message the error message
     * @throws InvalidRequestException if the specified expression is {@code false}.
     */
    public static void checkTrue(boolean expression, String message) throws InvalidRequestException
    {
        if (!expression)
            throw invalidRequest(message);
    }

    /**
     * Checks that the specified expression is <code>true</code>. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param messageArg the message argument
     * @throws InvalidRequestException if the specified expression is {@code false}.
     */
    public static void checkTrue(boolean expression,
                                 String messageTemplate,
                                 Object messageArg) throws InvalidRequestException
    {
        if (!expression)
            throw invalidRequest(messageTemplate, messageArg);
    }

    /**
     * Checks that the specified expression is <code>true</code>. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param arg1 the first message argument
     * @param arg2 the second message argument
     * @throws InvalidRequestException if the specified expression is {@code false}.
     */
    public static void checkTrue(boolean expression,
                                 String messageTemplate,
                                 Object arg1,
                                 Object arg2) throws InvalidRequestException
    {
        if (!expression)
            throw invalidRequest(messageTemplate, arg1, arg2);
    }

    /**
     * Checks that the specified expression is <code>true</code>. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param arg1 the first message argument
     * @param arg2 the second message argument
     * @param arg3 the third message argument
     * @throws InvalidRequestException if the specified expression is {@code false}.
     */
    public static void checkTrue(boolean expression,
                                 String messageTemplate,
                                 Object arg1,
                                 Object arg2,
                                 Object arg3) throws InvalidRequestException
    {
        if (!expression)
            throw invalidRequest(messageTemplate, arg1, arg2, arg3);
    }

    /**
     * Checks that the specified collections is NOT <code>empty</code>.
     * If it is an {@code InvalidRequestException} will be thrown.
     *
     * @param collection the collection to test
     * @param messageTemplate the template used to build the error message
     * @param messageArg the message argument
     * @return the collection
     * @throws InvalidRequestException if the specified collection is <code>empty</code>.
     */
    public static <T extends Collection<E>, E> T checkNotEmpty(T collection,
                                                               String messageTemplate,
                                                               Object messageArg)
                                                               throws InvalidRequestException
    {
        checkTrue(!collection.isEmpty(), messageTemplate, messageArg);
        return collection;
    }

    /**
     * Checks that the specified collections is NOT <code>empty</code>.
     * If it is an {@code InvalidRequestException} will be thrown.
     *
     * @param collection the collection to test
     * @param messageTemplate the template used to build the error message
     * @param arg1 the first message argument
     * @param arg2 the second message argument
     * @return the collection
     * @throws InvalidRequestException if the specified collection is <code>empty</code>.
     */
    public static <T extends Collection<E>, E> T checkNotEmpty(T collection,
                                                               String messageTemplate,
                                                               Object arg1,
                                                               Object arg2)
                                                               throws InvalidRequestException
    {
        checkTrue(!collection.isEmpty(), messageTemplate, arg1, arg2);
        return collection;
    }

    /**
     * Checks that the specified list does not contains duplicates.
     *
     * @param list the list to test
     * @param message the error message
     * @throws InvalidRequestException if the specified list contains duplicates.
     */
    public static void checkContainsNoDuplicates(List<?> list, String message) throws InvalidRequestException
    {
        if (new HashSet<>(list).size() != list.size())
            throw invalidRequest(message);
    }

    /**
     * Checks that the specified list contains only the specified elements.
     *
     * @param list the list to test
     * @param expectedElements the expected elements
     * @param message the error message
     * @throws InvalidRequestException if the specified list contains duplicates.
     */
    public static <E> void checkContainsOnly(List<E> list,
                                             List<E> expectedElements,
                                             String message) throws InvalidRequestException
    {
        List<E> copy = new ArrayList<>(list);
        copy.removeAll(expectedElements);
        if (!copy.isEmpty())
            throw invalidRequest(message);
    }

    /**
     * Checks that the specified expression is {@code false}. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param messageArg the message argument
     * @throws InvalidRequestException if the specified expression is <code>true</code>.
     */
    public static void checkFalse(boolean expression,
                                  String messageTemplate,
                                  Object messageArg) throws InvalidRequestException
    {
        checkTrue(!expression, messageTemplate, messageArg);
    }

    /**
     * Checks that the specified expression is {@code false}. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param arg1 the first message argument
     * @param arg2 the second message argument
     * @throws InvalidRequestException if the specified expression is <code>true</code>.
     */
    public static void checkFalse(boolean expression,
                                  String messageTemplate,
                                  Object arg1,
                                  Object arg2) throws InvalidRequestException
    {
        checkTrue(!expression, messageTemplate, arg1, arg2);
    }

    /**
     * Checks that the specified expression is {@code false}. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param arg1 the first message argument
     * @param arg2 the second message argument
     * @param arg3 the third message argument
     * @throws InvalidRequestException if the specified expression is <code>true</code>.
     */
    public static void checkFalse(boolean expression,
                                  String messageTemplate,
                                  Object arg1,
                                  Object arg2,
                                  Object arg3) throws InvalidRequestException
    {
        checkTrue(!expression, messageTemplate, arg1, arg2, arg3);
    }
    /**
     * Checks that the specified expression is {@code false}. If not an {@code InvalidRequestException} will
     * be thrown.
     *
     * @param expression the expression to test
     * @param message the error message
     * @throws InvalidRequestException if the specified expression is <code>true</code>.
     */
    public static void checkFalse(boolean expression, String message) throws InvalidRequestException
    {
        checkTrue(!expression, message);
    }

    /**
     * Checks that the specified object is NOT {@code null}.
     * If it is an {@code InvalidRequestException} will be thrown.
     *
     * @param object the object to test
     * @param message the error message
     * @return the object
     * @throws InvalidRequestException if the specified object is {@code null}.
     */
    public static <T> T checkNotNull(T object, String message) throws InvalidRequestException
    {
        checkTrue(object != null, message);
        return object;
    }

    /**
     * Checks that the specified object is NOT {@code null}.
     * If it is an {@code InvalidRequestException} will be thrown.
     *
     * @param object the object to test
     * @param messageTemplate the template used to build the error message
     * @param messageArg the message argument
     * @return the object
     * @throws InvalidRequestException if the specified object is {@code null}.
     */
    public static <T> T checkNotNull(T object, String messageTemplate, Object messageArg) throws InvalidRequestException
    {
        checkTrue(object != null, messageTemplate, messageArg);
        return object;
    }

    /**
     * Checks that the specified object is NOT {@code null}.
     * If it is an {@code InvalidRequestException} will be thrown.
     *
     * @param object the object to test
     * @param messageTemplate the template used to build the error message
     * @param arg1 the first message argument
     * @param arg2 the second message argument
     * @return the object
     * @throws InvalidRequestException if the specified object is {@code null}.
     */
    public static <T> T checkNotNull(T object,
                                     String messageTemplate,
                                     Object arg1,
                                     Object arg2) throws InvalidRequestException
    {
        checkTrue(object != null, messageTemplate, arg1, arg2);
        return object;
    }

    /**
     * Checks that the specified bind marker value is set to a meaningful value.
     * If it is not a {@code InvalidRequestException} will be thrown.
     *
     * @param b the <code>ByteBuffer</code> to test
     * @param messageTemplate the template used to build the error message
     * @param messageArg the message argument
     * @throws InvalidRequestException if the specified bind marker value is not set to a meaningful value.
     */
    public static void checkBindValueSet(ByteBuffer b, String messageTemplate, Object messageArg) throws InvalidRequestException
    {
        checkTrue(b != ByteBufferUtil.UNSET_BYTE_BUFFER, messageTemplate, messageArg);
    }

    /**
     * Checks that the specified object is {@code null}.
     * If it is not an {@code InvalidRequestException} will be thrown.
     *
     * @param object the object to test
     * @param messageTemplate the template used to build the error message
     * @param messageArg the message argument
     * @throws InvalidRequestException if the specified object is not {@code null}.
     */
    public static void checkNull(Object object, String messageTemplate, Object messageArg) throws InvalidRequestException
    {
        checkTrue(object == null, messageTemplate, messageArg);
    }

    /**
     * Checks that the specified object is {@code null}.
     * If it is not an {@code InvalidRequestException} will be thrown.
     *
     * @param object the object to test
     * @param message the error message
     * @throws InvalidRequestException if the specified object is not {@code null}.
     */
    public static void checkNull(Object object, String message) throws InvalidRequestException
    {
        checkTrue(object == null, message);
    }

    /**
     * Returns an {@code InvalidRequestException} with the specified message.
     *
     * @param message the error message
     * @return an {@code InvalidRequestException} with the specified message.
     */
    public static InvalidRequestException invalidRequest(String message)
    {
        return new InvalidRequestException(message);
    }

    /**
     * Returns an {@code InvalidRequestException} with the specified message.
     *
     * @param messageTemplate the template used to build the error message
     * @param messageArgs the message arguments
     * @return an {@code InvalidRequestException} with the specified message.
     */
    public static InvalidRequestException invalidRequest(String messageTemplate, Object... messageArgs)
    {
        return new InvalidRequestException(String.format(messageTemplate, messageArgs));
    }

    /**
     * This class must not be instantiated as it only contains static methods.
     */
    private RequestValidations()
    {

    }
}
