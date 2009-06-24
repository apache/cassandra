/**
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

package org.apache.cassandra.service;

import java.util.List;

import org.apache.cassandra.net.Message;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface IResponseResolver<T> {

	/*
	 * This Method resolves the responses that are passed in . for example : if
	 * its write response then all we get is true or false return values which
	 * implies if the writes were successful but for reads its more complicated
	 * you need to look at the responses and then based on differences schedule
	 * repairs . Hence you need to derive a response resolver based on your
	 * needs from this interface.
	 */
	public T resolve(List<Message> responses) throws DigestMismatchException;
	public boolean isDataPresent(List<Message> responses);

}
