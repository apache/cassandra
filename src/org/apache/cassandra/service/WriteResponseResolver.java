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

import org.apache.cassandra.db.WriteResponseMessage;
import org.apache.cassandra.net.Message;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class WriteResponseResolver implements IResponseResolver<Boolean> {

	private static Logger logger_ = Logger.getLogger(WriteResponseResolver.class);

	/*
	 * The resolve function for the Write looks at all the responses if all the
	 * respones returned are false then we have a problem since that means the
	 * key wa not written to any of the servers we want to notify the client of
	 * this so in that case we should return a false saying that the write
	 * failed.
	 * 
	 */
	public Boolean resolve(List<Message> responses) throws DigestMismatchException 
	{
		// TODO: We need to log error responses here for example
		// if a write fails for a key log that the key could not be replicated
		boolean returnValue = false;
		for (Message response : responses) {
			Object[] body = response.getMessageBody();
			WriteResponseMessage writeResponseMessage = (WriteResponseMessage) body[0];
			boolean result = writeResponseMessage.isSuccess();
			if (!result) {
				logger_.debug("Write at " + response.getFrom()
						+ " may have failed for the key " + writeResponseMessage.key());
			}
			returnValue |= result;
		}
		return returnValue;
	}

	public boolean isDataPresent(List<Message> responses)
	{
		return true;
	}
	
}
