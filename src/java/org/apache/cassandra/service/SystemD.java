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
package org.apache.cassandra.service;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jna.Library; // NOSONAR
import com.sun.jna.Native; // NOSONAR

public class SystemD {

	private static final Logger logger = LoggerFactory.getLogger(SystemD.class);
	private static final Api INSTANCE = init();

	private static final CLibrary LIBC = (CLibrary) Native.loadLibrary("c", CLibrary.class);

	private interface CLibrary extends Library {

		int getpid();
	}

	public static class Api {

		private final RawApi impl;

		private Api(RawApi rawApi) {
			this.impl = rawApi;
		}

		public void notifyReady() {
			int pid = LIBC.getpid();
			logger.info("Notify ready through systemd for PID {}...", pid);
			int errorCode = impl.sd_pid_notify(pid, 0, "READY=1");
			if (errorCode <= 0) {
				logger.error("Notify failed: {}", errorCode);
			} else {
				logger.info("Notified for {}, errorCode: {}", pid, errorCode);
			}
		}

	}

	private interface RawApi extends Library {

		int sd_pid_notify(int pid, int unset, String state); // NOSONAR

	}

	@VisibleForTesting
	public static boolean isAvailable() {
		if (INSTANCE == null) {
			return false;
		}
		return true;
	}

	@VisibleForTesting
	public static Api get() {
		if (!isAvailable()) {
			throw new RuntimeException("SystemD is not available");
		}
		return INSTANCE;
	}

	private static Api init() {
		try {
			RawApi rawApi = Native.load("systemd", RawApi.class);
			return new Api(rawApi);
		} catch (UnsatisfiedLinkError le) {
			logger.warn("SystemD support is not available: {}", le.getMessage());
			return null;
		}
	}

}
