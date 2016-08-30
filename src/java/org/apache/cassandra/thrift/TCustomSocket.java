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
package org.apache.cassandra.thrift;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Socket implementation of the TTransport interface.
 *
 * Adds socket buffering
 *
 */
public class TCustomSocket extends TIOStreamTransport
{

  private static final Logger LOGGER = LoggerFactory.getLogger(TCustomSocket.class.getName());

  /**
   * Wrapped Socket object
   */
  private Socket socket = null;

  /**
   * Remote host
   */
  private String host  = null;

  /**
   * Remote port
   */
  private int port = 0;

  /**
   * Socket timeout
   */
  private int timeout = 0;

  /**
   * Constructor that takes an already created socket.
   *
   * @param socket Already created socket object
   * @throws TTransportException if there is an error setting up the streams
   */
  public TCustomSocket(Socket socket) throws TTransportException
  {
    this.socket = socket;
    try
    {
      socket.setSoLinger(false, 0);
      socket.setTcpNoDelay(true);
    }
    catch (SocketException sx)
    {
      LOGGER.warn("Could not configure socket.", sx);
    }

    if (isOpen())
    {
      try
      {
        inputStream_ = new BufferedInputStream(socket.getInputStream(), 1024);
        outputStream_ = new BufferedOutputStream(socket.getOutputStream(), 1024);
      }
      catch (IOException iox)
      {
        close();
        throw new TTransportException(TTransportException.NOT_OPEN, iox);
      }
    }
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * @param host Remote host
   * @param port Remote port
   */
  public TCustomSocket(String host, int port)
  {
    this(host, port, 0);
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * @param host    Remote host
   * @param port    Remote port
   * @param timeout Socket timeout
   */
  public TCustomSocket(String host, int port, int timeout)
  {
    this.host = host;
    this.port = port;
    this.timeout = timeout;
    initSocket();
  }

  /**
   * Initializes the socket object
   */
  private void initSocket()
  {
    socket = new Socket();
    try
    {
      socket.setSoLinger(false, 0);
      socket.setTcpNoDelay(true);
      socket.setSoTimeout(timeout);
    }
    catch (SocketException sx)
    {
      LOGGER.error("Could not configure socket.", sx);
    }
  }

  /**
   * Sets the socket timeout
   *
   * @param timeout Milliseconds timeout
   */
  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
    try
    {
      socket.setSoTimeout(timeout);
    }
    catch (SocketException sx)
    {
      LOGGER.warn("Could not set socket timeout.", sx);
    }
  }

  /**
   * Returns a reference to the underlying socket.
   */
  public Socket getSocket()
  {
    if (socket == null)
    {
      initSocket();
    }
    return socket;
  }

  /**
   * Checks whether the socket is connected.
   */
  public boolean isOpen()
  {
    if (socket == null)
    {
      return false;
    }
    return socket.isConnected();
  }

  /**
   * Connects the socket, creating a new socket object if necessary.
   */
  public void open() throws TTransportException
  {
    if (isOpen())
    {
      throw new TTransportException(TTransportException.ALREADY_OPEN, "Socket already connected.");
    }

    if (host.length() == 0)
    {
      throw new TTransportException(TTransportException.NOT_OPEN, "Cannot open null host.");
    }
    if (port <= 0)
    {
      throw new TTransportException(TTransportException.NOT_OPEN, "Cannot open without port.");
    }

    if (socket == null)
    {
      initSocket();
    }

    try
    {
      socket.connect(new InetSocketAddress(host, port), timeout);
      inputStream_ = new BufferedInputStream(socket.getInputStream(), 1024);
      outputStream_ = new BufferedOutputStream(socket.getOutputStream(), 1024);
    }
    catch (IOException iox)
    {
      close();
      throw new TTransportException(TTransportException.NOT_OPEN, iox);
    }
  }

  /**
   * Closes the socket.
   */
  public void close()
  {
    // Close the underlying streams
    super.close();

    // Close the socket
    if (socket != null)
    {
      try
      {
        socket.close();
      }
      catch (IOException iox)
      {
        LOGGER.warn("Could not close socket.", iox);
      }
      socket = null;
    }
  }

}
