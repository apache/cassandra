package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.UnavailableException;

class NoConsistencyWriteResponseHandler implements IWriteResponseHandler
{
    static final IWriteResponseHandler instance = new NoConsistencyWriteResponseHandler();

    public void get() throws TimeoutException {}

    public void addHintCallback(Message hintedMessage, InetAddress destination) {}

    public void response(Message msg) {}

    public void assureSufficientLiveNodes() throws UnavailableException {}
}
