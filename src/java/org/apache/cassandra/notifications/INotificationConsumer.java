package org.apache.cassandra.notifications;

public interface INotificationConsumer
{
    void handleNotification(INotification notification, Object sender);
}
