package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.TriggerOptions;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropTriggerStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(DropTriggerStatement.class);
    private final String triggerName;

    public DropTriggerStatement(CFName name, String triggerName)
    {
        super(name);
        this.triggerName = triggerName;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily());
        if (cfm.getTriggerClasses() == null)
            throw new RequestValidationException(ExceptionCode.CONFIG_ERROR, "No triggers found") {};
        if (!TriggerOptions.hasTrigger(cfm, triggerName))
            throw new RequestValidationException(ExceptionCode.CONFIG_ERROR, "trigger: " + triggerName + ", not found") {};
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).clone();
        TriggerOptions.remove(cfm, triggerName);
        logger.info("Dropping trigger with name {}", triggerName);
        MigrationManager.announceColumnFamilyUpdate(cfm, false);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
