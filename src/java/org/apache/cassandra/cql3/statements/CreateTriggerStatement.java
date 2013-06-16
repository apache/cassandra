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
import org.apache.cassandra.triggers.TriggerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTriggerStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateTriggerStatement.class);

    private final String triggerName;
    private final String clazz;

    public CreateTriggerStatement(CFName name, String triggerName, String clazz)
    {
        super(name);
        this.triggerName = triggerName;
        this.clazz = clazz;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        try
        {
            TriggerExecutor.instance.loadTriggerInstance(clazz);
        }
        catch (Exception ex)
        {
            throw new RequestValidationException(ExceptionCode.INVALID, "Trigger class: " + clazz + ", doesnt exist.", ex) {};
        }
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).clone();
        TriggerOptions.update(cfm, triggerName, clazz);
        logger.info("Adding triggers with name {} and classes {}", triggerName, clazz);
        MigrationManager.announceColumnFamilyUpdate(cfm, false);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
