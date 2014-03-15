package io.teknek.arizona;

import io.teknek.arizona.transform.FunctionalTransform;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;

public class ArizonaProxy {

  public static ColumnFamily functional_transform(String keyspaceName, String cfName,
          ByteBuffer key, FunctionalTransform transform, 
          ConsistencyLevel consistencyForPaxos, ConsistencyLevel consistencyForCommit)
          throws UnavailableException, IsBootstrappingException, ReadTimeoutException,
          WriteTimeoutException, InvalidRequestException {
    return null;
  }
}

