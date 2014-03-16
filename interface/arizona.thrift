include "cassandra.thrift"
namespace java io.teknek.arizona

struct TransformRequest {
    1: optional binary key,
    2: optional string column_family,
    3: optional string function_name,
    4: optional map<binary,binary> function_properties
    5: optional cassandra.ConsistencyLevel serial_consistency_level=cassandra.ConsistencyLevel.SERIAL,
    6: optional cassandra.ConsistencyLevel commit_consistency_level=cassandra.ConsistencyLevel.QUORUM,
    7: optional cassandra.SlicePredicate predicate
}

struct TransformResponse {
    1: required bool success,
    2: optional list<cassandra.Column> current_value,
}

service Arizona extends cassandra.Cassandra {
  TransformResponse transform(1:required TransformRequest request)
       throws (1:cassandra.InvalidRequestException ire, 2:cassandra.UnavailableException ue, 3:cassandra.TimedOutException te)
}
