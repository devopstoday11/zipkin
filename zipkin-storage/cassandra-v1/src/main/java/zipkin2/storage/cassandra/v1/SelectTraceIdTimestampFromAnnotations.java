/*
 * Copyright 2015-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.cassandra.v1;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static zipkin2.storage.cassandra.v1.IndexTraceId.BUCKETS;
import static zipkin2.storage.cassandra.v1.Tables.ANNOTATIONS_INDEX;

final class SelectTraceIdTimestampFromAnnotations extends SelectTraceIdIndex.Factory<String> {
  SelectTraceIdTimestampFromAnnotations(CqlSession session) {
    super(session, ANNOTATIONS_INDEX, "annotation", 2);
  }

  @Override Select declarePartitionKey(Select select) {
    return super
      .declarePartitionKey(select)
      .whereColumn("bucket").in(bindMarker());
  }

  @Override void bindPartitionKey(BoundStatementBuilder bound, String partitionKey) {
    bound
      .setBytesUnsafe(0, CassandraUtil.toByteBuffer(partitionKey))
      .setList(1, BUCKETS, Integer.class);
  }
}
