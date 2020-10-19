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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.storage.ITStorage;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.DAY;
import static zipkin2.TestObjects.FRONTEND;
import static zipkin2.TestObjects.TODAY;
import static zipkin2.storage.cassandra.v1.CassandraStorageExtension.rowCount;

/**
 * Large amounts of writes can make other tests flake, so separate them into a separate keyspace.
 * Note: Schema installation takes ~10s for each method, so hesitate adding too many tests like
 * these.
 */
abstract class ITLargeTraceTests extends ITStorage<CassandraStorage> {

  @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
  }

  @Test public void overFetchesToCompensateForDuplicateIndexData() throws IOException {
    int traceCount = 2000;

    List<Span> spans = new ArrayList<>();
    for (int i = 0; i < traceCount; i++) {
      final long delta = i * 1000; // all timestamps happen a millisecond later
      for (Span s : TestObjects.TRACE) {
        Span.Builder builder = s.toBuilder()
          .traceId(Long.toHexString((i + 1) * 10L))
          .timestamp(s.timestampAsLong() + delta);
        s.annotations().forEach(a -> builder.addAnnotation(a.timestamp() + delta, a.value()));
        spans.add(builder.build());
      }
    }

    accept(spans.toArray(new Span[0]));

    // Index ends up containing more rows than services * trace count, and cannot be de-duped
    // in a server-side query.
    int localServiceCount = storage.serviceAndSpanNames().getServiceNames().execute().size();
    assertThat(storage
      .session()
      .execute("SELECT COUNT(*) from service_name_index")
      .one()
      .getLong(0))
      .isGreaterThan(traceCount * localServiceCount);

    // Implementation over-fetches on the index to allow the user to receive unsurprising results.
    QueryRequest request = requestBuilder()
      .serviceName("frontend") // Ensure we use serviceName so that trace_by_service_span is used
      .lookback(DAY).limit(traceCount).build();

    // Don't use hasSize on the result as it will fill console with span json
    assertThat(store().getTraces(request).execute().size())
      .isEqualTo(traceCount);
  }

  /**
   * Simulates a trace with a step pattern, where each span starts a millisecond after the prior
   * one. The consumer code optimizes index inserts to only represent the interval represented by
   * the trace as opposed to each individual timestamp.
   */
  @Test public void skipsRedundantIndexingInATrace() throws IOException {
    Span[] trace = new Span[101];
    trace[0] = TestObjects.CLIENT_SPAN.toBuilder().kind(Span.Kind.SERVER).build();

    IntStream.range(0, 100).forEach(i -> trace[i + 1] = Span.newBuilder()
      .traceId(trace[0].traceId())
      .parentId(trace[0].id())
      .id(Long.toHexString(i + 1))
      .name("get")
      .kind(Span.Kind.CLIENT)
      .localEndpoint(FRONTEND)
      .timestamp(trace[0].timestampAsLong() + i * 1000) // peer span timestamps happen 1ms later
      .duration(10L)
      .build());

    accept(trace);
    assertThat(rowCount(storage, Tables.ANNOTATIONS_INDEX)).isEqualTo(5L);
    // remoteEndpoint was only in the root span
    assertThat(rowCount(storage, Tables.SERVICE_REMOTE_SERVICE_NAME_INDEX)).isEqualTo(1L);

    // For this reason, we expect to see 3 rows for span[0], span[99] and span[100] timestamps.
    assertThat(rowCount(storage, Tables.SERVICE_NAME_INDEX)).isEqualTo(3L);
    assertThat(rowCount(storage, Tables.SERVICE_SPAN_NAME_INDEX)).isEqualTo(3L);

    // redundant store doesn't change the indexes
    accept(trace);
    assertThat(rowCount(storage, Tables.ANNOTATIONS_INDEX)).isEqualTo(5L);
    assertThat(rowCount(storage, Tables.SERVICE_REMOTE_SERVICE_NAME_INDEX)).isEqualTo(1L);
    assertThat(rowCount(storage, Tables.SERVICE_NAME_INDEX)).isEqualTo(3L);
    assertThat(rowCount(storage, Tables.SERVICE_SPAN_NAME_INDEX)).isEqualTo(3L);
  }

  QueryRequest.Builder requestBuilder() {
    return QueryRequest.newBuilder().endTs(TODAY + DAY).lookback(DAY * 2).limit(100);
  }
}
