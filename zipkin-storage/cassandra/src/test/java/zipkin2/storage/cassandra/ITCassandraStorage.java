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
package zipkin2.storage.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.StorageComponent.Builder;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.DAY;
import static zipkin2.TestObjects.TODAY;
import static zipkin2.storage.cassandra.InternalForTests.writeDependencyLinks;
import static zipkin2.storage.cassandra.Schema.TABLE_AUTOCOMPLETE_TAGS;
import static zipkin2.storage.cassandra.Schema.TABLE_SERVICE_REMOTE_SERVICES;
import static zipkin2.storage.cassandra.Schema.TABLE_SERVICE_SPANS;
import static zipkin2.storage.cassandra.Schema.TABLE_TRACE_BY_SERVICE_REMOTE_SERVICE;
import static zipkin2.storage.cassandra.Schema.TABLE_TRACE_BY_SERVICE_SPAN;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ITCassandraStorage {
  static final List<String> SEARCH_TABLES = asList(
    TABLE_AUTOCOMPLETE_TAGS,
    TABLE_SERVICE_REMOTE_SERVICES,
    TABLE_SERVICE_SPANS,
    TABLE_TRACE_BY_SERVICE_REMOTE_SERVICE,
    TABLE_TRACE_BY_SERVICE_SPAN
  );

  @RegisterExtension CassandraStorageExtension backend = new CassandraStorageExtension(
    "openzipkin/zipkin-cassandra:2.21.7");

  @Nested
  class ITTraces extends zipkin2.storage.ITTraces<CassandraStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Override @Test @Disabled("No consumer-side span deduplication")
    public void getTrace_deduplicates() {
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }
  }

  @Nested
  class ITSpanStore extends zipkin2.storage.ITSpanStore<CassandraStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Test void overFetchesToCompensateForDuplicateIndexData() throws IOException {
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
        .execute("SELECT COUNT(*) from trace_by_service_span")
        .one()
        .getLong(0))
        .isGreaterThan(traceCount * localServiceCount);

      // Implementation over-fetches on the index to allow the user to receive unsurprising results.
      QueryRequest request = requestBuilder()
        .serviceName("frontend") // Ensure we use serviceName so that trace_by_service_span is used
        .lookback(DAY).limit(traceCount).build();
      assertThat(store().getTraces(request).execute())
        .hasSize(traceCount);
    }

    @Test void searchingByAnnotationShouldFilterBeforeLimiting() throws IOException {
      int queryLimit = 2;
      int nbTraceFetched = queryLimit * storage.indexFetchMultiplier;

      for (int i = 0; i < nbTraceFetched; i++) {
        accept(TestObjects.LOTS_OF_SPANS[i++].toBuilder().timestamp((TODAY - i) * 1000L).build());
      }

      // Add two traces with the tag we're looking for before the preceding ones
      Endpoint endpoint = TestObjects.LOTS_OF_SPANS[0].localEndpoint();
      for (int i = 0; i < 2; i++) {
        int j = nbTraceFetched + i;
        accept(TestObjects.LOTS_OF_SPANS[j].toBuilder()
          .timestamp((TODAY - j) * 1000L)
          .localEndpoint(endpoint)
          .putTag("host.name", "host1")
          .build());
      }
      QueryRequest queryRequest = requestBuilder()
        .parseAnnotationQuery("host.name=host1")
        .serviceName(endpoint.serviceName())
        .limit(queryLimit)
        .build();
      assertThat(store().getTraces(queryRequest).execute()).hasSize(queryLimit);
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }
  }

  @Nested
  class ITSearchEnabledFalse extends zipkin2.storage.ITSearchEnabledFalse<CassandraStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }
  }

  @Nested
  class ITStrictTraceIdFalse extends zipkin2.storage.ITStrictTraceIdFalse<CassandraStorage> {
    CassandraStorage strictTraceId;

    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @BeforeEach void initializeStorageBeforeSwitch() {
      strictTraceId = CassandraStorageExtension.newStorageBuilder(storage.contactPoints)
        .keyspace(storage.keyspace)
        .build();
    }

    @AfterEach void closeStorageBeforeSwitch() {
      if (strictTraceId != null) {
        strictTraceId.close();
        strictTraceId = null;
      }
    }

    /** Ensures we can still lookup fully 128-bit traces when strict trace ID id disabled */
    @Test public void getTraces_128BitTraceId() throws IOException {
      getTraces_128BitTraceId(accept128BitTrace(strictTraceId));
    }

    /** Ensures data written before strict trace ID was enabled can be read */
    @Test public void getTrace_retrievesBy128BitTraceId_afterSwitch() throws IOException {
      List<Span> trace = accept128BitTrace(strictTraceId);

      assertThat(sortTrace(traces().getTrace(trace.get(0).traceId()).execute()))
        .containsExactlyElementsOf(trace);
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }
  }

  @Nested
  class ITServiceAndSpanNames extends zipkin2.storage.ITServiceAndSpanNames<CassandraStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }
  }

  @Nested
  class ITAutocompleteTags extends zipkin2.storage.ITAutocompleteTags<CassandraStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }
  }

  @Nested
  class ITDependencies extends zipkin2.storage.ITDependencies<CassandraStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }

    /**
     * The current implementation does not include dependency aggregation. It includes retrieval of
     * pre-aggregated links, usually made via zipkin-dependencies
     */
    @Override protected void processDependencies(List<Span> spans) {
      aggregateLinks(spans).forEach(
        (midnight, links) -> writeDependencyLinks(storage, links, midnight));
      blockWhileInFlight();
    }
  }

  @Nested
  class ITEnsureSchema extends zipkin2.storage.cassandra.ITEnsureSchema {
    TestInfo testInfo;

    @BeforeEach void setTestInfo(TestInfo testInfo) {
      this.testInfo = testInfo;
    }

    @Override protected String keyspace() {
      return InternalForTests.keyspace(testInfo);
    }

    @Override protected CqlSession session() {
      return backend.globalSession;
    }

    @Override protected String contactPoint() {
      return backend.contactPoint();
    }
  }

  @Nested
  class ITSpanConsumer extends zipkin2.storage.cassandra.ITSpanConsumer {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Override protected void blockWhileInFlight() {
      CassandraStorageExtension.blockWhileInFlight(storage);
    }

    @Override public void clear() {
      backend.clear(storage);
    }
  }
}
