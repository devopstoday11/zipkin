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
import java.io.IOException;
import java.util.List;
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
import zipkin2.storage.StorageComponent;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.TODAY;
import static zipkin2.storage.cassandra.v1.InternalForTests.writeDependencyLinks;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ITCassandraStorage {
  static final List<String> SEARCH_TABLES = asList(
    Tables.AUTOCOMPLETE_TAGS,
    Tables.REMOTE_SERVICE_NAMES,
    Tables.SPAN_NAMES,
    Tables.SERVICE_NAMES,
    Tables.ANNOTATIONS_INDEX,
    Tables.SERVICE_REMOTE_SERVICE_NAME_INDEX,
    Tables.SERVICE_SPAN_NAME_INDEX,
    Tables.SERVICE_SPAN_NAME_INDEX
  );

  @RegisterExtension CassandraStorageExtension backend =
    new CassandraStorageExtension("openzipkin/zipkin-cassandra:2.21.7");

  @Nested
  class ITTraces extends zipkin2.storage.ITTraces<CassandraStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return backend.newStorageBuilder(testInfo);
    }

    @Override @Test @Disabled("All services query unsupported when combined with other qualifiers")
    public void getTraces_tags() {
    }

    @Override @Test @Disabled("All services query unsupported when combined with other qualifiers")
    public void getTraces_serviceNames() {
    }

    @Override @Test @Disabled("All services query unsupported when combined with other qualifiers")
    public void getTraces_spanName() {
    }

    @Override @Test @Disabled("Duration unsupported") public void getTraces_duration() {
    }

    @Override @Test @Disabled("Duration unsupported") public void getTraces_minDuration() {
    }

    @Override @Test @Disabled("Duration unsupported") public void getTraces_maxDuration() {
    }

    @Override @Test @Disabled("Duration unsupported") public void getTraces_lateDuration() {
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
  class ITLargeTraceTests extends zipkin2.storage.cassandra.v1.ITLargeTraceTests {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
  class ITSearchEnabledFalse extends zipkin2.storage.ITSearchEnabledFalse<CassandraStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
  class ITServiceAndSpanNames extends zipkin2.storage.ITServiceAndSpanNames<CassandraStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
  class ITEnsureSchema extends zipkin2.storage.cassandra.v1.ITEnsureSchema {
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
  class ITSpanConsumer extends zipkin2.storage.cassandra.v1.ITSpanConsumer {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
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
