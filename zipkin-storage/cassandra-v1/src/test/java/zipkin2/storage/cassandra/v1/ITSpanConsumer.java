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
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.storage.ITStorage;
import zipkin2.storage.StorageComponent;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.FRONTEND;

abstract class ITSpanConsumer extends ITStorage<CassandraStorage> {
  @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
    storage.autocompleteKeys(asList("environment"));
  }

  /**
   * Core/Boundary annotations like "sr" aren't queryable, and don't add value to users. Address
   * annotations, like "sa", don't have string values, so are similarly not queryable. Skipping
   * indexing of such annotations dramatically reduces the load on cassandra and size of indexes.
   */
  @Test public void doesntIndexCoreOrNonStringAnnotations() throws IOException {
    accept(TestObjects.CLIENT_SPAN);

    assertThat(storage.session().execute("SELECT blobastext(annotation) from annotations_index")
      .all())
      .extracting(r -> r.getString(0))
      .containsExactlyInAnyOrder(
        "frontend:http.path",
        "frontend:http.path:/api",
        "frontend:clnt/finagle.version:6.45.0",
        "frontend:foo",
        "frontend:clnt/finagle.version");
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
      .timestamp(trace[0].timestampAsLong() + i * 1000) // all peer span timestamps happen 1ms later
      .duration(10L)
      .build());

    accept(trace);
    assertThat(rowCount(Tables.ANNOTATIONS_INDEX)).isEqualTo(5L);
    // remoteEndpoint was only in the root span
    assertThat(rowCount(Tables.SERVICE_REMOTE_SERVICE_NAME_INDEX)).isEqualTo(1L);

    // For this reason, we expect to see rows for span[0], span[99] and span[100] timestamps.
    assertThat(rowCount(Tables.SERVICE_NAME_INDEX)).isEqualTo(3L);
    assertThat(rowCount(Tables.SERVICE_SPAN_NAME_INDEX)).isEqualTo(3L);

    // redundant store doesn't change the indexes
    accept(trace);
    assertThat(rowCount(Tables.ANNOTATIONS_INDEX)).isEqualTo(5L);
    assertThat(rowCount(Tables.SERVICE_REMOTE_SERVICE_NAME_INDEX)).isEqualTo(1L);
    assertThat(rowCount(Tables.SERVICE_NAME_INDEX)).isEqualTo(3L);
    assertThat(rowCount(Tables.SERVICE_SPAN_NAME_INDEX)).isEqualTo(3L);
  }

  long rowCount(String table) {
    return storage.session()
      .execute("SELECT COUNT(*) from " + table)
      .one()
      .getLong(0);
  }

  static String getTagValue(CassandraStorage storage, String key) {
    return storage
      .session()
      .execute("SELECT value from " + Tables.AUTOCOMPLETE_TAGS + " WHERE key='" + key + "'")
      .one()
      .getString(0);
  }

  @Test public void addsAutocompleteTag() throws IOException {
    Span[] trace = new Span[2];
    trace[0] = TestObjects.CLIENT_SPAN;

    trace[1] = Span.newBuilder()
      .traceId(trace[0].traceId())
      .parentId(trace[0].id())
      .id(1)
      .name("1")
      .putTag("environment", "dev")
      .putTag("a", "b")
      .localEndpoint(FRONTEND)
      .timestamp(trace[0].timestampAsLong() + 1000L) // child span timestamps happen 1ms later
      .addAnnotation(trace[0].annotations().get(0).timestamp() + 1000L, "bar")
      .build();
    accept(trace);

    assertThat(rowCount(Tables.AUTOCOMPLETE_TAGS))
      .isGreaterThanOrEqualTo(1L);

    assertThat(getTagValue(storage, "environment")).isEqualTo("dev");
  }
}
