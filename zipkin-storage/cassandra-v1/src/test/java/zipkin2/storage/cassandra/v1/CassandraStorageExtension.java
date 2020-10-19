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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static zipkin2.Call.propagateIfFatal;

public class CassandraStorageExtension implements BeforeAllCallback, AfterAllCallback {
  static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorageExtension.class);
  static final int CASSANDRA_PORT = 9042;
  final String image;
  CassandraContainer container;
  CqlSession globalSession;

  CassandraStorageExtension(String image) {
    this.image = image;
  }

  @Override public void beforeAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    if (!"true".equals(System.getProperty("docker.skip"))) {
      try {
        LOGGER.info("Starting docker image " + image);
        container = new CassandraContainer(image).withExposedPorts(CASSANDRA_PORT);
        container.start();
      } catch (RuntimeException e) {
        LOGGER.warn("Couldn't start docker image " + image + ": " + e.getMessage(), e);
      }
    } else {
      LOGGER.info("Skipping startup of docker " + image);
    }

    try {
      globalSession = tryToInitializeSession(contactPoint());
    } catch (RuntimeException | Error e) {
      if (container == null) throw e;
      LOGGER.warn("Couldn't connect to docker image " + image + ": " + e.getMessage(), e);
      container.stop();
      container = null; // try with local connection instead
      globalSession = tryToInitializeSession(contactPoint());
    }
  }

  // Builds a session without trying to use a namespace or init UDTs
  static CqlSession tryToInitializeSession(String contactPoint) {
    CassandraStorage storage = newStorageBuilder(contactPoint).build();
    CqlSession session = null;
    try {
      session = SessionFactory.Default.buildSession(storage);
      session.execute("SELECT now() FROM system.local");
    } catch (Throwable e) {
      propagateIfFatal(e);
      if (session != null) session.close();
      assumeTrue(false, e.getMessage());
    }
    return session;
  }

  CassandraStorage.Builder newStorageBuilder(TestInfo testInfo) {
    if (testInfo.getTestMethod().isPresent()) {
      LOGGER.info("Building CassandraStorage for: " + testInfo.getTestMethod().get().getName());
    }
    return CassandraStorage.newBuilder()
      .contactPoints(contactPoint())
      .maxConnections(1)
      .keyspace(InternalForTests.keyspace(testInfo));
  }

  static CassandraStorage.Builder newStorageBuilder(String contactPoint) {
    return CassandraStorage.newBuilder()
      .contactPoints(contactPoint)
      .maxConnections(1)
      .keyspace("test_cassandra_v1");
  }

  String contactPoint() {
    if (container != null && container.isRunning()) {
      return container.getContainerIpAddress() + ":" + container.getMappedPort(CASSANDRA_PORT);
    } else {
      return "127.0.0.1:" + CASSANDRA_PORT;
    }
  }

  @Override public void afterAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }
    if (globalSession != null) globalSession.close();
  }

  static final class CassandraContainer extends GenericContainer<CassandraContainer> {
    CassandraContainer(String image) {
      super(image);
    }

    @Override protected void waitUntilContainerStarted() {
      Unreliables.retryUntilSuccess(120, TimeUnit.SECONDS, () -> {
        if (!isRunning()) throw new ContainerLaunchException("Container failed to start");

        String contactPoint = getContainerIpAddress() + ":" + getMappedPort(9042);
        try (CqlSession session = tryToInitializeSession(contactPoint)) {
          session.execute("SELECT now() FROM system.local");
          logger().info("Obtained a connection to container ({})", contactPoint);
          return null; // unused value
        }
      });
    }
  }
}
