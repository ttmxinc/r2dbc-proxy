/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.proxy.callback;

import io.r2dbc.proxy.ProxyConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link StatementCallbackHandler} with a batch built by calling
 * {@link Statement#add()} before binding each parameter set.
 */
class StatementCallbackHandlerIntegrationTest {

    private static final String R2DBC_URL = "r2dbc:h2:mem://sa@/add-before-bind;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";

    private final ConnectionFactory connectionFactory = ConnectionFactories.get(R2DBC_URL);

    @BeforeEach
    void setUp() {
        withConnection(connection -> Flux.concat(
                Flux.from(connection.createStatement("DROP TABLE IF EXISTS emp").execute()).flatMap(Result::getRowsUpdated),
                Flux.from(connection.createStatement("CREATE TABLE emp(id INT, name VARCHAR(20))").execute()).flatMap(Result::getRowsUpdated)
            ).then(),
            this.connectionFactory);
    }

    @Test
    void addBeforeEachBindingsWithoutProxy() {
        // control: the driver itself accepts this call order
        insertWithAddBeforeEachBindings(this.connectionFactory);

        assertThat(selectNames()).containsExactly("Foo", "Bar");
    }

    @Test
    void addBeforeEachBindingsWithProxy() {
        ConnectionFactory proxyConnectionFactory = ProxyConnectionFactory.builder(this.connectionFactory).build();

        insertWithAddBeforeEachBindings(proxyConnectionFactory);

        assertThat(selectNames()).containsExactly("Foo", "Bar");
    }

    private void insertWithAddBeforeEachBindings(ConnectionFactory factory) {
        withConnection(connection -> {
            Statement statement = connection.createStatement("INSERT INTO emp VALUES ($1, $2)");
            statement.add().bind("$1", 10).bind("$2", "Foo");
            statement.add().bind("$1", 20).bind("$2", "Bar");
            return Flux.from(statement.execute()).flatMap(Result::getRowsUpdated).then();
        }, factory);
    }

    private List<String> selectNames() {
        return Mono.usingWhen(this.connectionFactory.create(),
                connection -> Flux.from(connection.createStatement("SELECT name FROM emp ORDER BY id").execute())
                    .flatMap(result -> result.map((row, metadata) -> row.get("name", String.class)))
                    .collectList(),
                Connection::close)
            .block();
    }

    private static void withConnection(Function<Connection, Mono<Void>> action, ConnectionFactory factory) {
        Mono.usingWhen(factory.create(), action, Connection::close).block();
    }

}
