// Copyright 2026 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.postgresql;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the PostgreSQL storage backend. These are managed under the 'postgresql' namespace.
 */
@PreInitializeConfigOptions
public interface PostgreSQLConfigOptions {

    ConfigNamespace POSTGRESQL_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "postgresql",
        "PostgreSQL storage backend options");

    ConfigOption<String> JDBC_URL = new ConfigOption<>(
        POSTGRESQL_NS,
        "url",
        "JDBC URL for PostgreSQL. If set, it overrides host/port/database.",
        ConfigOption.Type.LOCAL,
        "");

    ConfigOption<String> HOST = new ConfigOption<>(
        POSTGRESQL_NS,
        "host",
        "PostgreSQL host.",
        ConfigOption.Type.LOCAL,
        "localhost");

    ConfigOption<Integer> PORT = new ConfigOption<>(
        POSTGRESQL_NS,
        "port",
        "PostgreSQL port.",
        ConfigOption.Type.LOCAL,
        5432);

    ConfigOption<String> DATABASE = new ConfigOption<>(
        POSTGRESQL_NS,
        "database",
        "PostgreSQL database name.",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    ConfigOption<String> USERNAME = new ConfigOption<>(
        POSTGRESQL_NS,
        "username",
        "PostgreSQL username.",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    ConfigOption<String> PASSWORD = new ConfigOption<>(
        POSTGRESQL_NS,
        "password",
        "PostgreSQL password.",
        ConfigOption.Type.LOCAL,
        "");

    ConfigOption<String> SCHEMA = new ConfigOption<>(
        POSTGRESQL_NS,
        "schema",
        "PostgreSQL schema for JanusGraph tables.",
        ConfigOption.Type.LOCAL,
        "public");

    ConfigOption<String> TABLE = new ConfigOption<>(
        POSTGRESQL_NS,
        "table",
        "PostgreSQL table name for key-column-value data.",
        ConfigOption.Type.LOCAL,
        "janusgraph_kcvs");

    ConfigOption<Integer> POOL_MAX_SIZE = new ConfigOption<>(
        POSTGRESQL_NS,
        "pool_max_size",
        "Maximum number of connections in the PostgreSQL connection pool.",
        ConfigOption.Type.LOCAL,
        128);

    ConfigOption<Integer> POOL_MIN_IDLE = new ConfigOption<>(
        POSTGRESQL_NS,
        "pool_min_idle",
        "Minimum number of idle connections kept in the PostgreSQL connection pool.",
        ConfigOption.Type.LOCAL,
        4);

    ConfigOption<Long> POOL_CONNECTION_TIMEOUT_MS = new ConfigOption<>(
        POSTGRESQL_NS,
        "pool_connection_timeout_ms",
        "Maximum time to wait for a connection from the PostgreSQL pool.",
        ConfigOption.Type.LOCAL,
        30000L);

    ConfigOption<Long> POOL_IDLE_TIMEOUT_MS = new ConfigOption<>(
        POSTGRESQL_NS,
        "pool_idle_timeout_ms",
        "Maximum idle time for connections in the PostgreSQL pool.",
        ConfigOption.Type.LOCAL,
        600000L);

    ConfigOption<Long> POOL_MAX_LIFETIME_MS = new ConfigOption<>(
        POSTGRESQL_NS,
        "pool_max_lifetime_ms",
        "Maximum lifetime for a connection in the PostgreSQL pool.",
        ConfigOption.Type.LOCAL,
        1800000L);
}
