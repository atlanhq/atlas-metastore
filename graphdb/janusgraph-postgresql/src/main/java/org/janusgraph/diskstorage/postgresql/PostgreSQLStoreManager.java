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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.DATABASE;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.HOST;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.JDBC_URL;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.PASSWORD;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.POOL_CONNECTION_TIMEOUT_MS;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.POOL_IDLE_TIMEOUT_MS;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.POOL_MAX_LIFETIME_MS;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.POOL_MAX_SIZE;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.POOL_MIN_IDLE;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.PORT;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.SCHEMA;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.TABLE;
import static org.janusgraph.diskstorage.postgresql.PostgreSQLConfigOptions.USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;

public class PostgreSQLStoreManager implements KeyColumnValueStoreManager {

    private final ConcurrentHashMap<String, PostgreSQLKeyColumnValueStore> stores = new ConcurrentHashMap<>();
    private final StoreFeatures features;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String schema;
    private final String table;
    private final String qualifiedTable;
    private final HikariDataSource dataSource;

    public PostgreSQLStoreManager() {
        this(Configuration.EMPTY);
    }

    public PostgreSQLStoreManager(Configuration configuration) {
        String configuredUrl = configuration.getOrDefault(JDBC_URL);
        if (configuredUrl != null && !configuredUrl.trim().isEmpty()) {
            this.jdbcUrl = configuredUrl.trim();
        } else {
            String host = configuration.getOrDefault(HOST);
            if (!configuration.has(HOST) && configuration.has(STORAGE_HOSTS)) {
                String[] hosts = configuration.get(STORAGE_HOSTS);
                if (hosts != null && hosts.length > 0) {
                    host = hosts[0];
                }
            }
            Integer port = configuration.getOrDefault(PORT);
            if (!configuration.has(PORT) && configuration.has(STORAGE_PORT)) {
                port = configuration.getOrDefault(STORAGE_PORT);
            }
            String database = configuration.getOrDefault(DATABASE);
            this.jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        }
        if (configuration.has(USERNAME)) {
            this.username = configuration.getOrDefault(USERNAME);
        } else if (configuration.has(AUTH_USERNAME)) {
            this.username = configuration.getOrDefault(AUTH_USERNAME);
        } else {
            this.username = configuration.getOrDefault(USERNAME);
        }
        if (configuration.has(PASSWORD)) {
            this.password = configuration.getOrDefault(PASSWORD);
        } else if (configuration.has(AUTH_PASSWORD)) {
            this.password = configuration.getOrDefault(AUTH_PASSWORD);
        } else {
            this.password = configuration.getOrDefault(PASSWORD);
        }
        this.schema = configuration.getOrDefault(SCHEMA);
        this.table = configuration.getOrDefault(TABLE);
        this.qualifiedTable = quoteIdentifier(schema) + "." + quoteIdentifier(table);

        this.dataSource = buildDataSource(configuration);

        try {
            initializeSchemaAndTable();
        } catch (PermanentBackendException e) {
            dataSource.close();
            throw new JanusGraphException("Failed to initialize PostgreSQL backend", e);
        }

        features = new StandardStoreFeatures.Builder()
            .orderedScan(false)
            .unorderedScan(true)
            .consistentScan(true)
            .keyOrdered(false)
            .multiQuery(true)
            .batchMutation(true)
            .locking(false)
            .distributed(false)
            .transactional(true)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .persists(true)
            .optimisticLocking(false)
            .build();
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            return new PostgreSQLTransaction(connection, config);
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
            throw new PermanentBackendException("Could not open PostgreSQL transaction", e);
        }
    }

    @Override
    public void close() throws BackendException {
        stores.clear();
        dataSource.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM " + qualifiedTable);
        } catch (SQLException e) {
            throw new PermanentBackendException("Failed to clear PostgreSQL storage", e);
        }
        stores.clear();
    }

    @Override
    public boolean exists() throws BackendException {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData meta = connection.getMetaData();
            try (ResultSet tables = meta.getTables(null, schema, table, new String[]{"TABLE"})) {
                if (!tables.next()) {
                    return false;
                }
            }
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1 FROM " + qualifiedTable + " LIMIT 1")) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new PermanentBackendException("Failed to check PostgreSQL storage existence", e);
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new PostgreSQLKeyColumnValueStore(name, qualifiedTable));
        }
        KeyColumnValueStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMut : mutations.entrySet()) {
            KeyColumnValueStore store = stores.get(storeMut.getKey());
            Preconditions.checkNotNull(store);
            for (Map.Entry<StaticBuffer, KCVMutation> keyMut : storeMut.getValue().entrySet()) {
                store.mutate(keyMut.getKey(), keyMut.getValue().getAdditions(), keyMut.getValue().getDeletions(), txh);
            }
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return jdbcUrl;
    }

    Connection getConnection(StoreTransaction txh) {
        return ((PostgreSQLTransaction) txh).getConnection();
    }

    String getQualifiedTable() {
        return qualifiedTable;
    }

    private void initializeSchemaAndTable() throws PermanentBackendException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(schema));
            statement.execute("CREATE TABLE IF NOT EXISTS " + qualifiedTable + " (" +
                "store_name TEXT NOT NULL, " +
                "key_bytes BYTEA NOT NULL, " +
                "column_bytes BYTEA NOT NULL, " +
                "value_bytes BYTEA NOT NULL, " +
                "PRIMARY KEY (store_name, key_bytes, column_bytes)" +
                ")");
        } catch (SQLException e) {
            throw new PermanentBackendException("Failed to initialize PostgreSQL schema", e);
        }
    }

    private static String quoteIdentifier(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private HikariDataSource buildDataSource(Configuration configuration) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        if (username != null) {
            hikariConfig.setUsername(username);
        }
        if (password != null) {
            hikariConfig.setPassword(password);
        }
        hikariConfig.setPoolName("janusgraph-postgresql");
        int maxPoolSize = Math.max(1, configuration.getOrDefault(POOL_MAX_SIZE));
        int minIdle = Math.max(0, configuration.getOrDefault(POOL_MIN_IDLE));
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setMinimumIdle(Math.min(minIdle, maxPoolSize));
        hikariConfig.setConnectionTimeout(Math.max(250L, configuration.getOrDefault(POOL_CONNECTION_TIMEOUT_MS)));
        hikariConfig.setIdleTimeout(Math.max(1000L, configuration.getOrDefault(POOL_IDLE_TIMEOUT_MS)));
        hikariConfig.setMaxLifetime(Math.max(1000L, configuration.getOrDefault(POOL_MAX_LIFETIME_MS)));
        return new HikariDataSource(hikariConfig);
    }
}
