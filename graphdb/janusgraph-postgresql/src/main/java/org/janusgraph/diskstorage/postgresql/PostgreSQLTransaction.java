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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import java.sql.Connection;
import java.sql.SQLException;

public class PostgreSQLTransaction extends AbstractStoreTransaction {

    private final Connection connection;

    public PostgreSQLTransaction(Connection connection, BaseTransactionConfig config) {
        super(config);
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public void commit() throws BackendException {
        super.commit();
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new PermanentBackendException("Failed to commit PostgreSQL transaction", e);
        } finally {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }

    @Override
    public void rollback() throws BackendException {
        super.rollback();
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new PermanentBackendException("Failed to rollback PostgreSQL transaction", e);
        } finally {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }
}
