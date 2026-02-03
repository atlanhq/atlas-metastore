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
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgreSQLKeyColumnValueStore implements KeyColumnValueStore {

    private final String storeName;
    private final String qualifiedTable;

    public PostgreSQLKeyColumnValueStore(String storeName, String qualifiedTable) {
        this.storeName = storeName;
        this.qualifiedTable = qualifiedTable;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        String sql = "SELECT column_bytes, value_bytes FROM " + qualifiedTable +
            " WHERE store_name = ? AND key_bytes = ? AND column_bytes >= ? AND column_bytes < ?" +
            " ORDER BY column_bytes ASC LIMIT ?";

        try (PreparedStatement ps = getConnection(txh).prepareStatement(sql)) {
            ps.setString(1, storeName);
            ps.setBytes(2, asBytes(query.getKey()));
            ps.setBytes(3, asBytes(query.getSliceStart()));
            ps.setBytes(4, asBytes(query.getSliceEnd()));
            ps.setInt(5, query.getLimit());

            List<Entry> entries = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byte[] column = rs.getBytes(1);
                    byte[] value = rs.getBytes(2);
                    entries.add(StaticArrayEntry.of(new StaticArrayBuffer(column), new StaticArrayBuffer(value)));
                }
            }
            return EntryArrayList.of(entries);
        } catch (SQLException e) {
            throw new PermanentBackendException("Failed to read slice from PostgreSQL", e);
        }
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = new HashMap<>(keys.size());
        for (StaticBuffer key : keys) {
            result.put(key, getSlice(new KeySliceQuery(key, query), txh));
        }
        return result;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        Connection connection = getConnection(txh);
        if (deletions != null && !deletions.isEmpty()) {
            String deleteSql = "DELETE FROM " + qualifiedTable +
                " WHERE store_name = ? AND key_bytes = ? AND column_bytes = ?";
            try (PreparedStatement ps = connection.prepareStatement(deleteSql)) {
                for (StaticBuffer column : deletions) {
                    ps.setString(1, storeName);
                    ps.setBytes(2, asBytes(key));
                    ps.setBytes(3, asBytes(column));
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new PermanentBackendException("Failed to delete from PostgreSQL", e);
            }
        }

        if (additions != null && !additions.isEmpty()) {
            String insertSql = "INSERT INTO " + qualifiedTable +
                " (store_name, key_bytes, column_bytes, value_bytes) VALUES (?, ?, ?, ?)" +
                " ON CONFLICT (store_name, key_bytes, column_bytes)" +
                " DO UPDATE SET value_bytes = EXCLUDED.value_bytes";
            try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
                for (Entry entry : additions) {
                    ps.setString(1, storeName);
                    ps.setBytes(2, asBytes(key));
                    ps.setBytes(3, asBytes(entry.getColumnAs(StaticBuffer.STATIC_FACTORY)));
                    ps.setBytes(4, asBytes(entry.getValueAs(StaticBuffer.STATIC_FACTORY)));
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new PermanentBackendException("Failed to insert into PostgreSQL", e);
            }
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("PostgreSQL backend does not support explicit locking");
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("PostgreSQL backend does not support ordered scans");
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        String sql = "SELECT DISTINCT key_bytes FROM " + qualifiedTable +
            " WHERE store_name = ? AND column_bytes >= ? AND column_bytes < ? ORDER BY key_bytes";
        List<StaticBuffer> keys = new ArrayList<>();
        try (PreparedStatement ps = getConnection(txh).prepareStatement(sql)) {
            ps.setString(1, storeName);
            ps.setBytes(2, asBytes(query.getSliceStart()));
            ps.setBytes(3, asBytes(query.getSliceEnd()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    keys.add(new StaticArrayBuffer(rs.getBytes(1)));
                }
            }
        } catch (SQLException e) {
            throw new PermanentBackendException("Failed to scan keys from PostgreSQL", e);
        }

        return new SimpleKeyIterator(keys, query, txh, this);
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("PostgreSQL backend does not support multi-slice scans");
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() throws BackendException {
        // No resources to close per-store
    }

    private Connection getConnection(StoreTransaction txh) {
        return ((PostgreSQLTransaction) txh).getConnection();
    }

    private static byte[] asBytes(StaticBuffer buffer) {
        return buffer.as(StaticBuffer.ARRAY_FACTORY);
    }

    private static final class SimpleKeyIterator implements KeyIterator {
        private final java.util.Iterator<StaticBuffer> iterator;
        private final SliceQuery query;
        private final StoreTransaction txh;
        private final PostgreSQLKeyColumnValueStore store;
        private StaticBuffer current;

        private SimpleKeyIterator(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh, PostgreSQLKeyColumnValueStore store) {
            this.iterator = keys.iterator();
            this.query = query;
            this.txh = txh;
            this.store = store;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public StaticBuffer next() {
            current = iterator.next();
            return current;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            EntryList entries;
            try {
                entries = store.getSlice(new KeySliceQuery(current, query), txh);
            } catch (BackendException e) {
                throw new RuntimeException("Failed to read entries for key scan", e);
            }
            java.util.Iterator<Entry> entryIterator = entries.iterator();
            return new RecordIterator<Entry>() {
                @Override
                public boolean hasNext() {
                    return entryIterator.hasNext();
                }

                @Override
                public Entry next() {
                    return entryIterator.next();
                }

                @Override
                public void close() {
                    // No-op
                }
            };
        }

        @Override
        public void close() {
            // No-op
        }
    }
}
