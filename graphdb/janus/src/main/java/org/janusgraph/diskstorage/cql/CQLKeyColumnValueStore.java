// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.vavr.collection.Array;
import io.vavr.collection.Iterator;
import io.vavr.control.Try;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLColValGetter;
import org.janusgraph.diskstorage.cql.CQLResultSetKeyIterator;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.diskstorage.cql.service.AsyncQueryExecutionService;
import org.janusgraph.diskstorage.cql.service.GroupingAsyncQueryExecutionService;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.CompletableFutureUtil;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;
import static com.datastax.oss.driver.api.querybuilder.select.Selector.column;
import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;
import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.*;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORE_META_TIMESTAMPS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORE_META_TTL;

/**
 * An implementation of {@link KeyColumnValueStore} which stores the data in a CQL connected backend.
 */
public class CQLKeyColumnValueStore implements KeyColumnValueStore {

    public static final String TTL_FUNCTION_NAME = "ttl";
    public static final String WRITETIME_FUNCTION_NAME = "writetime";

    public static final String KEY_COLUMN_NAME = "key";
    public static final String COLUMN_COLUMN_NAME = "column1";
    public static final String VALUE_COLUMN_NAME = "value";
    public static final String WRITETIME_COLUMN_NAME = "writetime";
    public static final String TTL_COLUMN_NAME = "ttl";

    public static final String KEY_BINDING = "key";
    public static final String COLUMN_BINDING = "column1";
    public static final String VALUE_BINDING = "value";
    public static final String TIMESTAMP_BINDING = "timestamp";
    public static final String TTL_BINDING = "ttl";
    public static final String SLICE_START_BINDING = "sliceStart";
    public static final String SLICE_END_BINDING = "sliceEnd";
    public static final String KEY_START_BINDING = "keyStart";
    public static final String KEY_END_BINDING = "keyEnd";
    public static final String LIMIT_BINDING = "maxRows";
    public static final String ENTRIES_COLUMN_NAME = "grouped_entries";
    public static final String VERSION_COLUMN_NAME = "grouped_version";
    public static final String ENTRIES_BINDING = "groupedEntries";
    public static final String VERSION_BINDING = "version";
    public static final String PREVIOUS_VERSION_BINDING = "previousVersion";
    private static final String APPLIED_COLUMN_NAME = "[applied]";
    private static final String GROUPED_TABLE_SUFFIX = "_grouped";
    private static final int MAX_GROUPED_MUTATION_RETRIES = 20;

    public static final Function<? super Throwable, BackendException> EXCEPTION_MAPPER = cause -> {
        cause = CompletableFutureUtil.unwrapExecutionException(cause);
        if(cause instanceof InterruptedException || cause.getCause() instanceof InterruptedException){
            Thread.currentThread().interrupt();
            return new PermanentBackendException(cause instanceof InterruptedException ? cause : cause.getCause());
        }
        if(cause instanceof BackendException){
            return (BackendException) cause;
        }
        return Match(cause).of(
            Case($(instanceOf(QueryValidationException.class)), PermanentBackendException::new),
            Case($(CQLKeyColumnValueStore::isKeySizeTooLarge), PermanentBackendException::new),
            Case($(), TemporaryBackendException::new));
    };

    /**
     * CQL keys are limited to 64k, and a query that attempts to filter on such
     * a key with a value that exceeds that length will produce a ServerError
     * on ScyllaDB, perhaps due to a bug, rather than an InvalidQueryException.
     * This type of ServerError is a permanent failure, since the same query with
     * the same value will always produce this error. Properly treating this as a
     * permanent failure avoids excessive retrying of bad CQL queries and allows
     * upstream code to properly report the exception.
     */
    private static boolean isKeySizeTooLarge(Throwable cause) {
        AllNodesFailedException failed = ExceptionUtils.throwableOfType(cause, AllNodesFailedException.class);
        return failed != null && failed.getAllErrors().values().stream().anyMatch(errors ->
            errors.stream().anyMatch(error -> (error instanceof ServerError)
                && error.getMessage().startsWith("Key size too large:")));
    }

    private final CQLStoreManager storeManager;
    private final CqlSession session;
    private final String tableName;
    private final boolean groupedStore;
    private final String groupedTableName;
    private final CQLColValGetter singleKeyGetter;
    private final CQLColValGetter multiKeysGetter;

    private final Runnable closer;

    private final PreparedStatement getKeysAll;
    private final PreparedStatement getKeysRanged;
    private final PreparedStatement deleteColumn;
    private final PreparedStatement insertColumn;
    private final PreparedStatement insertColumnWithTTL;
    private final PreparedStatement getLegacyAllColumnsByKey;
    private final PreparedStatement getGroupedEntriesByKey;
    private final PreparedStatement insertGroupedEntriesIfNotExists;
    private final PreparedStatement updateGroupedEntriesIfVersionMatches;

    private final QueryBackPressure queryBackPressure;
    private final AsyncQueryExecutionService asyncQueryExecutionService;

    /**
     * Creates an instance of the {@link KeyColumnValueStore} that stores the data in a CQL backed table.
     *
     * @param storeManager the {@link CQLStoreManager} that maintains the list of {@link CQLKeyColumnValueStore}s
     * @param tableName the name of the database table for storing the key/column/values
     * @param configuration data used in creating this store
     * @param closer callback used to clean up references to this store in the store manager
     */
    public CQLKeyColumnValueStore(final CQLStoreManager storeManager, final String tableName, final Configuration configuration, final Runnable closer) {
        this.storeManager = storeManager;

        this.tableName = tableName;
        this.groupedStore = EDGESTORE_NAME.equals(tableName);
        this.groupedTableName = groupedStore ? tableName + GROUPED_TABLE_SUFFIX : null;
        this.closer = closer;
        this.session = this.storeManager.getSession();
        EntryMetaData[] defaultEntryMetadata = storeManager.getMetaDataSchema(this.tableName);
        this.singleKeyGetter = new CQLColValGetter(defaultEntryMetadata);
        EntryMetaData[] multiKeyEntryMetadata = new EntryMetaData[defaultEntryMetadata.length+1];
        System.arraycopy(defaultEntryMetadata,0,multiKeyEntryMetadata,0, defaultEntryMetadata.length);
        multiKeyEntryMetadata[multiKeyEntryMetadata.length-1] = EntryMetaData.ROW_KEY;
        this.multiKeysGetter = new CQLColValGetter(multiKeyEntryMetadata);

        if(shouldInitializeTable()) {
            initializeTable(this.session, this.storeManager.getKeyspaceName(), tableName, configuration);
            if (configuration.has(INIT_WAIT_TIME) && configuration.get(INIT_WAIT_TIME) > 0) {
                try {
                    Thread.sleep(configuration.get(INIT_WAIT_TIME));
                } catch (InterruptedException e) {
                    throw new JanusGraphException("Interrupted while waiting for table initialization to complete", e);
                }
            }
        }

        if (groupedStore && shouldInitializeTable(groupedTableName)) {
            initializeGroupedTable(this.session, this.storeManager.getKeyspaceName(), groupedTableName, configuration);
            if (configuration.has(INIT_WAIT_TIME) && configuration.get(INIT_WAIT_TIME) > 0) {
                try {
                    Thread.sleep(configuration.get(INIT_WAIT_TIME));
                } catch (InterruptedException e) {
                    throw new JanusGraphException("Interrupted while waiting for grouped table initialization to complete", e);
                }
            }
        }
        if (this.storeManager.getFeatures().hasOrderedScan()) {
            final Select getKeysRangedSelect = selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .allowFiltering()
                .where(
                    Relation.token(KEY_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(KEY_START_BINDING)),
                    Relation.token(KEY_COLUMN_NAME).isLessThan(bindMarker(KEY_END_BINDING))
                )
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThanOrEqualTo(bindMarker(SLICE_END_BINDING));
            this.getKeysRanged = this.session.prepare(addTTLFunction(addTimestampFunction(getKeysRangedSelect)).build());
        } else {
            this.getKeysRanged = null;
        }

        if (this.storeManager.getFeatures().hasUnorderedScan()) {
            final Select getKeysAllSelect = selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .allowFiltering()
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThanOrEqualTo(bindMarker(SLICE_END_BINDING));
            this.getKeysAll = this.session.prepare(addTTLFunction(addTimestampFunction(getKeysAllSelect)).build());
        } else {
            this.getKeysAll = null;
        }

        final DeleteSelection deleteSelection = addUsingTimestamp(deleteFrom(this.storeManager.getKeyspaceName(), this.tableName));
        this.deleteColumn = this.session.prepare(deleteSelection
                .whereColumn(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isEqualTo(bindMarker(COLUMN_BINDING))
                .build());

        final Insert insertColumnInsert = addUsingTimestamp(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING)));
        this.insertColumn = this.session.prepare(insertColumnInsert.build());

        if (storeManager.getFeatures().hasCellTTL()) {
            this.insertColumnWithTTL = this.session.prepare(insertColumnInsert.usingTtl(bindMarker(TTL_BINDING)).build());
        } else {
            this.insertColumnWithTTL = null;
        }

        this.getLegacyAllColumnsByKey = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
            .column(COLUMN_COLUMN_NAME)
            .column(VALUE_COLUMN_NAME)
            .whereColumn(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING))
            .build());

        if (groupedStore) {
            this.getGroupedEntriesByKey = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.groupedTableName)
                .column(ENTRIES_COLUMN_NAME)
                .column(VERSION_COLUMN_NAME)
                .whereColumn(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING))
                .build());
            this.insertGroupedEntriesIfNotExists = this.session.prepare(insertInto(this.storeManager.getKeyspaceName(), this.groupedTableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(ENTRIES_COLUMN_NAME, bindMarker(ENTRIES_BINDING))
                .value(VERSION_COLUMN_NAME, bindMarker(VERSION_BINDING))
                .ifNotExists()
                .build());
            this.updateGroupedEntriesIfVersionMatches = this.session.prepare(update(this.storeManager.getKeyspaceName(), this.groupedTableName)
                .setColumn(ENTRIES_COLUMN_NAME, bindMarker(ENTRIES_BINDING))
                .setColumn(VERSION_COLUMN_NAME, bindMarker(VERSION_BINDING))
                .whereColumn(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING))
                .ifColumn(VERSION_COLUMN_NAME).isEqualTo(bindMarker(PREVIOUS_VERSION_BINDING))
                .build());
        } else {
            this.getGroupedEntriesByKey = null;
            this.insertGroupedEntriesIfNotExists = null;
            this.updateGroupedEntriesIfVersionMatches = null;
        }

        queryBackPressure = storeManager.getQueriesBackPressure();

        asyncQueryExecutionService = new GroupingAsyncQueryExecutionService(configuration, storeManager, tableName,
            this::addTTLFunction, this::addTimestampFunction, singleKeyGetter, multiKeysGetter);

        // @formatter:on
    }

    private DeleteSelection addUsingTimestamp(DeleteSelection deleteSelection) {
        if (storeManager.isAssignTimestamp()) {
            return deleteSelection.usingTimestamp(bindMarker(TIMESTAMP_BINDING));
        }
        return deleteSelection;
    }

    private Insert addUsingTimestamp(Insert insert) {
        if (storeManager.isAssignTimestamp()) {
            return insert.usingTimestamp(bindMarker(TIMESTAMP_BINDING));
        }
        return insert;
    }

    /**
     * Add WRITETIME function into the select query to retrieve the timestamp that the data was written to the database,
     * if {@link STORE_META_TIMESTAMPS} is enabled.
     * @param select original query
     * @return new query
     */
    private Select addTimestampFunction(Select select) {
        if (storeManager.getStorageConfig().get(STORE_META_TIMESTAMPS, this.tableName)) {
            return select.function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME);
        }
        return select;
    }

    /**
     * Add TTL function into the select query to retrieve how much longer the data is going to live, if {@link STORE_META_TTL}
     * is enabled.
     * @param select original query
     * @return new query
     */
    private Select addTTLFunction(Select select) {
        if (storeManager.getStorageConfig().get(STORE_META_TTL, this.tableName)) {
            return select.function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME);
        }
        return select;
    }

    /**
     * Check if the current table should be initialized.
     * NOTE: This additional check is needed when Cassandra security is enabled, for more info check issue #1103
     * @return true if table already exists in current keyspace, false otherwise
     */
    private boolean shouldInitializeTable() {
        return storeManager.getSession().getMetadata()
            .getKeyspace(storeManager.getKeyspaceName()).map(k -> !k.getTable(this.tableName).isPresent())
            .orElse(true);
    }

    private boolean shouldInitializeTable(String targetTableName) {
        if (targetTableName == null) {
            return false;
        }
        return storeManager.getSession().getMetadata()
            .getKeyspace(storeManager.getKeyspaceName()).map(k -> !k.getTable(targetTableName).isPresent())
            .orElse(true);
    }

    private static void initializeTable(final CqlSession session, final String keyspaceName, final String tableName, final Configuration configuration) {
        CreateTableWithOptions createTable = createTable(keyspaceName, tableName)
                .ifNotExists()
                .withPartitionKey(KEY_COLUMN_NAME, DataTypes.BLOB)
                .withClusteringColumn(COLUMN_COLUMN_NAME, DataTypes.BLOB)
                .withColumn(VALUE_COLUMN_NAME, DataTypes.BLOB);

        createTable = compactionOptions(createTable, configuration);
        createTable = compressionOptions(createTable, configuration);
        createTable = gcGraceSeconds(createTable, configuration);
        createTable = speculativeRetryOptions(createTable, configuration);

        session.execute(createTable.build());
    }

    private static void initializeGroupedTable(final CqlSession session, final String keyspaceName, final String tableName, final Configuration configuration) {
        CreateTableWithOptions createTable = createTable(keyspaceName, tableName)
            .ifNotExists()
            .withPartitionKey(KEY_COLUMN_NAME, DataTypes.BLOB)
            .withColumn(ENTRIES_COLUMN_NAME, DataTypes.listOf(DataTypes.BLOB, true))
            .withColumn(VERSION_COLUMN_NAME, DataTypes.BIGINT);

        createTable = compactionOptions(createTable, configuration);
        createTable = compressionOptions(createTable, configuration);
        createTable = gcGraceSeconds(createTable, configuration);
        createTable = speculativeRetryOptions(createTable, configuration);

        session.execute(createTable.build());
    }

    private static CreateTableWithOptions compressionOptions(final CreateTableWithOptions createTable,
                                                             final Configuration configuration) {
        if (!configuration.get(CF_COMPRESSION)) {
            // No compression
            return createTable.withNoCompression();
        }

        String compressionType = configuration.get(CF_COMPRESSION_TYPE);
        int chunkLengthInKb = configuration.get(CF_COMPRESSION_BLOCK_SIZE);

        return createTable.withOption("compression",
            ImmutableMap.of("sstable_compression", compressionType, "chunk_length_kb", chunkLengthInKb));
    }

    static CreateTableWithOptions compactionOptions(final CreateTableWithOptions createTable,
                                                    final Configuration configuration) {
        if (!configuration.has(COMPACTION_STRATEGY)) {
            return createTable;
        }

        CompactionStrategy<?> compactionStrategy = Match(configuration.get(COMPACTION_STRATEGY))
            .of(
                Case($("SizeTieredCompactionStrategy"), sizeTieredCompactionStrategy()),
                Case($("TimeWindowCompactionStrategy"), timeWindowCompactionStrategy()),
                Case($("LeveledCompactionStrategy"), leveledCompactionStrategy()));

        if (configuration.has(COMPACTION_OPTIONS)) {
            Iterator<Array<String>> groupedOptions = Array.of(configuration.get(COMPACTION_OPTIONS))
                                                          .grouped(2);
            for (Array<String> keyValue : groupedOptions) {
                compactionStrategy = compactionStrategy.withOption(keyValue.get(0), keyValue.get(1));
            }
        }
        return createTable.withCompaction(compactionStrategy);
    }

    private static CreateTableWithOptions gcGraceSeconds(final CreateTableWithOptions createTable,
                                                         final Configuration configuration) {
        if (!configuration.has(GC_GRACE_SECONDS)) {
            return createTable;
        }
        return createTable.withGcGraceSeconds(configuration.get(GC_GRACE_SECONDS));
    }

    private static CreateTableWithOptions speculativeRetryOptions(final CreateTableWithOptions createTable,
                                                                  final Configuration configuration) {
        if (!configuration.has(SPECULATIVE_RETRY)) {
            return createTable;
        }
        return createTable.withSpeculativeRetry(configuration.get(SPECULATIVE_RETRY));
    }

    @Override
    public void close() throws BackendException {
        this.closer.run();
    }

    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        if (groupedStore) {
            Optional<List<Entry>> groupedEntries = fetchGroupedEntries(query.getKey(), txh);
            if (groupedEntries.isPresent()) {
                return filterBySlice(groupedEntries.get(), query);
            }
        }
        try {
            return asyncQueryExecutionService.executeSingleKeySingleSlice(query, txh).get();
        } catch (Throwable throwable){
            throw EXCEPTION_MAPPER.apply(throwable);
        }
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        if (groupedStore) {
            final Map<StaticBuffer, EntryList> result = new HashMap<>(keys.size());
            for (StaticBuffer key : keys) {
                result.put(key, getSlice(new KeySliceQuery(key, query), txh));
            }
            return result;
        }
        try {
            return CompletableFutureUtil.unwrap(asyncQueryExecutionService.executeMultiKeySingleSlice(keys, query, txh));
        } catch (Throwable e) {
            throw EXCEPTION_MAPPER.apply(e);
        }
    }

    // This implementation is better optimized than `KeyColumnValueStoreUtil.getMultiRangeSliceNonOptimized(this, multiSliceQueriesForKeys, txh);`
    // because it sends all slice queries in parallel instead of using blocking calls to
    // `getSlice(final Collection<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh)`.
    // Moreover, if `sliceGroupingAllowed` is `true` it may also group some of the Slice queries together and perform
    // IN operator instead of multiple range queries.
    @Override
    public Map<SliceQuery, Map<StaticBuffer, EntryList>> getMultiSlices(final MultiKeysQueryGroups<StaticBuffer, SliceQuery> multiSliceQueriesForKeys, StoreTransaction txh) throws BackendException {
        if (groupedStore) {
            final Map<SliceQuery, Map<StaticBuffer, EntryList>> result = new HashMap<>(multiSliceQueriesForKeys.getMultiQueryContext().getTotalAmountOfQueries());
            for (org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup<StaticBuffer, SliceQuery> group : multiSliceQueriesForKeys.getQueryGroups()) {
                for (SliceQuery query : group.getQueries()) {
                    final Map<StaticBuffer, EntryList> perKey = result.computeIfAbsent(query, q -> new HashMap<>(group.getKeysGroup().size()));
                    for (StaticBuffer key : group.getKeysGroup()) {
                        perKey.put(key, getSlice(new KeySliceQuery(key, query), txh));
                    }
                }
            }
            return result;
        }
        try {
            return CompletableFutureUtil.unwrapMapOfMaps(asyncQueryExecutionService.executeMultiKeyMultiSlice(multiSliceQueriesForKeys, txh));
        } catch (Throwable e) {
            throw EXCEPTION_MAPPER.apply(e);
        }
    }

    public BatchableStatement<BoundStatement> deleteColumn(final StaticBuffer key, final StaticBuffer column) {
        return deleteColumn(key, column, null);
    }

    public BatchableStatement<BoundStatement> deleteColumn(final StaticBuffer key, final StaticBuffer column, final Long timestamp) {
        BoundStatementBuilder builder = deleteColumn.boundStatementBuilder()
            .setByteBuffer(KEY_BINDING, key.asByteBuffer())
            .setByteBuffer(COLUMN_BINDING, column.asByteBuffer());
        if (timestamp != null) {
            builder = builder.setLong(TIMESTAMP_BINDING, timestamp);
        }
        return builder.build();
    }

    public BatchableStatement<BoundStatement> insertColumn(final StaticBuffer key, final Entry entry) {
        return insertColumn(key, entry, null);
    }

    public BatchableStatement<BoundStatement> insertColumn(final StaticBuffer key, final Entry entry, final Long timestamp) {
        final Integer ttl = (Integer) entry.getMetaData().get(EntryMetaData.TTL);
        BoundStatementBuilder builder = ttl != null ? insertColumnWithTTL.boundStatementBuilder() : insertColumn.boundStatementBuilder();
        builder = builder.setByteBuffer(KEY_BINDING, key.asByteBuffer())
            .setByteBuffer(COLUMN_BINDING, entry.getColumn().asByteBuffer())
            .setByteBuffer(VALUE_BINDING, entry.getValue().asByteBuffer());
        if (ttl != null) {
            builder = builder.setInt(TTL_BINDING, ttl);
        }
        if (timestamp != null) {
            builder = builder.setLong(TIMESTAMP_BINDING, timestamp);
        }
        return builder.build();
    }

    @Override
    public void mutate(final StaticBuffer key, final List<Entry> additions, final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
        this.storeManager.mutateMany(Collections.singletonMap(this.tableName, Collections.singletonMap(key, new KCVMutation(additions, deletions))), txh);
    }

    public boolean isGroupedStore() {
        return groupedStore;
    }

    public void mutateGroupedKey(final StaticBuffer key, final KCVMutation keyMutations, final StoreTransaction txh) throws BackendException {
        if (!groupedStore) {
            throw new IllegalStateException("Grouped mutation called for non-grouped store: " + tableName);
        }
        if (isNoOpMutation(keyMutations)) {
            return;
        }
        for (int attempt = 0; attempt < MAX_GROUPED_MUTATION_RETRIES; attempt++) {
            final GroupedState state = fetchGroupedState(key, txh);
            final List<Entry> baseEntries = state.exists ? state.entries : fetchLegacyAllEntries(key, txh);
            final List<Entry> mergedEntries = applyMutation(baseEntries, keyMutations);
            if (state.exists && areEntriesEquivalent(state.entries, mergedEntries)) {
                return;
            }
            if (!state.exists && mergedEntries.isEmpty()) {
                return;
            }
            final List<ByteBuffer> encodedEntries = encodeEntries(mergedEntries);
            final long newVersion = state.version + 1L;

            final boolean applied;
            queryBackPressure.acquireBeforeQuery();
            try {
                final Row row;
                if (state.exists) {
                    row = session.execute(updateGroupedEntriesIfVersionMatches.boundStatementBuilder()
                        .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                        .setList(ENTRIES_BINDING, encodedEntries, ByteBuffer.class)
                        .setLong(VERSION_BINDING, newVersion)
                        .setLong(PREVIOUS_VERSION_BINDING, state.version)
                        .setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel())
                        .build()).one();
                } else {
                    row = session.execute(insertGroupedEntriesIfNotExists.boundStatementBuilder()
                        .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                        .setList(ENTRIES_BINDING, encodedEntries, ByteBuffer.class)
                        .setLong(VERSION_BINDING, newVersion)
                        .setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel())
                        .build()).one();
                }
                applied = row != null && row.getBoolean(APPLIED_COLUMN_NAME);
            } catch (Throwable t) {
                throw EXCEPTION_MAPPER.apply(t);
            } finally {
                queryBackPressure.releaseAfterQuery();
            }

            if (applied) {
                return;
            }
        }
        throw new TemporaryBackendException("Failed to apply grouped mutation after retries for table " + groupedTableName);
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) throws BackendException {
        final boolean hasLocking = this.storeManager.getFeatures().hasLocking();
        if (!hasLocking) {
            throw new UnsupportedOperationException(String.format("%s doesn't support locking", getClass()));
        }
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        if (!this.storeManager.getFeatures().hasOrderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when the byteorderedpartitioner is used.");
        }

        TokenMap tokenMap = this.session.getMetadata().getTokenMap().get();
        return Try.of(() -> new CQLResultSetKeyIterator(
            query,
            this.singleKeyGetter,
            new CQLPagingIterator(
                getKeysRanged.boundStatementBuilder()
                    .setToken(KEY_START_BINDING, tokenMap.newToken(query.getKeyStart().asByteBuffer()))
                    .setToken(KEY_END_BINDING, tokenMap.newToken(query.getKeyEnd().asByteBuffer()))
                    .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                    .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                    .setPageSize(this.storeManager.getPageSize())
                    .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()).build())))
            .getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh) throws BackendException {
        if (!this.storeManager.getFeatures().hasUnorderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when partitioner supports unordered scan");
        }

        return Try.of(() -> new CQLResultSetKeyIterator(
                query,
                this.singleKeyGetter,
                new CQLPagingIterator(
                    getKeysAll.boundStatementBuilder()
                        .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setPageSize(this.storeManager.getPageSize())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()).build())))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    private Optional<List<Entry>> fetchGroupedEntries(final StaticBuffer key, final StoreTransaction txh) throws BackendException {
        if (!groupedStore) {
            return Optional.empty();
        }
        return Optional.ofNullable(fetchGroupedState(key, txh)).filter(groupedState -> groupedState.exists).map(groupedState -> groupedState.entries);
    }

    private GroupedState fetchGroupedState(final StaticBuffer key, final StoreTransaction txh) throws BackendException {
        queryBackPressure.acquireBeforeQuery();
        try {
            final Row row = session.execute(getGroupedEntriesByKey.boundStatementBuilder()
                .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel())
                .build()).one();
            if (row == null) {
                return new GroupedState(false, Collections.emptyList(), 0L);
            }
            final List<ByteBuffer> rawEntries = row.getList(ENTRIES_COLUMN_NAME, ByteBuffer.class);
            final List<Entry> entries = rawEntries == null ? Collections.emptyList() : decodeEntries(rawEntries);
            final Long version = row.getLong(VERSION_COLUMN_NAME);
            return new GroupedState(true, entries, version == null ? 0L : version);
        } catch (Throwable t) {
            throw EXCEPTION_MAPPER.apply(t);
        } finally {
            queryBackPressure.releaseAfterQuery();
        }
    }

    private List<Entry> fetchLegacyAllEntries(final StaticBuffer key, final StoreTransaction txh) throws BackendException {
        queryBackPressure.acquireBeforeQuery();
        try {
            ResultSet resultSet = session.execute(getLegacyAllColumnsByKey.boundStatementBuilder()
                .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel())
                .build());
            List<Entry> result = new ArrayList<>();
            for (Row row : resultSet) {
                result.add(new StaticArrayEntry(concatColumnValue(row), row.getByteBuffer(COLUMN_COLUMN_NAME).remaining()));
            }
            result.sort(Comparator.comparing(Entry::getColumn));
            return result;
        } catch (Throwable t) {
            throw EXCEPTION_MAPPER.apply(t);
        } finally {
            queryBackPressure.releaseAfterQuery();
        }
    }

    private static byte[] concatColumnValue(Row row) {
        ByteBuffer column = row.getByteBuffer(COLUMN_COLUMN_NAME);
        ByteBuffer value = row.getByteBuffer(VALUE_COLUMN_NAME);
        ByteBuffer colDup = column.duplicate();
        ByteBuffer valDup = value.duplicate();
        byte[] data = new byte[colDup.remaining() + valDup.remaining()];
        colDup.get(data, 0, colDup.remaining());
        valDup.get(data, colDup.remaining(), valDup.remaining());
        return data;
    }

    private static List<Entry> applyMutation(List<Entry> entries, KCVMutation mutation) {
        TreeMap<StaticBuffer, Entry> merged = new TreeMap<>(StaticBuffer::compareTo);
        for (Entry entry : entries) {
            merged.put(entry.getColumn(), entry);
        }
        for (StaticBuffer deletion : mutation.getDeletions()) {
            merged.remove(deletion);
        }
        for (Entry addition : mutation.getAdditions()) {
            merged.put(addition.getColumn(), addition);
        }
        return new ArrayList<>(merged.values());
    }

    private static boolean isNoOpMutation(KCVMutation mutation) {
        return mutation == null || (mutation.getAdditions().isEmpty() && mutation.getDeletions().isEmpty());
    }

    private static boolean areEntriesEquivalent(List<Entry> left, List<Entry> right) {
        if (left == right) {
            return true;
        }
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            Entry leftEntry = left.get(i);
            Entry rightEntry = right.get(i);
            if (leftEntry.getColumn().compareTo(rightEntry.getColumn()) != 0) {
                return false;
            }
            if (!leftEntry.getValue().asByteBuffer().equals(rightEntry.getValue().asByteBuffer())) {
                return false;
            }
        }
        return true;
    }

    private static List<ByteBuffer> encodeEntries(List<Entry> entries) {
        List<ByteBuffer> encoded = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            ByteBuffer column = entry.getColumn().asByteBuffer();
            ByteBuffer value = entry.getValue().asByteBuffer();
            ByteBuffer encodedEntry = ByteBuffer.allocate(Integer.BYTES + column.remaining() + value.remaining());
            encodedEntry.putInt(column.remaining());
            encodedEntry.put(column.duplicate());
            encodedEntry.put(value.duplicate());
            encodedEntry.flip();
            encoded.add(encodedEntry);
        }
        return encoded;
    }

    private static List<Entry> decodeEntries(List<ByteBuffer> encoded) {
        List<Entry> entries = new ArrayList<>(encoded.size());
        for (ByteBuffer encodedEntry : encoded) {
            ByteBuffer duplicate = encodedEntry.duplicate();
            int columnLength = duplicate.getInt();
            byte[] data = new byte[duplicate.remaining()];
            duplicate.get(data);
            entries.add(new StaticArrayEntry(StaticArrayBuffer.of(data), columnLength));
        }
        entries.sort(Comparator.comparing(Entry::getColumn));
        return entries;
    }

    private static EntryList filterBySlice(List<Entry> entries, SliceQuery query) {
        if (entries.isEmpty()) {
            return EntryList.EMPTY_LIST;
        }
        final EntryArrayList result = new EntryArrayList();
        for (Entry entry : entries) {
            if (entry.getColumn().compareTo(query.getSliceStart()) < 0) {
                continue;
            }
            if (entry.getColumn().compareTo(query.getSliceEnd()) >= 0) {
                break;
            }
            result.add(entry);
            if (query.hasLimit() && result.size() >= query.getLimit()) {
                break;
            }
        }
        return result.isEmpty() ? EntryList.EMPTY_LIST : result;
    }

    private static final class GroupedState {
        private final boolean exists;
        private final List<Entry> entries;
        private final long version;

        private GroupedState(boolean exists, List<Entry> entries, long version) {
            this.exists = exists;
            this.entries = entries;
            this.version = version;
        }
    }

    /**
     * This class provides a paging implementation that sits on top of the DSE Cassandra driver.
     */
    private class CQLPagingIterator implements Iterator<Row> {

        private java.util.Iterator<Row> currentPageIterator = Collections.emptyIterator();

        private CompletableFuture<AsyncResultSet> futureResultSet;
        private AsyncResultSet currentAsyncResultSet;

        public CQLPagingIterator(BoundStatement boundStatement) {
            queryBackPressure.acquireBeforeQuery();
            try{
                futureResultSet = session.executeAsync(boundStatement)
                    .whenComplete((asyncResultSet, throwable) -> queryBackPressure.releaseAfterQuery()).toCompletableFuture();
            } catch (RuntimeException e){
                queryBackPressure.releaseAfterQuery();
                throw e;
            }
        }

        @Override
        public boolean hasNext() {
            if(currentPageIterator.hasNext()){
                return true;
            }

            if(currentAsyncResultSet == null){
                currentAsyncResultSet = CompletableFutureUtil.get(futureResultSet);
                currentPageIterator = currentAsyncResultSet.currentPage().iterator();
                if(currentPageIterator.hasNext()){
                    return true;
                }
            }

            if(currentAsyncResultSet.hasMorePages()){
                queryBackPressure.acquireBeforeQuery();
                try{
                    futureResultSet = currentAsyncResultSet.fetchNextPage()
                        .whenComplete((asyncResultSet, throwable) -> queryBackPressure.releaseAfterQuery()).toCompletableFuture();
                } catch (RuntimeException e){
                    queryBackPressure.releaseAfterQuery();
                    throw e;
                }
                currentAsyncResultSet = CompletableFutureUtil.get(futureResultSet);
                currentPageIterator = currentAsyncResultSet.currentPage().iterator();
                return hasNext();
            }

            return false;
        }

        @Override
        public Row next() {
            if(hasNext()){
                return currentPageIterator.next();
            }
            throw new NoSuchElementException();
        }
    }
}
