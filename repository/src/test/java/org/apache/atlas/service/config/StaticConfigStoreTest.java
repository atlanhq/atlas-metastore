package org.apache.atlas.service.config;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.config.DynamicConfigCacheStore.ConfigEntry;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StaticConfigStore.
 *
 * Covers:
 * - Happy path: Cassandra returns "cassandra" -> isCassandraGraphBackend() == true
 * - Empty row -> fallback to application.properties
 * - Cassandra unreachable -> fail-fast: System.exit called
 * - Immutability: Attempt to modify returned map -> UnsupportedOperationException
 * - Static API methods return correct values
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StaticConfigStoreTest {

    private static MockedStatic<CassandraConfigDAO> mockedCassandraDAO;
    private static CassandraConfigDAO mockDAO;

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
            config.setProperty("atlas.static.config.store.enabled", "true");
            config.setProperty("atlas.config.store.cassandra.enabled", "true");
            // Application properties fallback values
            config.setProperty("atlas.graphdb.backend", "janus");
            config.setProperty("atlas.graph.id.strategy", "legacy");
            config.setProperty("atlas.graph.claim.enabled", "false");
            // atlas.graph.index.search.es.prefix intentionally NOT set (tests null fallback)
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test configuration", e);
        }
    }

    @BeforeEach
    void setUp() {
        mockDAO = mock(CassandraConfigDAO.class);

        mockedCassandraDAO = mockStatic(CassandraConfigDAO.class);
        mockedCassandraDAO.when(CassandraConfigDAO::getInstance).thenReturn(mockDAO);
        mockedCassandraDAO.when(() -> CassandraConfigDAO.initialize(any())).thenAnswer(inv -> null);
        mockedCassandraDAO.when(CassandraConfigDAO::isInitialized).thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        mockedCassandraDAO.close();
    }

    @AfterAll
    void tearDownAll() {
        ApplicationProperties.forceReload();
    }

    // =================== Happy Path ===================

    @Test
    void testHappyPath_cassandraReturnsCassandra_isCassandraGraphBackend() throws AtlasBaseException {
        Map<String, ConfigEntry> cassandraData = new HashMap<>();
        cassandraData.put("atlas.graphdb.backend", entry("cassandra"));

        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(cassandraData);

        StaticConfigStore store = createStore(true);
        store.initialize();

        assertTrue(StaticConfigStore.isReady());
        assertTrue(StaticConfigStore.isCassandraGraphBackend());
        assertEquals("cassandra", StaticConfigStore.getGraphBackend());
    }

    @Test
    void testHappyPath_cassandraReturnsJanus_isNotCassandraGraphBackend() throws AtlasBaseException {
        Map<String, ConfigEntry> cassandraData = new HashMap<>();
        cassandraData.put("atlas.graphdb.backend", entry("janus"));

        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(cassandraData);

        StaticConfigStore store = createStore(true);
        store.initialize();

        assertTrue(StaticConfigStore.isReady());
        assertFalse(StaticConfigStore.isCassandraGraphBackend());
        assertEquals("janus", StaticConfigStore.getGraphBackend());
    }

    // =================== Empty Row -> Fallback to Application Properties ===================

    @Test
    void testEmptyRow_fallsBackToApplicationProperties() throws AtlasBaseException {
        // Cassandra reachable but no rows for atlas_static partition
        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(new HashMap<>());

        StaticConfigStore store = createStore(true);
        store.initialize();

        assertTrue(StaticConfigStore.isReady());
        // Falls back to atlas-application.properties where atlas.graphdb.backend=janus
        assertFalse(StaticConfigStore.isCassandraGraphBackend());
        assertEquals("janus", StaticConfigStore.getGraphBackend());
    }

    @Test
    void testPartialRows_missingKeysFallBackToApplicationProperties() throws AtlasBaseException {
        // Only GRAPH_BACKEND is set in Cassandra, others should fallback to application.properties
        Map<String, ConfigEntry> cassandraData = new HashMap<>();
        cassandraData.put("atlas.graphdb.backend", entry("cassandra"));

        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(cassandraData);

        StaticConfigStore store = createStore(true);
        store.initialize();

        // GRAPH_BACKEND from Cassandra
        assertEquals("cassandra", StaticConfigStore.getConfig("atlas.graphdb.backend"));

        // GRAPH_ID_STRATEGY from application.properties (atlas.graph.id.strategy=legacy)
        assertEquals("legacy", StaticConfigStore.getConfig("atlas.graph.id.strategy"));

        // GRAPH_CLAIM_ENABLED from application.properties (atlas.graph.claim.enabled=false)
        assertEquals("false", StaticConfigStore.getConfig("atlas.graph.claim.enabled"));
        assertFalse(StaticConfigStore.getConfigAsBoolean("atlas.graph.claim.enabled"));

        // GRAPH_ES_INDEX_PREFIX not in application.properties either -> null
        assertNull(StaticConfigStore.getConfig("atlas.graph.index.search.es.prefix"));
    }

    // =================== Fail-Fast: Kill Process ===================

    @Test
    void testCassandraUnreachable_callsExitProcess() throws AtlasBaseException {
        // DAO throws (Cassandra unreachable after retries)
        when(mockDAO.getAllConfigsForApp("atlas_static"))
                .thenThrow(new AtlasBaseException("Cassandra unreachable after 3 retries"));

        // Use spy to intercept exitProcess() and prevent actual System.exit
        StaticConfigStore store = spy(createStore(true));
        doNothing().when(store).exitProcess(anyInt());

        store.initialize();

        verify(store).exitProcess(1);
        assertFalse(StaticConfigStore.isReady());
    }

    // =================== Immutability ===================

    @Test
    void testGetAllConfigs_returnsUnmodifiableMap() throws AtlasBaseException {
        Map<String, ConfigEntry> cassandraData = new HashMap<>();
        cassandraData.put("atlas.graphdb.backend", entry("janus"));

        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(cassandraData);

        StaticConfigStore store = createStore(true);
        store.initialize();

        Map<String, String> configs = StaticConfigStore.getAllConfigs();

        assertThrows(UnsupportedOperationException.class,
                () -> configs.put("atlas.graphdb.backend", "cassandra"),
                "Returned map should be unmodifiable");
    }

    // =================== Disabled Store ===================

    @Test
    void testDisabledStore_fallsBackToApplicationProperties() {
        StaticConfigStore store = createStore(false);
        store.initialize();

        assertTrue(StaticConfigStore.isReady());
        // Falls back to atlas-application.properties
        assertEquals("janus", StaticConfigStore.getGraphBackend());
        assertFalse(StaticConfigStore.isCassandraGraphBackend());
        assertEquals("legacy", StaticConfigStore.getConfig("atlas.graph.id.strategy"));
    }

    // =================== getConfigAsBoolean ===================

    @Test
    void testGetConfigAsBoolean_trueString_returnsTrue() throws AtlasBaseException {
        Map<String, ConfigEntry> cassandraData = new HashMap<>();
        cassandraData.put("atlas.graph.claim.enabled", entry("true"));

        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(cassandraData);

        StaticConfigStore store = createStore(true);
        store.initialize();

        assertTrue(StaticConfigStore.getConfigAsBoolean("atlas.graph.claim.enabled"));
    }

    @Test
    void testGetConfigAsBoolean_falseString_returnsFalse() throws AtlasBaseException {
        Map<String, ConfigEntry> cassandraData = new HashMap<>();
        cassandraData.put("atlas.graph.claim.enabled", entry("false"));

        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(cassandraData);

        StaticConfigStore store = createStore(true);
        store.initialize();

        assertFalse(StaticConfigStore.getConfigAsBoolean("atlas.graph.claim.enabled"));
    }

    // =================== Seed Config ===================

    @Test
    void testSeedConfig_writesToCassandra() throws AtlasBaseException {
        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(new HashMap<>());

        StaticConfigStore store = createStore(true);
        store.initialize();

        StaticConfigStore.seedConfig("atlas.graphdb.backend", "cassandra", "admin");

        verify(mockDAO).putConfigForApp("atlas_static", "atlas.graphdb.backend", "cassandra", "admin");
    }

    @Test
    void testSeedConfig_doesNotChangeInMemoryValue() throws AtlasBaseException {
        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(new HashMap<>());

        StaticConfigStore store = createStore(true);
        store.initialize();

        // In-memory value is "janus" (from application.properties fallback)
        assertEquals("janus", StaticConfigStore.getGraphBackend());

        // Seed "cassandra" to Cassandra
        StaticConfigStore.seedConfig("atlas.graphdb.backend", "cassandra", "admin");

        // In-memory value should STILL be "janus" (restart required)
        assertEquals("janus", StaticConfigStore.getGraphBackend());
    }

    // =================== All Configs with Cassandra Override ===================

    @Test
    void testAllConfigsFromCassandra_overrideApplicationProperties() throws AtlasBaseException {
        Map<String, ConfigEntry> cassandraData = new HashMap<>();
        cassandraData.put("atlas.graphdb.backend", entry("cassandra"));
        cassandraData.put("atlas.graph.id.strategy", entry("nanoid"));
        cassandraData.put("atlas.graph.claim.enabled", entry("true"));
        cassandraData.put("atlas.graph.index.search.es.prefix", entry("myprefix"));

        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(cassandraData);

        StaticConfigStore store = createStore(true);
        store.initialize();

        assertEquals("cassandra", StaticConfigStore.getConfig("atlas.graphdb.backend"));
        assertEquals("nanoid", StaticConfigStore.getConfig("atlas.graph.id.strategy"));
        assertEquals("true", StaticConfigStore.getConfig("atlas.graph.claim.enabled"));
        assertEquals("myprefix", StaticConfigStore.getConfig("atlas.graph.index.search.es.prefix"));
    }

    // =================== Unknown Key ===================

    @Test
    void testGetConfig_unknownKey_returnsNull() throws AtlasBaseException {
        when(mockDAO.getAllConfigsForApp("atlas_static")).thenReturn(new HashMap<>());

        StaticConfigStore store = createStore(true);
        store.initialize();

        assertNull(StaticConfigStore.getConfig("non.existent.key"));
    }

    // =================== Helpers ===================

    private StaticConfigStore createStore(boolean enabled) {
        StaticConfigStoreConfig config = mock(StaticConfigStoreConfig.class);
        when(config.isEnabled()).thenReturn(enabled);
        when(config.getAppName()).thenReturn("atlas_static");
        when(config.getKeyspace()).thenReturn("config_store");
        when(config.getTable()).thenReturn("configs");
        when(config.getHostname()).thenReturn("localhost");
        when(config.getCassandraPort()).thenReturn(9042);
        when(config.getReplicationFactor()).thenReturn(1);
        when(config.getDatacenter()).thenReturn("datacenter1");
        when(config.getConsistencyLevel()).thenReturn("LOCAL_ONE");
        return new StaticConfigStore(config);
    }

    private static ConfigEntry entry(String value) {
        return new ConfigEntry(value, "test", Instant.now());
    }
}
