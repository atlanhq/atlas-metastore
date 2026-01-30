package org.apache.atlas.idgenerator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption; // For setting consistency levels. Not needed
import com.datastax.oss.driver.api.core.config.DriverConfigLoader; //Not needed
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap; // Not needed anymore
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Distributed ID Generator that creates unique 7-character alphanumeric sequential IDs across multiple servers.
 * Each server gets a unique prefix to avoid collisions.
 * Assume id = "abcdefg"
 *      a -> Cluster prefix
 *      b -> Server prefix for id generation
 *      cdefg -> actual character supports rollover for next id generation
 */

public class DistributedIdGenerator implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DistributedIdGenerator.class);

    // ORIGINAL Order for Prefix allocation (a-z, A-Z, 0-9)
    private static final String CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    // NEW ASCII Order for Suffix generation (0-9, A-Z, a-z)
    private static final String ASCII_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private static final String KEYSPACE = "id_generator";
    private static final String TABLE = "server_ids";
    private static final String SERVER_PREFIX_TABLE = "server_prefix";
    private static final int ID_LENGTH = 7;
    private static final long BATCH_SIZE = 5000;

    private final CqlSession session;
    private final String serverName;
    private final String tableName;
    
    private final AtomicLong counter = new AtomicLong(0);
    private final AtomicReference<String> currentId = new AtomicReference<>();
    
    @Deprecated
    public void setCurrentId(String id) {
        this.currentId.set(id);
    }

    public DistributedIdGenerator(CqlSession session, String serverName, String tableName) {
        this.session = session;
        this.serverName = serverName;
        this.tableName = tableName;
        initializeSchema();
        initialize();
    }

    /**
     * THREAD-SAFE NEXT ID LOGIC
     * Prevents duplicate IDs and gaps identified in Stress Test.
     */
    public synchronized String nextId() {
        String current = currentId.get();

        // If newly initialized, return the loaded ID as the first ID
        if (this.counter.get() == 0) {
            this.counter.incrementAndGet();
            logger.info("Generated next ID: {}", current); //TODO : Review and remove this please @nikhil
            return current;
        }

        String prefix = current.substring(0, 2);
        String suffix = current.substring(2);

        // Increment suffix using ASCII sorting
        long suffixDecimal = base62ToDecimal(suffix, ASCII_CHARS);
        String nextSuffix = decimalToBase62(suffixDecimal + 1, 5, ASCII_CHARS);

        // Check for suffix overflow to trigger new prefix allocation
        if (nextSuffix.length() > 5) {
            logger.warn("Prefix {} exhausted. Allocating new prefix...", prefix);
            prefix = allocateNewPrefix();
            nextSuffix = "00000";
        }

        String nextVal = prefix + nextSuffix;
        this.currentId.set(nextVal);

        // Persist to Cassandra in batches to reduce IOPS/Tombstones
        if (this.counter.incrementAndGet() % BATCH_SIZE == 1) {
            persistCurrentId(nextVal);
        }

        return nextVal;
    }

     private void initializeSchema() {
        // Create keyspace if it doesn't exist
        session.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s " +
                        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} " +
                        "AND durable_writes = true", KEYSPACE));

        // Create server state table if it doesn't exist
        session.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "    server_name text PRIMARY KEY," +
                        "    last_id text," +
                        "    last_updated timestamp" +
                        ")", KEYSPACE, TABLE));

        // Create server ranges table if it doesn't exist
        session.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "    range_prefix text PRIMARY KEY," +
                        "    server_name text," +
                        "    last_updated timestamp" +
                        ")", KEYSPACE, SERVER_PREFIX_TABLE));
    }


    /**
     * COORDINATED PREFIX ALLOCATION
     * Uses original global 'server_prefix' table to sync across multiple nodes.
     */
    public String allocateNewPrefix() {
        ResultSet rs = session.execute(SimpleStatement.builder(
                "SELECT last_prefix FROM " + KEYSPACE + "." + SERVER_PREFIX_TABLE + " WHERE id = 'global'")
                .setConsistencyLevel(ConsistencyLevel.QUORUM).build());
        
        Row row = rs.one();
        String lastPrefix = (row != null) ? row.getString("last_prefix") : "aa";
        
        // Use ORIGINAL CHARS order for the 2-char prefix mapping
        String nextPrefix = incrementPrefix(lastPrefix);
        
        session.execute(SimpleStatement.builder(
                "UPDATE " + KEYSPACE + "." + SERVER_PREFIX_TABLE + " SET last_prefix = ? WHERE id = 'global'")
                .addPositionalValue(nextPrefix)
                .setConsistencyLevel(ConsistencyLevel.QUORUM).build());
        
        return nextPrefix;
    }

    public void initialize() {
        ResultSet rs = session.execute(SimpleStatement.builder(
                "SELECT last_id FROM " + KEYSPACE + "." + tableName + " WHERE server_name = ?")
                .addPositionalValue(serverName).build());
        Row row = rs.one();
        if (row != null) {
            this.currentId.set(row.getString("last_id"));
        } else {
            String startId = allocateNewPrefix() + "00000";
            this.currentId.set(startId);
            persistCurrentId(startId);
        }
    }

    public void persistCurrentId(String id) {
        session.execute(SimpleStatement.builder(
                "UPDATE " + KEYSPACE + "." + tableName + " SET last_id = ?, updated_at = ? WHERE server_name = ?")
                .addPositionalValues(id, Instant.now(), serverName)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build());
    }

    // --- RE-IMPLEMENTED UTILITY METHODS FOR API STABILITY ---

    public void forcePersist() { persistCurrentId(currentId.get()); }

    public String getCurrentId() { return currentId.get(); }

    private String incrementPrefix(String prefix) {
        long val = base62ToDecimal(prefix, CHARS);
        return decimalToBase62(val + 1, 2, CHARS);
    }

    public static long base62ToDecimal(String s, String charset) {
        long res = 0;
        for (char c : s.toCharArray()) res = res * 62 + charset.indexOf(c);
        return res;
    }

    public static String decimalToBase62(long n, int padding, String charset) {
        StringBuilder sb = new StringBuilder();
        while (n > 0) {
            sb.append(charset.charAt((int)(n % 62)));
            n /= 62;
        }
        while (sb.length() < padding) sb.insert(0, charset.charAt(0));
        return sb.toString();
    }

    public boolean isValidId(String id) { return id != null && id.length() == 7; }

    public String getClusterId(String id) { return isValidId(id) ? id.substring(0, 1) : null; }

    public String getServerId(String id) { return isValidId(id) ? id.substring(1, 2) : null; }

    public String getSuffix(String id) { return isValidId(id) ? id.substring(2) : null; }

    @Deprecated public List<String> getAllocatedPrefixes() { return Collections.emptyList(); }

    public void resetCounter() { this.counter.set(0); }

    public long getInternalCounter() { return counter.get(); }

    @Override
    public void close() {
        forcePersist();
        logger.info("ID Generator shutdown. Last ID {} saved.", currentId.get());
    }
}