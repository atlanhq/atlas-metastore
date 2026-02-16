package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IndexRepository {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRepository.class);

    private final CqlSession session;
    private PreparedStatement insertIndexStmt;
    private PreparedStatement selectIndexStmt;
    private PreparedStatement deleteIndexStmt;

    public IndexRepository(CqlSession session) {
        this.session = session;
        prepareStatements();
    }

    private void prepareStatements() {
        insertIndexStmt = session.prepare(
            "INSERT INTO vertex_index (index_name, index_value, vertex_id) VALUES (?, ?, ?)"
        );

        selectIndexStmt = session.prepare(
            "SELECT vertex_id FROM vertex_index WHERE index_name = ? AND index_value = ?"
        );

        deleteIndexStmt = session.prepare(
            "DELETE FROM vertex_index WHERE index_name = ? AND index_value = ?"
        );
    }

    public void addIndex(String indexName, String indexValue, String vertexId) {
        session.execute(insertIndexStmt.bind(indexName, indexValue, vertexId));
    }

    public String lookupVertex(String indexName, String indexValue) {
        ResultSet rs = session.execute(selectIndexStmt.bind(indexName, indexValue));
        Row row = rs.one();
        return row != null ? row.getString("vertex_id") : null;
    }

    public void removeIndex(String indexName, String indexValue) {
        session.execute(deleteIndexStmt.bind(indexName, indexValue));
    }

    public void batchAddIndexes(List<IndexEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        for (IndexEntry entry : entries) {
            batch.addStatement(insertIndexStmt.bind(entry.indexName, entry.indexValue, entry.vertexId));
        }
        session.execute(batch.build());
    }

    public static class IndexEntry {
        public final String indexName;
        public final String indexValue;
        public final String vertexId;

        public IndexEntry(String indexName, String indexValue, String vertexId) {
            this.indexName  = indexName;
            this.indexValue = indexValue;
            this.vertexId   = vertexId;
        }
    }
}
