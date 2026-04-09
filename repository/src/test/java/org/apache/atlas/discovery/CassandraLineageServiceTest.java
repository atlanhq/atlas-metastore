package org.apache.atlas.discovery;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CassandraLineageService}, focused on the hideProcess
 * virtual-edge logic in {@code classicTraverse()}.
 *
 * <p>Test graph:
 * <pre>
 *   ds_a  ──▶  proc_ab  ──▶  ds_b  ──▶  proc_bc  ──▶  ds_c
 *         (input)      (output)   (input)       (output)
 * </pre>
 */
class CassandraLineageServiceTest {

    // --- Vertex IDs (Cassandra primary keys) ---
    private static final String VID_DS_A    = "v_ds_a";
    private static final String VID_PROC_AB = "v_proc_ab";
    private static final String VID_DS_B    = "v_ds_b";
    private static final String VID_PROC_BC = "v_proc_bc";
    private static final String VID_DS_C    = "v_ds_c";

    // --- Entity GUIDs ---
    private static final String GUID_DS_A    = "guid-ds-a";
    private static final String GUID_PROC_AB = "guid-proc-ab";
    private static final String GUID_DS_B    = "guid-ds-b";
    private static final String GUID_PROC_BC = "guid-proc-bc";
    private static final String GUID_DS_C    = "guid-ds-c";

    // --- Edge labels (must match CassandraLineageService constants) ---
    private static final String PROCESS_INPUTS  = "__Process.inputs";
    private static final String PROCESS_OUTPUTS = "__Process.outputs";

    private CqlSession          session;
    private AtlasTypeRegistry   typeRegistry;
    private PreparedStatement   findVertexIdByGuidStmt;
    private PreparedStatement   getVertexStmt;
    private PreparedStatement   getEdgesInByLabelStmt;
    private PreparedStatement   getEdgesOutByLabelStmt;

    // Track bound statements to map session.execute() results
    private final Map<BoundStatement, ResultSet> executeResults = new IdentityHashMap<>();

    private CassandraLineageService service;

    // ================================================================
    //  Setup / teardown
    // ================================================================

    @BeforeEach
    void setUp() {
        session      = mock(CqlSession.class);
        typeRegistry = mock(AtlasTypeRegistry.class);

        findVertexIdByGuidStmt  = mock(PreparedStatement.class);
        getVertexStmt           = mock(PreparedStatement.class);
        getEdgesInByLabelStmt   = mock(PreparedStatement.class);
        getEdgesOutByLabelStmt  = mock(PreparedStatement.class);

        when(session.prepare(contains("vertex_index"))).thenReturn(findVertexIdByGuidStmt);
        when(session.prepare(contains("FROM vertices"))).thenReturn(getVertexStmt);
        when(session.prepare(contains("edges_in")))     .thenReturn(getEdgesInByLabelStmt);
        when(session.prepare(contains("edges_out")))    .thenReturn(getEdgesOutByLabelStmt);

        // Default: session.execute(any bound statement) → look up in executeResults map;
        // return empty ResultSet if not found
        when(session.execute(any(BoundStatement.class))).thenAnswer(inv -> {
            BoundStatement bs = inv.getArgument(0);
            ResultSet rs = executeResults.get(bs);
            if (rs != null) return rs;
            // Fallback: empty ResultSet
            ResultSet empty = mock(ResultSet.class);
            when(empty.one()).thenReturn(null);
            when(empty.iterator()).thenReturn(Collections.<Row>emptyList().iterator());
            return empty;
        });

        // Type registry: Table is a DataSet (Catalog), Process is a Process
        AtlasEntityType tableType = mock(AtlasEntityType.class);
        when(tableType.getTypeAndAllSuperTypes()).thenReturn(
                new HashSet<>(Arrays.asList("Table", "Catalog", "Referenceable", "Asset")));
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(tableType);

        AtlasEntityType processType = mock(AtlasEntityType.class);
        when(processType.getTypeAndAllSuperTypes()).thenReturn(
                new HashSet<>(Arrays.asList("Process", "Referenceable", "Asset")));
        when(typeRegistry.getEntityTypeByName("Process")).thenReturn(processType);

        service = new CassandraLineageService(session, typeRegistry);
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
        executeResults.clear();
    }

    // ================================================================
    //  Tests
    // ================================================================

    /**
     * With hideProcess=true, relations must be virtual dataset→dataset edges.
     * No Process GUID may appear as fromEntityId or toEntityId.
     *
     * <p>Graph: ds_a → proc_ab → ds_b  (OUTPUT direction)
     * <p>Expected relation: ds_a → ds_b  (with processId = proc_ab)
     */
    @Test
    void hideProcess_outputDirection_createsVirtualRelation() throws Exception {
        setupSingleHopGraph();

        AtlasLineageContext context = buildContext(GUID_DS_A,
                AtlasLineageInfo.LineageDirection.OUTPUT, 3, true);

        AtlasLineageInfo result = service.getClassicLineageInfo(context);

        // Collect process GUIDs from guidEntityMap
        Set<String> processGuids = new HashSet<>();
        result.getGuidEntityMap().forEach((guid, header) -> {
            if ("Process".equals(header.getTypeName())) processGuids.add(guid);
        });
        assertFalse(processGuids.isEmpty(), "Process should be in guidEntityMap");

        // Assert: no relation has a Process as an endpoint
        for (LineageRelation rel : result.getRelations()) {
            assertFalse(processGuids.contains(rel.getFromEntityId()),
                    "fromEntityId should not be a Process, got " + rel.getFromEntityId());
            assertFalse(processGuids.contains(rel.getToEntityId()),
                    "toEntityId should not be a Process, got " + rel.getToEntityId());
        }

        // Assert: exactly one virtual relation ds_a → ds_b
        assertEquals(1, result.getRelations().size(), "Should have exactly 1 virtual relation");
        LineageRelation rel = result.getRelations().iterator().next();
        assertEquals(GUID_DS_A, rel.getFromEntityId());
        assertEquals(GUID_DS_B, rel.getToEntityId());
        assertEquals(GUID_PROC_AB, rel.getProcessId(),
                "processId should reference the hidden process");
    }

    /**
     * With hideProcess=false (normal mode), relations include the Process as endpoint.
     *
     * <p>Graph: ds_a → proc_ab → ds_b  (OUTPUT direction)
     * <p>Expected relations: ds_a → proc_ab, proc_ab → ds_b
     */
    @Test
    void normalMode_outputDirection_processInRelations() throws Exception {
        setupSingleHopGraph();

        AtlasLineageContext context = buildContext(GUID_DS_A,
                AtlasLineageInfo.LineageDirection.OUTPUT, 3, false);

        AtlasLineageInfo result = service.getClassicLineageInfo(context);

        assertEquals(2, result.getRelations().size(), "Normal mode should have 2 relations");

        // At least one relation must reference the process GUID
        boolean processFound = false;
        for (LineageRelation rel : result.getRelations()) {
            if (GUID_PROC_AB.equals(rel.getFromEntityId()) ||
                    GUID_PROC_AB.equals(rel.getToEntityId())) {
                processFound = true;
                break;
            }
        }
        assertTrue(processFound, "Normal mode relations should include Process as endpoint");
    }

    /**
     * Multi-hop hideProcess: ds_a → proc_ab → ds_b → proc_bc → ds_c
     *
     * <p>Expected virtual relations: ds_a → ds_b, ds_b → ds_c
     */
    @Test
    void hideProcess_multiHop_outputDirection() throws Exception {
        setupMultiHopGraph();

        AtlasLineageContext context = buildContext(GUID_DS_A,
                AtlasLineageInfo.LineageDirection.OUTPUT, 5, true);

        AtlasLineageInfo result = service.getClassicLineageInfo(context);

        // Collect process GUIDs
        Set<String> processGuids = new HashSet<>();
        result.getGuidEntityMap().forEach((guid, header) -> {
            if ("Process".equals(header.getTypeName())) processGuids.add(guid);
        });

        // No process as relation endpoint
        for (LineageRelation rel : result.getRelations()) {
            assertFalse(processGuids.contains(rel.getFromEntityId()),
                    "fromEntityId should not be a Process: " + rel.getFromEntityId());
            assertFalse(processGuids.contains(rel.getToEntityId()),
                    "toEntityId should not be a Process: " + rel.getToEntityId());
        }

        // Should have exactly 2 virtual relations
        assertEquals(2, result.getRelations().size(), "Multi-hop should have 2 virtual relations");

        // Verify both virtual edges exist
        Set<String> relStrings = new HashSet<>();
        for (LineageRelation rel : result.getRelations()) {
            relStrings.add(rel.getFromEntityId() + "->" + rel.getToEntityId());
        }
        assertTrue(relStrings.contains(GUID_DS_A + "->" + GUID_DS_B), "Should have ds_a → ds_b");
        assertTrue(relStrings.contains(GUID_DS_B + "->" + GUID_DS_C), "Should have ds_b → ds_c");
    }

    /**
     * hideProcess=true with INPUT direction.
     *
     * <p>Graph: ds_a → proc_ab → ds_b  (INPUT direction from ds_b)
     * <p>Expected: virtual relation ds_a → ds_b with processId = proc_ab
     */
    @Test
    void hideProcess_inputDirection_createsVirtualRelation() throws Exception {
        setupSingleHopGraph();
        mockGuidLookup(GUID_DS_B, VID_DS_B);

        AtlasLineageContext context = buildContext(GUID_DS_B,
                AtlasLineageInfo.LineageDirection.INPUT, 3, true);

        AtlasLineageInfo result = service.getClassicLineageInfo(context);

        // No Process as relation endpoint
        Set<String> processGuids = new HashSet<>();
        result.getGuidEntityMap().forEach((guid, header) -> {
            if ("Process".equals(header.getTypeName())) processGuids.add(guid);
        });

        for (LineageRelation rel : result.getRelations()) {
            assertFalse(processGuids.contains(rel.getFromEntityId()),
                    "fromEntityId should not be a Process: " + rel.getFromEntityId());
            assertFalse(processGuids.contains(rel.getToEntityId()),
                    "toEntityId should not be a Process: " + rel.getToEntityId());
        }

        assertEquals(1, result.getRelations().size(), "Should have exactly 1 virtual relation");
        LineageRelation rel = result.getRelations().iterator().next();
        assertEquals(GUID_PROC_AB, rel.getProcessId());
    }

    // ================================================================
    //  Graph setup helpers
    // ================================================================

    /**
     * Sets up: ds_a → proc_ab → ds_b
     */
    private void setupSingleHopGraph() {
        // GUID → vertex_id
        mockGuidLookup(GUID_DS_A, VID_DS_A);

        // Vertices
        mockVertex(VID_DS_A,    GUID_DS_A,    "Table",   "ACTIVE");
        mockVertex(VID_PROC_AB, GUID_PROC_AB, "Process", "ACTIVE");
        mockVertex(VID_DS_B,    GUID_DS_B,    "Table",   "ACTIVE");

        // Edge: proc_ab --[__Process.inputs]--> ds_a  (ds_a is input to proc_ab)
        //   edges_in query on ds_a with label __Process.inputs returns this edge
        mockEdgesIn(VID_DS_A, PROCESS_INPUTS, Collections.singletonList(
                edgeRow("e1", VID_PROC_AB, VID_DS_A, PROCESS_INPUTS)));

        // Edge: proc_ab --[__Process.outputs]--> ds_b  (ds_b is output of proc_ab)
        //   edges_out query on proc_ab with label __Process.outputs returns this edge
        mockEdgesOut(VID_PROC_AB, PROCESS_OUTPUTS, Collections.singletonList(
                edgeRow("e2", VID_PROC_AB, VID_DS_B, PROCESS_OUTPUTS)));

        // For INPUT direction from ds_b:
        //   edges_in on ds_b with label __Process.outputs returns the edge from proc_ab
        mockEdgesIn(VID_DS_B, PROCESS_OUTPUTS, Collections.singletonList(
                edgeRow("e2", VID_PROC_AB, VID_DS_B, PROCESS_OUTPUTS)));
        //   edges_out on proc_ab with label __Process.inputs returns the edge to ds_a
        mockEdgesOut(VID_PROC_AB, PROCESS_INPUTS, Collections.singletonList(
                edgeRow("e1", VID_PROC_AB, VID_DS_A, PROCESS_INPUTS)));

        // Leaf: ds_b has no further OUTPUT edges
        mockEdgesIn(VID_DS_B, PROCESS_INPUTS, Collections.emptyList());
        // Leaf: ds_a has no further INPUT edges
        mockEdgesIn(VID_DS_A, PROCESS_OUTPUTS, Collections.emptyList());
    }

    /**
     * Sets up: ds_a → proc_ab → ds_b → proc_bc → ds_c
     */
    private void setupMultiHopGraph() {
        // GUID → vertex_id
        mockGuidLookup(GUID_DS_A, VID_DS_A);

        // Vertices
        mockVertex(VID_DS_A,    GUID_DS_A,    "Table",   "ACTIVE");
        mockVertex(VID_PROC_AB, GUID_PROC_AB, "Process", "ACTIVE");
        mockVertex(VID_DS_B,    GUID_DS_B,    "Table",   "ACTIVE");
        mockVertex(VID_PROC_BC, GUID_PROC_BC, "Process", "ACTIVE");
        mockVertex(VID_DS_C,    GUID_DS_C,    "Table",   "ACTIVE");

        // ds_a → proc_ab: edges_in on ds_a with __Process.inputs
        mockEdgesIn(VID_DS_A, PROCESS_INPUTS, Collections.singletonList(
                edgeRow("e1", VID_PROC_AB, VID_DS_A, PROCESS_INPUTS)));

        // proc_ab → ds_b: edges_out on proc_ab with __Process.outputs
        mockEdgesOut(VID_PROC_AB, PROCESS_OUTPUTS, Collections.singletonList(
                edgeRow("e2", VID_PROC_AB, VID_DS_B, PROCESS_OUTPUTS)));

        // ds_b → proc_bc: edges_in on ds_b with __Process.inputs
        mockEdgesIn(VID_DS_B, PROCESS_INPUTS, Collections.singletonList(
                edgeRow("e3", VID_PROC_BC, VID_DS_B, PROCESS_INPUTS)));

        // proc_bc → ds_c: edges_out on proc_bc with __Process.outputs
        mockEdgesOut(VID_PROC_BC, PROCESS_OUTPUTS, Collections.singletonList(
                edgeRow("e4", VID_PROC_BC, VID_DS_C, PROCESS_OUTPUTS)));

        // Leaf: ds_c has no further OUTPUT edges
        mockEdgesIn(VID_DS_C, PROCESS_INPUTS, Collections.emptyList());
    }

    // ================================================================
    //  Cassandra mock helpers
    // ================================================================

    private void mockGuidLookup(String guid, String vertexId) {
        BoundStatement bs = mock(BoundStatement.class);
        when(findVertexIdByGuidStmt.bind(guid)).thenReturn(bs);

        ResultSet rs = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(row.getString("vertex_id")).thenReturn(vertexId);
        when(rs.one()).thenReturn(row);
        executeResults.put(bs, rs);
    }

    private void mockVertex(String vertexId, String guid, String typeName, String state) {
        BoundStatement bs = mock(BoundStatement.class);
        when(getVertexStmt.bind(vertexId)).thenReturn(bs);

        ResultSet rs = mock(ResultSet.class);
        Row row = mock(Row.class);
        String propsJson = String.format(
                "{\"__guid\":\"%s\",\"__typeName\":\"%s\",\"__state\":\"%s\"}", guid, typeName, state);
        when(row.getString("properties")).thenReturn(propsJson);
        when(row.getString("type_name")).thenReturn(typeName);
        when(row.getString("state")).thenReturn(state);
        when(rs.one()).thenReturn(row);
        executeResults.put(bs, rs);
    }

    private void mockEdgesIn(String vertexId, String label, List<MockEdgeRow> edges) {
        BoundStatement bs = mock(BoundStatement.class);
        when(getEdgesInByLabelStmt.bind(vertexId, label)).thenReturn(bs);

        ResultSet rs = mock(ResultSet.class);
        List<Row> rows = new ArrayList<>();
        for (MockEdgeRow e : edges) {
            Row row = mock(Row.class);
            when(row.getString("edge_id")).thenReturn(e.edgeId);
            when(row.getString("out_vertex_id")).thenReturn(e.outVertexId);
            when(row.getString("edge_label")).thenReturn(e.label);
            when(row.getString("properties")).thenReturn(e.propertiesJson);
            when(row.getString("state")).thenReturn("ACTIVE");
            rows.add(row);
        }
        when(rs.iterator()).thenReturn(rows.iterator());
        executeResults.put(bs, rs);
    }

    private void mockEdgesOut(String vertexId, String label, List<MockEdgeRow> edges) {
        BoundStatement bs = mock(BoundStatement.class);
        when(getEdgesOutByLabelStmt.bind(vertexId, label)).thenReturn(bs);

        ResultSet rs = mock(ResultSet.class);
        List<Row> rows = new ArrayList<>();
        for (MockEdgeRow e : edges) {
            Row row = mock(Row.class);
            when(row.getString("edge_id")).thenReturn(e.edgeId);
            when(row.getString("in_vertex_id")).thenReturn(e.inVertexId);
            when(row.getString("edge_label")).thenReturn(e.label);
            when(row.getString("properties")).thenReturn(e.propertiesJson);
            when(row.getString("state")).thenReturn("ACTIVE");
            rows.add(row);
        }
        when(rs.iterator()).thenReturn(rows.iterator());
        executeResults.put(bs, rs);
    }

    // ================================================================
    //  Context / data helpers
    // ================================================================

    private AtlasLineageContext buildContext(String guid,
                                            AtlasLineageInfo.LineageDirection direction,
                                            int depth, boolean hideProcess) {
        AtlasLineageContext ctx = new AtlasLineageContext();
        ctx.setGuid(guid);
        ctx.setDirection(direction);
        ctx.setDepth(depth);
        ctx.setHideProcess(hideProcess);
        ctx.setAttributes(Collections.emptySet());
        return ctx;
    }

    /** Lightweight carrier for mock edge data. */
    private static class MockEdgeRow {
        final String edgeId;
        final String outVertexId;
        final String inVertexId;
        final String label;
        final String propertiesJson;

        MockEdgeRow(String edgeId, String outVertexId, String inVertexId, String label) {
            this.edgeId        = edgeId;
            this.outVertexId   = outVertexId;
            this.inVertexId    = inVertexId;
            this.label         = label;
            this.propertiesJson = String.format("{\"r:__guid\":\"%s\"}", edgeId);
        }
    }

    private static MockEdgeRow edgeRow(String edgeId, String outVertexId, String inVertexId, String label) {
        return new MockEdgeRow(edgeId, outVertexId, inVertexId, label);
    }
}
