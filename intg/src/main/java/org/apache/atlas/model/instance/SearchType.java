package org.apache.atlas.model.instance;

public enum SearchType {
        SIMPLE,      // Normal search without PIT
        PIT_CREATE,  // Create a new PIT
        PIT_SEARCH,  // Search using existing PIT
        PIT_DELETE   // Delete an existing PIT

}
