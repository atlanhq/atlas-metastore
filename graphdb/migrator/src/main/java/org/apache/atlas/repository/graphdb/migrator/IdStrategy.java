package org.apache.atlas.repository.graphdb.migrator;

public enum IdStrategy {
    LEGACY,
    HASH_JG;

    public static IdStrategy from(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return LEGACY;
        }

        String v = raw.trim().toLowerCase();
        if ("hash-jg".equals(v) || "hash_jg".equals(v) || "deterministic".equals(v)) {
            return HASH_JG;
        }

        return LEGACY;
    }
}

