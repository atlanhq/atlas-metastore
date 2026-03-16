package org.apache.atlas.repository.graphdb.migrator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of a single validation check.
 */
public class ValidationCheckResult {

    public enum Severity { PASS, WARN, FAIL }

    private final String checkName;
    private final String description;
    private final Severity severity;
    private final boolean passed;
    private final String message;
    private final Map<String, Object> details;
    private final List<String> sampleFailures;

    public ValidationCheckResult(String checkName, String description,
                                 boolean passed, Severity severity,
                                 String message) {
        this.checkName   = checkName;
        this.description = description;
        this.passed      = passed;
        this.severity    = severity;
        this.message     = message;
        this.details     = new LinkedHashMap<>();
        this.sampleFailures = new ArrayList<>();
    }

    public void addDetail(String key, Object value) {
        details.put(key, value);
    }

    public void addSampleFailure(String failure) {
        if (sampleFailures.size() < 10) {
            sampleFailures.add(failure);
        }
    }

    public boolean isPassed()            { return passed; }
    public Severity getSeverity()        { return severity; }
    public String getCheckName()         { return checkName; }
    public String getDescription()       { return description; }
    public String getMessage()           { return message; }
    public Map<String, Object> getDetails()    { return details; }
    public List<String> getSampleFailures()    { return sampleFailures; }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("check_name", checkName);
        m.put("description", description);
        m.put("passed", passed);
        m.put("severity", severity.name());
        m.put("message", message);
        if (!details.isEmpty()) m.put("details", details);
        if (!sampleFailures.isEmpty()) m.put("sample_failures", sampleFailures);
        return m;
    }
}
