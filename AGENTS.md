# AGENTS.md — Atlan AI Agent Guidelines

> **Applies To:** `atlas-metastore`
> **Companion file:** See `CLAUDE.md` for the lean summary.

---

## Security

`atlas-metastore` is an Apache Atlas fork (Atlan's metadata store). Based on upstream Apache Atlas with JanusGraph backend.

### Security Contact
`#bu-security-and-it` on Slack.

---

### Gremlin Query Injection Prevention

```java
// ❌ User input interpolated into Gremlin query string
String query = "g.V().has('Asset', 'name', '" + userInput + "')";

// ✅ Gremlin parameterised bindings
Map<String, Object> bindings = new HashMap<>();
bindings.put("name", userInput);
graph.traversal().V().has("Asset", "name", __.bindings(bindings.get("name")));
```

**[MUST]** All Gremlin queries from user input must use parameterised bindings.

---

### JanusGraph Backend Credentials

```properties
# ❌ Hardcoded in atlas-application.properties
atlas.graph.storage.hostname=cassandra-host
atlas.graph.storage.username=cassandra_user
atlas.graph.storage.password=cassandra_password

# ✅ Reference environment variables or external secrets
atlas.graph.storage.password=${CASSANDRA_PASSWORD}
```

**[MUST]** JanusGraph backend credentials from environment variables or Kubernetes Secrets — never hardcoded in property files committed to git.

---

### General Invariants

- **[MUST]** Atlas REST API: authentication required, no anonymous access.
- **[MUST]** Gremlin queries: parameterised bindings for all user input.
- **[MUST]** JanusGraph credentials: from environment/secrets only.
- **[SHOULD]** Apply upstream Apache Atlas security patches promptly.
