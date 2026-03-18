# CLAUDE.md — Atlan AI Agent Guidelines

> **Applies To:** `atlas-metastore`
> **Full security policy:** See `AGENTS.md`

---

## Security

`atlas-metastore` is an Apache Atlas fork used as Atlan's metadata store. Based on the upstream Apache Atlas project. Key surfaces: JanusGraph backend security, Atlas REST API authentication, and lineage graph query injection.

### Security Contact
Security questions → `#bu-security-and-it` on Slack.

### General Invariants
- **[MUST]** Atlas REST API must require Kerberos or basic authentication — disable anonymous access.
- **[MUST]** Gremlin queries constructed from user input must use parameterised bindings — never string interpolation.
- **[MUST]** JanusGraph backend credentials (Cassandra/HBase/Elasticsearch) from environment/secrets — never hardcoded in `atlas-application.properties`.
- **[SHOULD]** Apply upstream Apache Atlas security patches promptly.
