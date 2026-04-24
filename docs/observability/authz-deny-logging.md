# AUTHZ_DENY logging

Centralized WARN-level log emitted every time Atlas authorization denies an operation and would throw `ATLAS-403-00-001 UNAUTHORIZED_ACCESS`. Intended for SRE / support to answer "who got denied doing what to which asset, and which policy caused it" without attaching a debugger or re-running the workflow with DEBUG.

This is observability only. The 403 response returned to the client is **unchanged**.

## Where it fires

`AtlasAuthorizationUtils.verifyAccess(...)` for all four access-request shapes:

| Scope | Privileges this covers |
|---|---|
| Entity | `entity-create`, `entity-update`, `entity-delete`, `entity-read`, `entity-*-classification`, `entity-add/remove-label`, `entity-update-business-metadata` |
| Relationship | `add-relationship`, `update-relationship`, `remove-relationship` |
| Type | `type-create`, `type-update`, `type-delete`, `type-read` |
| Admin | `admin-export`, `admin-import`, `admin-purge`, `admin-audits`, etc. |

This covers the entity CRUD paths that connector publish-phase workflows hit (bulk entity, relationships, classifications).

**Not covered** (direct role-check denies in REST layer; already include the user in their thrown message): `AttributeREST`, `BusinessLineageREST`, `DirectSearchREST`, `PurposeDiscoveryREST`, and `DataProductPreProcessor` lineage-status guard.

## Log line format

Single-line, key=value pairs, logger name `org.apache.atlas.authz.deny`.

### Entity / relationship / type / admin

```
AUTHZ_DENY user="<user>" groups=<N> action="<privilege>" entity="<type>/<guid>" qn="<qualifiedName>"
           cause="UNAUTHORIZED_ACCESS" policyId="<id|implicit>" policyName="<name|null>"
           enforcer="<atlas|abac_auth|merged_auth>" explicitDeny=<bool> priority=<int>
           purposes="[<guid>, ...]" msg="<errorMsgParam>"
```

Relationship denies replace `entity`/`qn` with `relationshipType="..." end1="<type>/<guid>" end1Qn="..." end2="<type>/<guid>" end2Qn="..."`.
Type denies replace `entity`/`qn` with `typeDef="<name>"`.
Admin denies use `scope="admin"`.

### Field meanings

| Field | Meaning |
|---|---|
| `user` | `RequestContext.getCurrentUser()` at the moment of the deny. |
| `groups` | Count of user groups (not names — avoids leaking group membership into logs). |
| `action` | `AtlasPrivilege.getType()` — stable string like `entity-delete`. |
| `entity` / `end1` / `end2` | `<typeName>/<guid>` — both are cheaply available from the request header; no graph lookup. |
| `qn` | `qualifiedName` attribute from the entity header. Truncated to 512 chars with `...` suffix. `"null"` if missing. |
| `cause` | Always `UNAUTHORIZED_ACCESS` for denies emitted by this logger. |
| `policyId` | ID of the policy that produced the deny. `"implicit"` if no specific policy matched (implicit deny). |
| `policyName` | Display name of the matched policy. Populated for ABAC denies (via `RangerPolicy.getName()`); `"null"` for implicit denies and for Ranger-engine denies (the Ranger plugin only propagates policy GUIDs). Use `policyId` as the canonical handle; `policyName` is a human-readable hint. |
| `enforcer` | `atlas` (Ranger/ACL engine), `abac_auth` (ABAC), or `merged_auth` (post-merge of both). |
| `explicitDeny` | `true` if a policy explicitly denied; `false` for implicit deny. |
| `priority` | Policy priority. `100` = OVERRIDE, `0` = NORMAL, `-1` = not set. |
| `purposes` | Best-effort list of Purpose GUIDs the user has access to (see below). |
| `msg` | Free-form context from the caller (e.g. `"delete entity: default.db.tbl"`). Truncated to 512 chars. |

## Purpose enrichment (best-effort)

When `atlas.authz.deny-logging.enrich-personas-purposes=true` (**default**), every deny triggers two ES queries against the `AuthPolicy` and `Purpose` indices via `PurposeDiscoveryServiceImpl.getUniquePurposeGuids(user, groups)` — the same lightweight path used by the `/api/meta/purposes/user` endpoint. The resulting GUIDs are appended as `purposes="[...]"`.

- Limited to 10 GUIDs per deny so both the ES cost and the log-line size stay bounded.
- Any failure (ES unavailable, timeout, null response) is swallowed: the base line is still emitted with `purposes=""`.
- **Persona reverse-lookup is not included**: the Atlas repository does not expose a cheap mapping from `(user, groups)` → Personas. Use the `policyId` field to trace back to the parent Persona/Purpose entity via the policy store.

Flip `atlas.authz.deny-logging.enrich-personas-purposes=false` in `atlas-application.properties` to disable the enrichment (e.g. if a cluster sees a pathologically high deny rate and the extra ES calls become noticeable).

## Config

| Key | Default | Effect |
|---|---|---|
| `atlas.authz.deny-logging.enabled` | `true` | Master switch for the entire `AUTHZ_DENY` line. Flip to `false` as an emergency escape hatch. |
| `atlas.authz.deny-logging.enrich-personas-purposes` | `true` | Toggle for the `purposes=[...]` enrichment only. |

Both are read once at class-load time.

## Logger routing

The line uses a dedicated logger name so operators can route / filter independently from the broader `org.apache.atlas.authorizer` logger:

```xml
<logger name="org.apache.atlas.authz.deny" level="WARN" additivity="false">
    <appender-ref ref="AUTHZ_DENY_FILE"/>
</logger>
```

Level is `WARN` by default. Set to `OFF` in `atlas-logback.xml` to suppress at the logger level while leaving the Atlas config flag alone (useful for noisy test environments).

## Example

Incoming `DELETE /api/atlas/v2/entity/guid/abc-123` from a connector publish step, denied by an ABAC policy:

```
AUTHZ_DENY user="svc.csa-bigquery" groups=2 action="entity-delete" entity="bigquery_table/abc-123"
           qn="default/bq/project.dataset.tbl@production" cause="UNAUTHORIZED_ACCESS"
           policyId="pol-99f8e2" policyName="deny-prod-tables" enforcer="merged_auth"
           explicitDeny=true priority=100 purposes="[purpose-prod-ro, purpose-finance-guard]"
           msg="delete entity"
```

From the `policyId`, look up the full AuthPolicy via `/api/atlas/v2/entity/guid/pol-99f8e2` to see which Persona or Purpose it belongs to and what filter criteria triggered the deny. `policyName` is a convenience hint and is only present for ABAC-path denies.
