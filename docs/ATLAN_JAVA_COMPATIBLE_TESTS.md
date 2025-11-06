# Potentially Compatible atlan-java Tests

**Analysis Date:** November 6, 2024

## Summary

Out of 39 `atlan-java` integration tests, **most cannot run** against standalone `atlas-metastore` due to dependencies on Atlan-specific services (Heracles, Heka, Chronos, Keycloak).

However, a **very small subset** might work if they only use `AtlasEndpoint` APIs.

## Service Dependencies by Endpoint

### ✅ **AtlasEndpoint** (Compatible with atlas-metastore)
These endpoints use standard Apache Atlas REST APIs:
- `AssetEndpoint` → `/api/atlas/v2/entity/*`
- `TypeDefsEndpoint` → `/api/atlas/v2/types/*`
- `TaskEndpoint` → `/api/atlas/v2/tasks/*`
- `SearchLogEndpoint` → `/api/atlas/admin/audits`

### ❌ **HeraclesEndpoint** (Requires Atlan Heracles service)
Service URL: `http://heracles-service.heracles.svc.cluster.local`
Prefix: `/api/service`

Used by:
- `ApiTokensEndpoint`
- `ContractsEndpoint`
- `CredentialsEndpoint`
- `FilesEndpoint`
- `GroupsEndpoint`
- `ImagesEndpoint`
- `LogsEndpoint`
- `OAuthClientsEndpoint`
- `PermissionsEndpoint`
- `RequestsEndpoint`
- `RolesEndpoint` ⚠️ **Called by almost ALL tests**
- `SSOEndpoint`
- `UsersEndpoint`
- `WorkflowsEndpoint`

### ❌ **HekaEndpoint** (Requires Atlan Heka service)
Used by:
- `QueriesEndpoint`
- `QueryParserEndpoint`

### ❌ **ChronosEndpoint** (Requires Atlan Chronos service)
Used by:
- `OpenLineageEndpoint`

### ❌ **KeycloakEndpoint** (Requires Keycloak)
Used by:
- `ImpersonationEndpoint`

## Test Analysis

### ❌ **29 Tests Depend on ConnectionTest**
These tests call `ConnectionTest.createConnection()` which immediately calls:
```java
String adminRoleGuid = client.getRoleCache().getIdForSid("$admin");
```
This requires `RolesEndpoint` (Heracles), so all 29 tests **FAIL**:

- ADLSAssetTest
- AdminTest
- AirflowAssetTest
- AnaplanAssetTest
- APIAssetTest
- AppAssetTest
- AtlanTagTest
- AzureEventHubTest
- ConnectionTest
- CubeTest
- CustomAssetTest
- CustomMetadataTest
- DataMeshTest
- DataStudioAssetTest
- DataverseAssetTest
- FileTest
- GCSAssetTest
- KafkaTest
- LineageTest
- LinkTest
- ModelTest
- PersonaTest
- PresetAssetTest
- PurposeTest
- RequestsTest
- S3AssetTest
- SQLAssetTest
- SupersetAssetTest
- TableSearchTest

### 🤔 **10 Tests Might Be Standalone**
These don't directly import `ConnectionTest`:

1. **GlossaryTest** - Creates glossaries, categories, terms
   - **Likely works** if it only uses Atlas entity APIs
   - Needs verification

2. **InsightsTest** - ❌ Probably uses Heka/analytics endpoints

3. **OAuthTest** - ❌ Uses `OAuthClientsEndpoint` (Heracles)

4. **QueryParserTest** - ❌ Uses `QueryParserEndpoint` (Heka)

5. **ResourceTest** - 🤔 Unknown, needs inspection

6. **SearchTest** - Creates entities and searches for them
   - ⚠️ References `Persona` and `Purpose` (might need Heracles)
   - Needs verification

7. **SSOTest** - ❌ Uses `SSOEndpoint` (Heracles)

8. **SuggestionsTest** - 🤔 Unknown, needs inspection

9. **WorkflowTest** - ❌ Uses `WorkflowsEndpoint` (Heracles)

## Realistic Assessment

### 🎯 **Maybe 1-3 tests could work**

The **most promising candidates** are tests that:
1. Create entities directly using `AssetEndpoint`
2. Don't create connections (which need roles)
3. Don't use Persona/Purpose/Workflow
4. Only search/read basic metadata

**Potential candidates:**
- ✅ **GlossaryTest** (if it doesn't depend on connections/roles)
- 🤔 **SearchTest** (if we skip Persona/Purpose tests)
- 🤔 **ResourceTest** (needs inspection)

### ❌ **Reality Check**

Even if 1-3 tests technically "work":
- **Limited value:** Testing <10% of SDK functionality
- **Maintenance burden:** Tests may break as SDK evolves
- **False confidence:** Passing tests don't mean SDK works with Atlas
- **Better alternative:** Existing atlas-metastore tests already cover Atlas REST API

## Recommendation

**Don't pursue this.** The effort to identify, isolate, and maintain 1-3 compatible tests is **not worth it** when:

1. **atlas-metastore already has comprehensive integration tests** for Atlas REST API
2. **atlan-java is designed for the full Atlan product**, not standalone Atlas
3. **99% of SDK features require Atlan services** that don't exist in atlas-metastore

## If You Must Test atlan-java

Test against:
- ✅ **Full Atlan deployment** (production/staging)
- ✅ **Atlan development environment**
- ❌ **NOT standalone atlas-metastore**

## References

- Main analysis: `docs/ATLAN_JAVA_SDK_COMPATIBILITY.md`
- Service endpoints: `atlan-java/sdk/src/main/java/com/atlan/api/*Endpoint.java`
- Test dependencies: `atlan-java/integration-tests/src/test/java/com/atlan/java/sdk/*Test.java`

