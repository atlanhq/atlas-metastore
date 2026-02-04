## 📋 Implementation Plan Summary - Ready for Validation

### Proposed Approach
New API endpoint **`POST /api/meta/purposes/user`** in Atlas-Metastore using ES aggregation to return all accessible Purposes in a single call.

**How it works:**
1. Query AuthPolicies → aggregate unique Purpose GUIDs (ES aggregation)
2. Multi-Get → fetch Purpose details by GUIDs

### API Contract

**Request:**
```json
{ "username": "john.doe", "groups": ["data-team"], "limit": 100 }
```

**Response:**
```json
{ "purposes": [{ "guid": "...", "name": "..." }], "count": 10, "totalCount": 50 }
```

### Estimated Timeline
- **Week 1**: Core implementation + tests
- **Week 2**: Staging + load testing
- **Week 3-4**: Production rollout (canary → gradual)

### ⚠️ Open Questions (Need Input)

| Question | Owner |
|----------|-------|
| Is `accessControl.guid` indexed in ES? | @backend |
| Max purposes per tenant expected? | @product |
| Add Redis caching layer? | TBD |
| Auth requirements same as /whoami? | @security |
| Include nested policy details in response? | @frontend |

### Dependencies
- **Frontend**: ✅ Already ready (waiting on this API)
- **Heracles**: Will need update to call new API

---

**📄 Full docs:** [Implementation Plan](https://github.com/atlanhq/atlas-metastore/blob/main/docs/MS-546/IMPLEMENTATION_PLAN.md)

**Please confirm:**
- [ ] API contract meets frontend requirements
- [ ] ES aggregation approach is acceptable
- [ ] Timeline is realistic
- [ ] Open questions assigned correctly

cc: @shashank.sharma @frontend-team
