# Quick Start: Extended Integration Tests

## 🚀 Run It Now!

```bash
cd atlas-metastore
./run-extended-integration-tests.sh
```

That's it! The script will:
1. ✅ Build Atlas (~5 min)
2. ✅ Run atlas-metastore tests (~10 min)
3. ✅ Run atlan-java tests (~15 min)
4. ✅ Clean up automatically

## ⚡ Faster Options

### Already built Atlas?

```bash
./run-extended-integration-tests.sh --skip-build
```

### Run specific tests only?

```bash
./run-extended-integration-tests.sh --tests "ConnectionTest SearchTest"
```

### Debug mode?

```bash
./run-extended-integration-tests.sh --debug
```

## 📋 What You Need

- ✅ Docker Desktop running
- ✅ Java 17 installed
- ✅ ~10GB free disk space
- ✅ **GitHub Token** (required for Maven dependencies):
  ```bash
  export GITHUB_TOKEN='your-personal-access-token'
  ```
  Create one at: https://github.com/settings/tokens/new (needs `read:packages` scope)
- ✅ API Key (optional, for atlan-java tests):
  ```bash
  export ATLAN_API_KEY='...'
  ```

## 🔍 Troubleshooting

### Script says "Permission denied"?

```bash
chmod +x run-extended-integration-tests.sh
```

### Containers fail to start?

```bash
docker system prune -a  # Clean up old containers
```

### Tests timeout?

Increase timeout in script (line ~200):
```bash
MAX_RETRIES=120  # Default is 60
```

## 📊 Results

- ✅ **Logs**: `target/test-logs/`
- ✅ **Reports**: `/tmp/atlan-java-*/integration-tests/build/reports/`
- ✅ **Summary**: Printed at end of script

## 📚 Full Documentation

See [EXTENDED_INTEGRATION_TESTS.md](EXTENDED_INTEGRATION_TESTS.md) for complete details!

## 🎯 Available Tests

Run any of these atlan-java tests:

- `ConnectionTest` - Connection management ✅ Recommended
- `SearchTest` - Search functionality ✅ Recommended  
- `GlossaryTest` - Glossary operations ✅ Recommended
- `CustomMetadataTest` - Custom metadata
- `LineageTest` - Lineage operations
- `AtlanTagTest` - Tag management
- `AdminTest` - Admin operations
- `WorkflowTest` - Workflow management
- ...and 32 more!

Example:
```bash
./run-extended-integration-tests.sh --tests "ConnectionTest SearchTest GlossaryTest"
```

---

**Need help?** Check [EXTENDED_INTEGRATION_TESTS.md](EXTENDED_INTEGRATION_TESTS.md) or create an issue!

