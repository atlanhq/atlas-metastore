#!/bin/bash

ES_URL="${1:-http://localhost:9500}"

echo "Using Elasticsearch at: ${ES_URL}"

echo "Fetching aliases from janusgraph_vertex_index..."
VERTEX_ALIASES=$(curl -s "${ES_URL}/janusgraph_vertex_index/_alias")

if [ -z "$VERTEX_ALIASES" ]; then
  echo "ERROR: Could not fetch aliases from janusgraph_vertex_index"
  exit 1
fi

# Save to a temp file so Python can read it
TMPFILE=$(mktemp)
echo "$VERTEX_ALIASES" > "$TMPFILE"

ACTIONS_JSON=$(python3 << PYTHON_SCRIPT
import json, sys, re

with open("$TMPFILE") as f:
    data = json.load(f)

aliases = data["janusgraph_vertex_index"]["aliases"]

actions = []
uuid_pattern = re.compile(r"^[a-zA-Z0-9]{20,}$")

for alias_name, alias_config in aliases.items():
    if uuid_pattern.match(alias_name):
        action = {
            "add": {
                "index": "atlas_graph_vertex_index",
                "alias": "atlas_graph_" + alias_name
            }
        }
        if "filter" in alias_config:
            action["add"]["filter"] = alias_config["filter"]
        actions.append(action)

if not actions:
    sys.exit(1)

for a in actions:
    sys.stderr.write("  " + a["add"]["alias"] + "\n")

sys.stderr.write("Found {} UUID aliases to copy\n".format(len(actions)))
print(json.dumps({"actions": actions}))
PYTHON_SCRIPT
)

rm -f "$TMPFILE"

if [ $? -ne 0 ] || [ -z "$ACTIONS_JSON" ]; then
  echo "ERROR: No UUID aliases found or parsing failed"
  exit 1
fi

echo ""
echo "Applying aliases to atlas_graph_vertex_index..."
RESULT=$(curl -s -X POST "${ES_URL}/_aliases" \
  -H "Content-Type: application/json" \
  -d "$ACTIONS_JSON")

echo ""
echo "Result: $RESULT"

echo ""
echo "Verifying aliases on atlas_graph_vertex_index..."
EDGE_ALIASES=$(curl -s "${ES_URL}/atlas_graph_vertex_index/_alias")
TMPFILE2=$(mktemp)
echo "$EDGE_ALIASES" > "$TMPFILE2"

python3 << VERIFY_SCRIPT
import json
with open("$TMPFILE2") as f:
    data = json.load(f)
aliases = sorted(data["atlas_graph_vertex_index"]["aliases"].keys())
print("Total aliases on atlas_graph_vertex_index: {}".format(len(aliases)))
for a in aliases:
    print("  - " + a)
VERIFY_SCRIPT

rm -f "$TMPFILE2"

echo ""
echo "Done!"