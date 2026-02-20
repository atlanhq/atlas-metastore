#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${1:-atlas2}"
ATLAS_STS="${ATLAS_STS:-atlas}"
ATLAS_CM="${ATLAS_CM:-atlas-config}"
ATLAS_POD="${ATLAS_POD:-atlas-0}"

token_from_keycloak() {
  kubectl run -n "$NAMESPACE" tokenprobe --image=curlimages/curl:8.6.0 --rm -i --restart=Never --command -- sh -lc \
    'curl -sS -X POST "http://keycloak:8080/auth/realms/atlas/protocol/openid-connect/token" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data-urlencode "client_id=atlas" \
      --data-urlencode "client_secret=atlas-dev-secret" \
      --data-urlencode "grant_type=client_credentials" \
      | sed -n "s/.*\"access_token\":\"\([^\"]*\)\".*/\1/p"' | tail -n 1
}

smoke_request() {
  local token="$1"
  kubectl exec -n "$NAMESPACE" "$ATLAS_POD" -- sh -lc \
    "code=\$(curl -sS -o /tmp/atlas-auth-smoke.json -w '%{http_code}' \
      -H 'Authorization: Bearer $token' \
      http://127.0.0.1:21000/api/atlas/v2/types/typedefs);
     echo \"status=\$code\";
     head -c 220 /tmp/atlas-auth-smoke.json; echo"
}

set_mode() {
  local mode="$1"
  local authorizer
  local user_service_url="http://heracles-service.heracles.svc.cluster.local"

  case "$mode" in
    rbac)
      authorizer="atlas"
      ;;
    ranger)
      authorizer="org.apache.ranger.authorization.atlas.authorizer.RangerAtlasAuthorizer"
      ;;
    *)
      echo "Unsupported mode: $mode" >&2
      return 1
      ;;
  esac

  kubectl get cm "$ATLAS_CM" -n "$NAMESPACE" -o jsonpath='{.data.atlas-application\.properties}' > /tmp/atlas-application.properties
  sed -i '' "s|^atlas.authorizer.impl=.*|atlas.authorizer.impl=${authorizer}|" /tmp/atlas-application.properties
  sed -i '' "s|^atlas.user-service-url=.*|atlas.user-service-url=${user_service_url}|" /tmp/atlas-application.properties

  kubectl create configmap "$ATLAS_CM" -n "$NAMESPACE" \
    --from-file=atlas-application.properties=/tmp/atlas-application.properties \
    --dry-run=client -o yaml | kubectl apply -f -

  kubectl rollout restart "statefulset/${ATLAS_STS}" -n "$NAMESPACE"
  kubectl rollout status "statefulset/${ATLAS_STS}" -n "$NAMESPACE" --timeout=420s
  kubectl exec -n "$NAMESPACE" "$ATLAS_POD" -- sh -lc \
    "grep -n '^atlas.authorizer.impl\|^atlas.user-service-url\|^atlas.ranger.base.url' /opt/apache-atlas/conf/atlas-application.properties"
}

run_mode() {
  local mode="$1"
  echo "==== Switching mode: ${mode} ===="
  set_mode "$mode"

  local token
  token="$(token_from_keycloak)"
  if [[ -z "$token" ]]; then
    echo "token_status=empty"
  else
    smoke_request "$token"
  fi

  echo "---- Atlas auth diagnostics (${mode}) ----"
  kubectl logs -n "$NAMESPACE" "$ATLAS_POD" --tail=400 | \
    rg -n "Ranger|PolicyRefresher|policy-engine|Unauthorized|401|403|Exception|heracles|ranger-service|keycloak" || true
  echo
}

run_mode rbac
run_mode ranger

echo "Matrix finished."
