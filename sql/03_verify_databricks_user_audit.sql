-- =============================================================================
-- Verify End-User Identity Propagation in Databricks Audit Logs
-- Run in Databricks SQL Editor (auditable events typically land within ~15 mins)
-- =============================================================================
SELECT
  event_time,
  service_name,
  action_name,
  user_identity.email AS federated_gcp_user_email,
  request_params,
  response.status_code
FROM
  system.access.audit
WHERE
  (
    (service_name = 'accounts' AND action_name = 'oidcTokenAuthorization')
    OR (
      service_name = 'unityCatalog'
      AND action_name IN ('getTable', 'generateTemporaryTableCredential')
    )
  )
  AND event_time >= NOW() - INTERVAL 60 MINUTE
ORDER BY
  event_time DESC
LIMIT 25;
