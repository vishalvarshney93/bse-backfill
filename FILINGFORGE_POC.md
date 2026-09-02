# FilingForge Research POC

This POC uses the FilingForge **Python engine**, not its MCP server. Python is
the deterministic batch interface for GitHub Actions; MCP remains useful later
for interactive developer exploration of a local library.

## What the POC does

1. Pulls BSE filings with a pinned FilingForge commit. Ambiguous names can be
  pinned as `name|BSE-scrip-code`; a valid six-digit pinned code is
  authoritative even when BSE's name-search endpoint rejects punctuation or
  spelling in the display name.
2. Converts all available categories to Markdown.
3. Uploads Markdown and a stable per-company manifest to private Azure Blob
   Storage. PDFs are temporary conversion inputs and are deleted.
4. Selects a balanced recent sample of annual reports, quarterly results,
   presentations, and concalls.
5. Uses NVIDIA NIM to extract exact-quote evidence and rejects unsupported
   quotations.
6. Produces a citation-bound company snapshot containing detailed overview
   sections, positives, risks, management guidance, future deliverables, and a
   walk-the-talk table.
7. Records per-company completion in Azure Table Storage.

This is an ingestion and research-contract POC. It does not yet add UI to the
Ticker Vector company page or create normalized financial/shareholding tables.
Those follow only after the POC corpus and evidence quality are accepted.

## Azure resources

Create a dedicated resource group and deploy `infra/filingforge-poc.bicep`.
It creates:

- One Standard LRS GPv2 storage account with shared-key access disabled.
- Private `filings-md` and `research-snapshots` Blob containers.
- `ResearchIngestionState` Azure Table.
- A lifecycle rule moving historical Markdown to Cool after 30 days.
- Blob and Table contributor roles for the GitHub service principal.

The roles are scoped to only those two containers and that one table. The
workflow cannot modify unrelated data in the storage account.

Use a separate storage account from the current Bhavcopy account. The POC
template intentionally enforces OAuth-only access.

## One-time Azure and GitHub preparation

Run these commands from this repository after signing Azure CLI into the
personal subscription that owns Ticker Vector. Choose a globally unique storage
account name.

```powershell
$resourceGroup = "ticker-vector-research-poc"
$location = "centralindia"
$storageAccount = "tvresearchpoc12345"
$repo = "vishalvarshney93/bse-backfill"

az group create --name $resourceGroup --location $location

$app = az ad app create --display-name "ticker-vector-filingforge-actions" | ConvertFrom-Json
$sp = az ad sp create --id $app.appId | ConvertFrom-Json
$operatorReaderPrincipalId = az ad signed-in-user show --query id -o tsv

$credential = @{
  name = "bse-backfill-main"
  issuer = "https://token.actions.githubusercontent.com"
  subject = "repo:$repo`:ref:refs/heads/main"
  audiences = @("api://AzureADTokenExchange")
} | ConvertTo-Json -Compress

az ad app federated-credential create --id $app.id --parameters $credential

az deployment group create `
  --resource-group $resourceGroup `
  --template-file infra/filingforge-poc.bicep `
  --parameters storageAccountName=$storageAccount githubPrincipalId=$($sp.id) `
    operatorReaderPrincipalId=$operatorReaderPrincipalId
```

The signed-in Azure user running this deployment must be allowed to create role
assignments, such as Owner or User Access Administrator on the resource group.
Wait several minutes after deployment for data-plane RBAC propagation before
running the workflow.

In GitHub repository **Settings > Secrets and variables > Actions**, add:

Secrets:

- `AZURE_CLIENT_ID`: `$app.appId`
- `AZURE_TENANT_ID`: output of `az account show --query tenantId -o tsv`
- `AZURE_SUBSCRIPTION_ID`: output of `az account show --query id -o tsv`
- `NVIDIA_NIM_API_KEY`: NVIDIA API Catalog key

Variables:

- `FILINGFORGE_STORAGE_ACCOUNT`: the value of `$storageAccount`
- `NVIDIA_NIM_MODEL`: the full publisher/model identifier, including `/`, such
  as `nvidia/nemotron-3-ultra-550b-a55b` or
  `nvidia/nemotron-3.5-lightning-30b-a3b`
- `NVIDIA_NIM_EXTRACTION_MODEL`: optional fast model for per-document evidence
  extraction; when blank, extraction reuses `NVIDIA_NIM_MODEL`

No Azure client secret or storage key is needed.

## First run

Open **Actions > FilingForge Research POC > Run workflow**:

- Companies: `SHILPAMED|530549,Maruti Suzuki India|532500,HDFC Bank|500180`
- Years: `2`
- Run AI analysis: `true`
- Maximum documents: `10`
- Windows per long document: `2` for the POC; use `3` for deeper
  beginning/middle/end coverage after the first run succeeds

For the cheapest storage-only smoke test, set AI analysis to `false`.

The workflow deliberately completes all slow FilingForge downloads and NVIDIA
analysis between two Azure logins. The first login immediately validates OIDC,
both Blob containers, the Table endpoint, the NVIDIA key, and the selected
model before any filings are downloaded. The second login obtains a fresh OIDC
token immediately before the short upload-only phase. This prevents
`AADSTS700024` when a five-minute GitHub federated assertion expires during a
long download and fails fast when any configuration is wrong.

Expected Blob paths:

```text
filings-md/companies/{company-key}/manifest.json
filings-md/companies/{company-key}/documents/{category}/{year}/{document-id}/{content-sha256}.md
research-snapshots/companies/{company-key}/latest.json
research-snapshots/companies/{company-key}/history/{timestamp}.json
```

The `latest.json` contract is documented in
`schemas/company-research.schema.json`.

## Manual Azure verification

Shared-key access is intentionally disabled, so access keys cannot browse this
account. Use Entra authentication. The following isolated Azure CLI profile
keeps the personal tenant separate from any Microsoft work-tenant login:

```powershell
$env:AZURE_CONFIG_DIR = "$HOME\.azure-ticker-vector"
$tenantId = "47e037bc-bb25-4beb-b44b-62d242f97a2e"
$subscriptionId = "636c95d0-c24c-4684-8840-d1338ae47708"
$storageAccount = "filingforgepocstorage"

az login --tenant $tenantId --use-device-code
az account set --subscription $subscriptionId

$account = az storage account show --name $storageAccount | ConvertFrom-Json
$resourceGroup = $account.resourceGroup
$accountId = $account.id
$operatorObjectId = az ad signed-in-user show --query id -o tsv
```

For the existing deployment, run these once from an identity allowed to create
role assignments, then wait up to ten minutes for RBAC propagation:

```powershell
$filingsScope = "$accountId/blobServices/default/containers/filings-md"
$researchScope = "$accountId/blobServices/default/containers/research-snapshots"
$tableScope = "$accountId/tableServices/default/tables/ResearchIngestionState"

az role assignment create --assignee-object-id $operatorObjectId `
  --assignee-principal-type User --role "Storage Blob Data Reader" --scope $filingsScope
az role assignment create --assignee-object-id $operatorObjectId `
  --assignee-principal-type User --role "Storage Blob Data Reader" --scope $researchScope
az role assignment create --assignee-object-id $operatorObjectId `
  --assignee-principal-type User --role "Storage Table Data Reader" --scope $tableScope
```

List and download the uploaded SHILPA artifacts with Entra authentication:

```powershell
az storage blob list --account-name $storageAccount --container-name filings-md `
  --auth-mode login --prefix "companies/SHILPA-530549/" `
  --query "[].{Name:name,Bytes:properties.contentLength}" -o table

az storage blob list --account-name $storageAccount --container-name research-snapshots `
  --auth-mode login --prefix "companies/SHILPA-530549/" `
  --query "[].{Name:name,Bytes:properties.contentLength}" -o table

New-Item -ItemType Directory -Force verification | Out-Null
az storage blob download --account-name $storageAccount --container-name filings-md `
  --auth-mode login --name "companies/SHILPA-530549/manifest.json" `
  --file "verification/SHILPA-manifest.json"
az storage blob download --account-name $storageAccount --container-name research-snapshots `
  --auth-mode login --name "companies/SHILPA-530549/latest.json" `
  --file "verification/SHILPA-latest.json"

Get-Content "verification/SHILPA-manifest.json" -Raw | ConvertFrom-Json
Get-Content "verification/SHILPA-latest.json" -Raw | ConvertFrom-Json

$manifest = Get-Content "verification/SHILPA-manifest.json" -Raw | ConvertFrom-Json
$document = $manifest.documents[0]
az storage blob download --account-name $storageAccount --container-name filings-md `
  --auth-mode login --name $document.blob_name `
  --file "verification/sample-filing.md"
$actualHash = (Get-FileHash "verification/sample-filing.md" -Algorithm SHA256).Hash.ToLowerInvariant()
"Expected: $($document.content_sha256)"
"Actual:   $actualHash"
Get-Content "verification/sample-filing.md" -TotalCount 80
```

Query the ingestion state and confirm the row has `Status=complete`,
`DocumentCount=27`, and `Detail=analysis=True` for the successful quarter-year
SHILPA run:

```powershell
az storage entity query --account-name $storageAccount `
  --table-name ResearchIngestionState --auth-mode login `
  --filter "PartitionKey eq 'FILINGFORGE_POC'" `
  --query "items[].{Company:RowKey,Status:Status,Documents:DocumentCount,Detail:Detail,UpdatedAt:UpdatedAt}" `
  -o table
```

The current upload phase also performs read-after-write verification. A green
workflow must log `verified Azure manifest, ... Markdown hashes, ... table
state, and zero PDFs`; missing or mismatched data now fails the workflow.

For portal inspection, sign in at `https://portal.azure.com` with the same
personal-tenant identity, switch directory to tenant
`47e037bc-bb25-4beb-b44b-62d242f97a2e`, open `filingforgepocstorage`, then use
**Storage browser**. Blob data is under **Blob containers** > `filings-md` or
`research-snapshots`; state rows are under **Tables** >
`ResearchIngestionState`. If the data panes remain blank after role assignment,
wait for RBAC propagation, sign out/in, and verify the portal directory and
subscription are the personal ones above.

## Acceptance checks

- No PDF exists in either Blob container.
- Every manifest document hash matches its Markdown Blob.
- Every document retains FilingForge `news_id`, `source_pdf`, extraction status,
  and converter version; citations remain stable if Markdown is regenerated.
- `empty`, `thin`, `garbled`, and `failed` documents remain cataloged but are
  excluded from AI synthesis.
- Every evidence quote occurs verbatim in its cited Markdown.
- Every overview, positive, risk, guidance, deliverable, and walk-the-talk row
  references a valid manifest document ID.
- Future guidance remains `pending`; it is never labeled `missed` early.
- The same run can be repeated without duplicate Blob paths.

## Incremental ingestion and pilot

- Before each pull, the workflow hydrates manifest-backed Markdown from Azure,
  verifies every content hash, and restores FilingForge's `news_id` ledger.
  FilingForge then downloads only unseen filings while the complete accumulated
  corpus remains available for manifest generation and research analysis.
- Start the controlled pilot with 5-10 companies over two years. For every
  company, confirm the workflow logs the hydration count, new/skipped filing
  counts, `analysis=True` or an explicit availability status, and the final
  Azure verification line.
- Manually audit at least five evidence quotes per company against their cited
  Markdown, every guidance/deliverable classification, and every
  walk-the-talk status before increasing the company universe.

## Known POC boundaries

- FilingForge does not OCR image-only PDFs. The manifest should eventually
  preserve an `extraction_status` for these gaps.
- The POC analyzes only a bounded document sample. Production analysis will use
  staged evidence extraction over all high-value artifacts.
- NVIDIA API Catalog is a trial endpoint, not a production SLA. Ingestion must
  still succeed when analysis is disabled; query-time Research AI will use
  lexical fallback if NVIDIA reranking is unavailable.
- NVIDIA extraction uses 12,000-character windows, a 4,096-token response cap,
  reasoning disabled, two attempts, and a 60-second timeout. After two failed
  windows the company is marked analysis-unavailable, but its Markdown and
  manifest still upload successfully.
- NVIDIA synthesis uses `nvidia/nemotron-3.5-lightning-30b-a3b` by default,
  streams the response, and sends `temperature=0.2`, `top_p=0.95`,
  `max_tokens=32768`, thinking enabled, and `reasoning_budget=8192`.
- Financial statements, shareholding, corporate actions, and fundamental
  screening require deterministic XBRL/feed ingestion into Azure SQL. LLM
  extraction is evidence for narrative research, not the numeric source of
  truth.

## Troubleshooting

### `AADSTS700024: Client assertion is not within its valid time range`

Use the current two-phase workflow. Older workflow versions authenticated to
Azure before downloading hundreds of filings, so the OIDC assertion had expired
before the Python SDK first requested a Storage token. No secret changes are
required. Re-run the workflow after updating both `filingforge-poc.yml` and
`filingforge_poc.py`.

The current workflow also runs a frontloaded preflight before FilingForge. It
sends a tiny JSON completion to each distinct configured NVIDIA model before
checking Azure. A misconfigured account, expired/invalid OIDC setup, missing
data-plane role, invalid NVIDIA key, retired model, or failed completion now
fails before the long download starts.

GitHub-hosted runners are ephemeral, but successful uploads are now hydrated
from Azure on the next run. Files downloaded by a failed run before its upload
phase remain unavailable and must be fetched again.

### `TableClient.query_entities() missing query_filter`

Update `filingforge_poc.py` to the current version. Azure Tables SDK 12.7.0
requires an explicit query filter even for a one-row preflight probe. The
current implementation queries the known `FILINGFORGE_POC` partition and works
whether the table is empty or populated.

### `NVIDIA_NIM_MODEL must include its publisher prefix`

The model variable must contain the slash after `nvidia`. For example, use
`nvidia/nemotron-3-ultra-550b-a55b`, not
`nvidianemotron-3-ultra-550b-a55b`.

## Next build phase

After the POC is accepted:

1. Add official XBRL parsers and normalized Azure SQL fact tables.
2. Add current/TTM company snapshots with indexes for cross-company fundamental
   filters and screeners.
3. Add query-time lexical retrieval and NVIDIA's hosted
   `llama-nemotron-rerank-1b-v2`; we build the integration, not the model.
4. Add portfolio/watchlist/sector scope resolution.
5. Add the detailed Fundamentals UI panels for overview, positives, risks,
   deliverables, management guidance, and walk-the-talk.
6. Add historical replay so old guidance is evaluated only against documents
   filed after that guidance.
