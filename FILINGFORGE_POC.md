# FilingForge Research POC

This POC uses the FilingForge **Python engine**, not its MCP server. Python is
the deterministic batch interface for GitHub Actions; MCP remains useful later
for interactive developer exploration of a local library.

## What the POC does

1. Pulls BSE filings with a pinned FilingForge commit. Ambiguous names can be
  pinned as `name|BSE-scrip-code`.
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
  --parameters storageAccountName=$storageAccount githubPrincipalId=$($sp.id)
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
- `NVIDIA_NIM_MODEL`: a tested chat-completions model, initially the same model
  used by the announcement Function App

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

## Known POC boundaries

- A fresh GitHub runner makes FilingForge enumerate/download the selected
  history again. Scaling requires a remote-manifest-aware fetch adapter so
  already-held filings are skipped before download.
- FilingForge does not OCR image-only PDFs. The manifest should eventually
  preserve an `extraction_status` for these gaps.
- The POC analyzes only a bounded document sample. Production analysis will use
  staged evidence extraction over all high-value artifacts.
- NVIDIA API Catalog is a trial endpoint, not a production SLA. Ingestion must
  still succeed when analysis is disabled; query-time Research AI will use
  lexical fallback if NVIDIA reranking is unavailable.
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

The current workflow also runs a frontloaded preflight before FilingForge. A
misconfigured account, expired/invalid OIDC setup, missing data-plane role, bad
NVIDIA key, or unavailable model now fails before the long download starts.

GitHub-hosted runners are ephemeral, so a failed run's downloaded files are not
available to a later run. The first retry must download them again. Subsequent
successful production work will add remote-manifest-aware incremental pulls.

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
