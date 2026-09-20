# Azure OpenAI

Model Harbor can route named Azure deployments through the same local bridge as your other providers. Each Codex task keeps its own model choice.

## Model catalog versus your deployments

Azure is the hosting and billing connection. Model authors supply the models; a deployment is a named instance made available through your Azure resource. Harbor sends that deployment name in API requests.

**Find deployments** lists ready deployments from your resource. The **Browse Azure’s full model catalog** link opens Microsoft Foundry’s broader catalog, which includes models from several authors. A catalog listing does not mean your endpoint can call that model: region, resource type, access and a suitable deployment still matter. Harbor adds only the deployments you choose and successfully verify. It does not automatically create Azure deployments or add the full catalog to Codex.

## Connect

1. Open **Add provider → Azure → Connect** in Harbor.
2. Enter your resource endpoint, such as `https://your-resource.openai.azure.com/openai/v1/`. Direct `services.ai.azure.com` endpoints are also accepted. Redirects, URL credentials, custom ports, query strings, and non-Azure hosts are rejected.
3. Enter the resource API key in the secure field. It is saved in macOS Keychain and held in memory by the local bridge while Harbor is running.
4. Click **Find deployments** to check the key and load ready deployments from your resource. Choose one from the picker, or enter its exact name manually if the resource does not expose discovery. The chooser shows the underlying model beside custom deployment aliases and displays the model version when Azure reports it. Missing model metadata is labeled as unavailable rather than inferred from the deployment name. Harbor lists deployments, not the separate catalog of base models. Discovery does not create deployments or run inference.
5. In **Deployment options**, select a reasoning setting, enable image input if supported, and set a context token limit from the deployed model's specifications. The conservative default is 128,000 tokens; it is not a discovered capacity. Deployment default omits reasoning controls from requests.
6. Choose whether to use this deployment for new tasks and hide Baseten from both Harbor and the Codex picker.
7. Click **Verify & add deployment**. This sends one small, billable Responses request and requires a completed function call. When image input is enabled, the check also includes a tiny test image. A failed check does not save the new connection. The form shows elapsed time and a Cancel action; discovery stops after 20 seconds and verification after 60 seconds.
8. Reopen Codex once to load newly added catalog entries. Select the named Azure deployment in an existing Harbor task to move that task to Azure. Existing choices are never migrated automatically.

Add more deployments using **Manage connection → Add or update deployment**. Enter an existing deployment name to update its options. A connection is pinned to one Azure resource; additional resources can use the separate custom-provider workflow.

Only the reasoning setting selected during setup is advertised for that deployment. Harbor applies that verified setting even if an older task inherits a different effort. To change it, update the deployment options and verify again. This avoids advertising unsupported effort levels. The connection check is a basic protocol check, not proof of every Codex tool or workload.

## Visibility and defaults

Open **Settings → Models** or **Manage models…** on a connection page. Turn a provider off to hide all its models from Codex, including models added by future Harbor updates. Individual model checkboxes let you keep a smaller shortlist. Turning a provider back on restores your individual choices. The new-task default must remain visible; choose another default before hiding it.

**Settings → Providers → Visible providers** separately controls Harbor tabs and the usage picker. Hiding models or tabs preserves credentials and existing task routes. Choosing Azure as the new-task default affects future tasks; it does not change tasks already using Baseten.

Codex loads the custom catalog at startup. Reopen Codex when active tasks are finished to refresh the picker after adding or hiding entries. Switching between entries already loaded does not need a restart. Harbor cannot insert an “Add more” action into Codex’s native model menu; model management lives in Harbor.

## Connection and performance

Azure uses `/openai/v1/responses` with the resource key in `api-key`. Harbor never forwards the Codex subscription token to Azure, never substitutes another model, and does not place Azure requests in the Baseten queue. Namespaces and custom tools are translated to function tools. Tool-result images retain their original data and call IDs and are moved into a labeled following message.

Each generated Azure catalog entry sets `auto_review_model_override` to its own full Harbor route, such as `harbor/azure/gpt-5.6-sol`. This explicitly keeps automatic review on that deployment, including when an imported catalog names a different review model. The hidden compatibility entry retains the route of its pinned Azure deployment. Harbor generates this metadata; no separate hand-edited Codex model bundle is needed.

This is routing hardening, not a proven explanation for the reported incident. Current Codex can fall back to the parent model when an automatic-review model is unavailable. A missing override alone does not establish that review used another model, and a passing catalog-generation test does not prove what an already-running desktop loaded.

Azure still applies deployment quotas and capacity limits. HTTP 429 is returned with available retry headers. Harbor permits one bounded pre-body POST retry when no model bytes were possible. Harbor never replays an inference request after dispatch. The verified `gpt-5.6-sol` deployment uses Azure stored background responses for streaming. If its SSE connection drops after Azure supplies a response ID and sequence number, Harbor resumes that same provider response with Azure's `GET ...?stream=true&starting_after=...` operation inside the original request deadline. The stored response is deleted afterward. Other deployments keep the previous one-POST, non-resumable behavior unless their catalog explicitly enables the capability after equivalent validation. Automatic context compaction remains Codex-owned. A compaction event near a context limit does not establish the cause of an Azure disconnect. The new provider does not guarantee lower latency. Test your actual workload and inspect Azure's deployment metrics when evaluating performance.

Azure inference keys do not provide a Harbor billing integration. Usage shows that Azure quota and spend are unavailable and links to the Azure dashboard. Use Azure Cost Management for billed charges.

## Troubleshooting

- **401/403:** verify the key belongs to the resource and that network access rules allow this Mac. Reconnect after correcting the problem.
- **404:** check the exact deployment name and endpoint.
- **400:** confirm Responses, tool calls, the selected effort, and image input are supported. Try Deployment default and disable image input to isolate a capability mismatch.
- **429:** inspect the deployment's quota and available capacity; wait for the provider cooldown.
- **Incomplete tool check:** choose a lower effort or a deployment that completes the tool check within its output budget.
- **Bridge cannot accept Azure settings:** finish active requests and reopen the updated Harbor app once. An older running bridge cannot load new provider code until restarted.

## References

- [Microsoft: Azure OpenAI v1 API](https://learn.microsoft.com/en-us/azure/foundry/openai/api-version-lifecycle)
- [OpenAI: Codex configuration reference](https://developers.openai.com/codex/config-reference/)

## Continuing a task from another provider

Harbor replays portable conversation text, tool calls, and tool results when you switch to Azure. It removes provider-owned item IDs from inline history while preserving each `call_id` that links a tool result to its call. This also applies when Codex custom tool results are converted to standard function results; a `ctco_` ID must not be forwarded on the converted item.

Encrypted reasoning is provider-owned. Harbor forwards it only when it was previously returned by the same Azure resource and deployment. Encrypted items from Codex subscription, another provider, another Azure resource, or another Azure deployment are removed before dispatch. Same-binding Azure encrypted reasoning is retained unchanged so tool continuations can use it. The registry stores only bounded SHA-256 provenance digests, not ciphertext or conversation content, and survives gateway restarts. API-key rotation on the same resource does not invalidate that provenance.

No saved task history is rewritten. The first request after an upgrade that introduces provenance tracking drops older unregistered encrypted items conservatively while retaining the portable history.


## Staged stream admission candidate

The September 17 safe-runtime candidate limits each Azure endpoint/deployment to two active gateway requests and sixteen waiting requests. A FIFO waiter can remain queued for at most 30 seconds. These are local policy defaults, not discovered Azure quotas. A slot remains occupied through the complete response or terminal stream event and upstream connection close. Gateway readiness probes use the same queue and a shorter ten-second wait budget. Queue cancellation checks use intervals no longer than 100 ms; this is not an active-stream cancellation guarantee.

A full or expired queue returns local HTTP 503 with Retry-After: 1. Cancelled queued requests never call Azure and do not mark their task delivery uncertain. A local busy/cancelled readiness probe preserves the existing provider proof. The authenticated status response exposes only aggregate Azure admission counters.

This admission policy is process-local. It does not coordinate multiple gateway runtimes, discover account limits, or replay Azure inference. The candidate also applies a [shared queue-to-response deadline](azure-request-policy.md). Upstream statuses and Retry-After headers retain their original values and each explicit request makes one inference POST. Explicitly capable deployments may use same-response recovery GETs as described above. Effective retry settings in the already-running Codex desktop remain unverified. The candidate is staged separately until coordinated activation is recorded.
