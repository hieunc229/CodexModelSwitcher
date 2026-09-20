# Context and stream repair, 2026-09-18

Candidate branch: `codex/context-stream-fix`, based on installed integration source `b72ad38`.

- Native Sol metadata observed: default 272,000; maximum 872,000.
- Live Azure and merged catalogs corrected and reread successfully.
- Backup: `/Users/andrewn/.codex/model-catalogs/context-repair-backup-20260918-135650`.
- Integrated tests: 103 Swift and 421 Python passed.
- Additional real HTTP Azure EOF regression: 12 deadline/EOF tests passed. Partial output is preserved, failure is explicit, uncertain delivery is retained, a repeated turn is rejected, and the dispatched stream interruption makes only one upstream POST.
- Signed Xcode build succeeded using the existing Apple Development identity. Strict signature verification passed.
- Stage: `build/staged-updates/context-stream-20260918/Model Harbor.app`; manifest is beside it.
- Backend activation: completed through user-authorized coordinated maintenance on 2026-09-18. Transaction `36e26cc5-5690-46e2-b691-521dce465390` ended in `resumed`. All captured clients resumed; no Codex process remained stopped.
- Desktop catalog reload: unverified. On-disk correction does not prove the loaded task adopted new limits.
- Live provider verification: Azure Sol and OpenRouter Fable passed isolated preflight and current-boot restoration probes. Both readiness flags are true. Five subsequent Codex-subscription requests completed with zero failures at the verification point.

The screenshot proves a stream ended without completion. It does not identify whether the cause was an upstream disconnect, a downstream client disconnect, a deadline, or a bridge exception. This repair fixes missing error signaling; it does not claim to eliminate provider outages.

See `../codex-router-comparison.md` for the source comparison and remaining work.

## Installed activation

Runtime `12082cbe94b9b3619196f3317a9fc4999926dd309eac0c891d01dcd6c5f09564`, boot `58ad39e6-ee41-407f-bd70-6b1ad65d82c6` is active. The maintenance controller confirmed preserved ownership and shared configuration. Its admission marker is cleared.

The matching signed interface is installed at `/Applications/Model Harbor.app`; its inventory matches the staged manifest. The prior interface is retained under `~/Library/Application Support/Model Harbor/updates/context-stream-20260918-ui/`. Interface replacement preserved the gateway boot identity and configuration revision. The AppleScript quit returned cancellation; only the verified independent gateway’s interface process was subsequently terminated and reopened. The inference gateway was not stopped by that interface update.

Both live catalog files were reread after reopening Harbor and still contain 272,000 / 872,000 for Azure Sol. No Codex restart or conversation replacement was performed. Loaded desktop catalog adoption remains unverified.
