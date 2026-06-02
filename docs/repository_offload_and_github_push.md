# Repository Offload and GitHub Push Safety

This note documents the helper scripts used to keep the Databento worktree GitHub-safe while moving large generated datasets/logs to the external Databento vault.

## External vault location

Default vault mount:

```bash
/Volumes/DatabentoVault
```

Default offload destination:

```bash
/Volumes/DatabentoVault/trading-agent-offload/databento
```

You can override the mount or destination with:

```bash
DATABENTO_VAULT=/Volumes/DatabentoVault scripts/offload_large_artifacts_to_vault.sh --execute
scripts/offload_large_artifacts_to_vault.sh --dest-base /Volumes/DatabentoVault/trading-agent-offload/databento --execute
```

## Scripts

| Script | Purpose | Typical command |
| --- | --- | --- |
| `scripts/git_size_audit.py` | Audits GitHub push risk: large reachable Git blobs, staged/modified large files, and untracked non-ignored files over GitHub limits. | `python3 scripts/git_size_audit.py` |
| `scripts/estimate_github_push_size.py` | Builds a temporary commit from the current staged index and estimates the pack size that would be pushed to `origin`. | `python3 scripts/estimate_github_push_size.py` |
| `scripts/estimate_offload_savings.py` | Estimates local disk that will be saved by offloading generated Databento artifacts and replacing them with symlinks. | `python3 scripts/estimate_offload_savings.py` |
| `scripts/offload_large_artifacts_to_vault.sh` | Moves generated datasets/logs/model-export runs to the vault and replaces local paths with symlinks. Defaults to dry-run. | `scripts/offload_large_artifacts_to_vault.sh --execute` |
| `move_snapshot_to_databento_disk.sh` | Moves Databento runtime snapshot folders to the vault and leaves a symlink behind. | `./move_snapshot_to_databento_disk.sh --all` |

## Safe run order before pushing

1. Confirm the vault is mounted and writable:

```bash
ls -ld /Volumes/DatabentoVault
df -h /Volumes/DatabentoVault
touch /Volumes/DatabentoVault/.databento_write_check
rm /Volumes/DatabentoVault/.databento_write_check
```

2. Estimate offload savings:

```bash
python3 scripts/estimate_offload_savings.py
```

3. Preview offload actions:

```bash
scripts/offload_large_artifacts_to_vault.sh
```

4. Execute the offload:

```bash
scripts/offload_large_artifacts_to_vault.sh --execute
```

5. Audit GitHub push size risk:

```bash
python3 scripts/git_size_audit.py
python3 scripts/estimate_github_push_size.py
git count-objects -vH
```

## Resume after accidental external-disk removal

If the external disk is unplugged during offload:

1. Reconnect the drive.
2. Verify it is mounted and writable:

```bash
ls -ld /Volumes/DatabentoVault
df -h /Volumes/DatabentoVault
touch /Volumes/DatabentoVault/.databento_write_check
rm /Volumes/DatabentoVault/.databento_write_check
```

3. Re-run the same execute command:

```bash
scripts/offload_large_artifacts_to_vault.sh --execute
```

The offload script uses `rsync -a --partial`, so already-copied files are reused where possible. It only removes the local source after `rsync` succeeds and after the vault passes a fresh write check.

### Snapshot symlink conflict

If `runtime/databento/snapshots` already contains per-snapshot symlinks into the vault, `rsync` may report errors like:

```text
unlinkat: Directory not empty
```

This means the local child is a symlink while the vault child is the real directory. The offload helper detects directories that contain only symlinks pointing into the destination and replaces the parent directory with one symlink to the vault instead of trying to copy child symlinks over real directories.

You can repair just this path with:

```bash
scripts/offload_large_artifacts_to_vault.sh --execute runtime/databento/snapshots
```

Monitor progress:

```bash
tail -f /tmp/databento_offload_execute.log
```

## Current measured baseline

Measured on 2026-06-01 after the resumed offload completed:

```text
workspace_current=14.05 GiB
selected_to_offload=0.00 GiB
projected_workspace_after_symlinks=14.05 GiB
vault_offload_size=1.1 TiB
vault_free_space=3.4 TiB
```

The GitHub push risk audit after ignoring/untracking generated artifacts showed:

```text
Staged added/modified/copied/renamed files >=50 MiB: count=0
Modified tracked working-tree files >=50 MiB: count=0
Untracked non-ignored files >=100 MiB: count=0
Git pack size: about 64 MiB
```

After staging active runtime properties and offload symlink placeholders:

```text
staged_symlink_count=184
staged_active_runtime_bot_properties=100
staged_runtime_top_level_trading_properties=6
staged_worktree_payload_uncompressed=1.53 MiB
estimated_push_pack_size=106.87 MiB
github_100mb_blob_violations=0
```

Decision: the staged branch remains well under a 2 GiB per-push limit. The estimated storage increase on GitHub is about 107 MiB, plus existing repository history already present on the remote.

Decision: the repository does not need to be split into two pushes for the 2 GiB push limit once generated artifacts are ignored/untracked. Two commits are still recommended for review clarity:

1. Repository hygiene/offload tooling and artifact removals from Git.
2. Databento feature/code changes.






