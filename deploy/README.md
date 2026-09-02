# VM provisioning templates

These templates install the complete v2 stack from a published release:
orchestrator, API server, UI server, and frontend assets. They verify the
downloaded stack archive against `checksums.txt` before installation.

## Linux cloud-init

Use [`cloud-init/ncc-v2.yaml`](cloud-init/ncc-v2.yaml) on Ubuntu/Debian or
RHEL/Rocky. It detects `amd64`/`arm64`, installs prerequisites, creates
`/etc/ncc/config.yaml` using the canonical schema, and registers the stack with
the native boot service.
The UI uses the default HTTPS behavior. When installed, `ufw` or `firewalld`
rules are updated for the standard UI/API ports.

Customize these values in the cloud-init file before provisioning; cloud-init
writes them to `/etc/ncc/ncc-install.env`:

```yaml
NCC_VERSION="2.2.0"
NCC_RELEASE_BASE_URL="https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download"
NCC_CLUSTERS=""
NCC_PCS=""
NCC_USERNAME="admin"
NCC_PASSWORD="<inject securely at first boot>"
```

The install root and configuration are under `/opt/ncc-v2`; all runner,
API, UI, scheduler, and supervisor logs are kept under `/opt/ncc-v2/logs`.
The installer settings are kept in `/etc/ncc/ncc-install.env`. Inject the
password securely, configure at least one cluster or Prism Central target, and ensure the VM can
reach the required Nutanix endpoints.

## Windows Sysprep

Copy [`windows/setup-ncc.ps1`](windows/setup-ncc.ps1) and
[`windows/Unattend.xml`](windows/Unattend.xml) to `C:\NCC\` in the reference
image. Edit the environment values in `Unattend.xml`, especially
`NCC_VERSION`, `NCC_CLUSTERS`, and `NCC_PCS`. Inject `NCC_PASSWORD` securely,
then run:

```powershell
sysprep.exe /generalize /oobe /shutdown /unattend:C:\NCC\Unattend.xml
```

During the `specialize` pass, the script downloads and checksum-verifies the
Windows `amd64` stack archive, installs it under
`C:\ProgramData\NCC\v2`, creates the config and output directories, opens
private/domain firewall rules for ports 8080 and 8081, and registers the
`NCC Orchestrator v2` scheduled task to start at boot.

The templates intentionally do not embed production credentials. Use an image
secret-injection mechanism before first run.
For production, prefer an internal artifact mirror by setting
`NCC_RELEASE_BASE_URL` rather than relying on public GitHub availability.

Runtime API/UI logs rotate at 50 MiB, retain five compressed backups, and keep
30 days of history. The orchestrator's `doctor` log-size check monitors
`logs/*.log`; run `ncc-orchestrator doctor --fix` to rotate an oversized log
immediately if needed. The Linux template also installs a 15-minute systemd
timer, and the Windows template installs an equivalent scheduled task, so
oversized logs are checked and rotated automatically.
