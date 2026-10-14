# Security

- Never send the RUCKUS One Shared Secret through chat or screenshots.
- The prototype stores credentials locally in `config/settings.json`.
- Restrict access to the Windows account running the service.
- For production, move secrets to Windows Credential Manager or another OS-backed secret store.
- Prefer a Read-Only application token for the initial integration.
