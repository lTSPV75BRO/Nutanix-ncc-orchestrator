# HTTPS and certificates

The dashboard is served over HTTPS from the first start. With no certificate
supplied, the server makes a **self-signed** one, and plain `http://` requests
on the same port are redirected to `https://`. That is enough to try it out,
but browsers warn about a self-signed certificate. For daily use, install a
certificate your browsers already trust.

Everything here is in **Settings → Access → HTTPS** (admin only).

## Option 1: your own certificate (recommended)

1. Ask your PKI team (or a public CA) for a certificate for the name people
   will use, for example `ncc.example.com`. The name must be in the
   certificate's *Subject Alternative Names*.
2. You need two PEM files: the **certificate chain** (your certificate first,
   then any intermediates) and its **private key** (unencrypted).
3. **Settings → Access → HTTPS → Advanced — bring your own certificate.** Paste
   both and click **Enable HTTPS & restart** (or **Replace certificate**).
4. The services restart (a few seconds) and the page reconnects. Check the
   padlock in the browser.

The private key is stored with `0600` permissions and never shown again.
Session cookies are marked `Secure` from then on.

To convert other formats:

```bash
# .pfx / .p12 (Windows) to PEM
openssl pkcs12 -in ncc.pfx -clcerts -nokeys -out ncc.crt
openssl pkcs12 -in ncc.pfx -nocerts -nodes -out ncc.key
# add the intermediate CA after your certificate
cat ncc.crt intermediate.crt > ncc-chain.crt
```

## Option 2: keep the self-signed certificate and trust it

Good for a lab or a small team.

1. **Settings → Access → HTTPS → Generate self-signed certificate** (or
   **Renew**) creates one for the name you opened the dashboard with.
2. Click **Download certificate** and add it to the trusted roots on each
   computer (Windows: *Trusted Root Certification Authorities*; macOS:
   Keychain Access, *Always Trust*), or push it with group policy / MDM.

The sign-in page also offers the certificate under **Browser warns about the
certificate?**, with its SHA-256 fingerprint so people can compare it with the
one you give them.

## Kubernetes

The UI pods keep the certificate on the shared volume and reload it without a
restart, so the same HTTPS page works. If HTTPS ends at your Ingress instead,
the page says so and the certificate is managed there. See
[`k8s/README.md`](../../k8s/README.md).

## Startup flags

For automated installs: `ncc-orchestrator v2-start --ui-tls-cert-file
ncc-chain.crt --ui-tls-key-file ncc.key`. `--ui-insecure-http` serves plain
HTTP, which breaks SAML sign-in and sends passwords unencrypted, so only use
it behind a proxy that terminates TLS.

## Prism and SMTP certificates

These are the certificates of the systems NCC Orchestrator connects *to*:

- **Prism Element / Prism Central** with an internal CA: set `ca-bundle` (a PEM
  file) or pin the certificate with `pin-sha256` in the config instead of
  turning off verification. See
  [`FEATURES_AND_CONFIG_FLAGS.md` §2.11a](../FEATURES_AND_CONFIG_FLAGS.md#211a-tls-trust-custom-ca-bundle-and-certificate-pinning).
- **Mail server:** see [Notifications → Email](NOTIFICATIONS.md#email).
