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

## Certificates of the systems it connects to

These are the certificates of the systems NCC Orchestrator connects *to*:
Prism, the mail server, webhook and chat receivers, and sign-in providers.
If they come from your internal CA, the simplest fix is to trust that CA
once in the operating system (below). Turning off verification
(`insecure-skip-verify`) also accepts a man-in-the-middle's certificate, so
avoid it.

### Trust your CA in the operating system

Every connection trusts the operating system's certificate store unless it
has a CA of its own configured, so one import covers all of them.

| Platform | Where to import the CA |
| --- | --- |
| Windows | **Local Computer** → *Trusted Root Certification Authorities* (`certlm.msc`, or `certutil -addstore Root ca.crt` elevated, or group policy). Not *Current User*: the service runs under another account and does not see your user store. |
| Linux | Debian/Ubuntu: copy to `/usr/local/share/ca-certificates/ca.crt` and run `update-ca-certificates`. RHEL/Rocky: copy to `/etc/pki/ca-trust/source/anchors/` and run `update-ca-trust`. `SSL_CERT_FILE` / `SSL_CERT_DIR` override the system bundle. |
| macOS | The **System** keychain, set to *Always Trust* (`sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ca.crt`). |
| Docker / Kubernetes | The containers do not see the host's store, and the images do not keep extra CAs (`ncc-host-cas.pem` is used only while the image is built). Mount a PEM bundle that contains the public roots **and** your CA, and set `SSL_CERT_FILE` to it, or use the per-connection settings below. |

**Restart the stack afterwards** (`ncc-orchestrator v2-restart`, or restart
the service or pods). Each process reads the store once, when it starts.

### What uses the store

| Connection | Trusts the OS store | Its own setting |
| --- | --- | --- |
| Prism Element and Prism Central | Yes | `ca-bundle` **adds** CAs to the store; `pin-sha256` accepts only the pinned certificates. See [`FEATURES_AND_CONFIG_FLAGS.md` §2.11a](../FEATURES_AND_CONFIG_FLAGS.md#211a-tls-trust-custom-ca-bundle-and-certificate-pinning). |
| Webhook, Slack and other chat notifications sent after a run | Yes | `ca-bundle` applies here too. |
| **Send test** and API-server alerts (self-heal, digest) | Yes | None: `ca-bundle` is not used, so trust the CA in the store. |
| Email | Yes | None; `smtp-insecure-skip-verify` only turns verification off. See [Notifications → Email](NOTIFICATIONS.md#email). |
| OpenID Connect, SAML metadata, audit forwarding | Yes | None. |
| LDAP / Active Directory | Only while **CA certificate (PEM)** is empty | A pasted CA (or `--ldap-ca-file`) **replaces** the store for that directory: only that CA is trusted. |
| UI server → API server | Only while `--backend-ca-file` is unset | The file **replaces** the store for that connection. |

So with your CA in the operating system, leave the LDAP CA field and
`--backend-ca-file` empty unless you want those connections to trust *only*
that CA.
