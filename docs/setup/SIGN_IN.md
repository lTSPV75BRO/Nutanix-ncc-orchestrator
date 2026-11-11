# Set up sign-in: local accounts, Active Directory, OpenID Connect and SAML

This guide gets people signed in to the dashboard with the right role. Every
step happens in the browser unless a step says otherwise.

- [Before you start](#before-you-start)
- [Roles](#roles)
- [Local accounts](#local-accounts) (on by default)
- [Active Directory / LDAP](#active-directory--ldap)
- [OpenID Connect](#openid-connect) (Microsoft Entra ID, Okta, Google, Keycloak)
- [SAML](#saml) (Entra ID, Okta, ADFS)
- [Limit people to some clusters](#limit-people-to-some-clusters)
- [Check it works](#check-it-works)
- [Troubleshooting](#troubleshooting)

The full technical reference (flags, API endpoints, security details) is in
[`SECURITY_AND_TRUST.md`](../SECURITY_AND_TRUST.md#api-server-access-control-rbac--login).

## Before you start

- **Sign in as `admin`.** On first start the server creates `admin` with a
  random password. It is in `.ncc-initial-admin-password` in the install folder
  (Kubernetes: the `initial-admin-password` key of the users Secret, or the API
  pod log). You are asked to change it at first sign-in.
- **Use the address people will use.** SSO providers send people back to a
  fixed URL, so decide the dashboard address first, for example
  `https://ncc.example.com` or `https://ncc-host:8080`. Open the dashboard at
  that address while you set things up.
- **HTTPS.** The dashboard uses HTTPS by default with a self-signed
  certificate. SAML needs HTTPS. For production, install a certificate your
  browsers trust: [HTTPS and certificates](HTTPS.md).
- All sign-in settings are under **Settings → Access → Sign-in → External
  authentication**. The **Provider** list only picks which form you edit; you
  can turn on OIDC, SAML and LDAP at the same time.

## Roles

| Role | Can |
| --- | --- |
| `viewer` | See the dashboard, findings, insights, runs and the schedule. |
| `operator` | Everything a viewer can, plus start and cancel runs, change the schedule, acknowledge and resolve findings, and send test notifications. |
| `admin` | Everything, including settings, users, sign-in, backups and updates. |

Directory and SSO users get their role from **group mapping**: you list which
group gives which role, and the highest match wins. Anyone who matches no group
gets the **default role** (`viewer` unless you change it). With OpenID
Connect you can also set it to `none` to refuse people outside your mapped
groups; for SAML and LDAP, limit who can sign in at the provider (assign the
app, or use a narrower **User filter**).

## Local accounts

Local accounts work out of the box and always work, even when the directory or
SSO provider is down. Keep `admin` (or another local admin) as a break-glass
account.

1. Go to **Settings → Access → Users & roles → Add user**.
2. Enter a username, pick a role and set a temporary password. The user must
   change it at first sign-in.
3. Send the person the dashboard address, username and temporary password.

People who forget their password use **Forgot password?** on the sign-in page;
the request shows up under **Users & roles** for an admin to reset. If you lose
the `admin` password, reset it on the server:

```bash
ncc-orchestrator v2-reset-password --user admin
```

## Active Directory / LDAP

People sign in on the normal form with their usual Windows username
(`jdoe`) or UPN (`jdoe@corp.example.com`). Local accounts are checked first,
then the directory.

**You need from your AD team:**

- A domain controller address, ideally LDAPS: `ldaps://dc1.corp.example.com:636`.
  Add a second one after a comma for failover.
- A read-only **service account** (its DN and password), used only to look
  users up.
- The **base DN** to search, for example `DC=corp,DC=example,DC=com`.
- The DNs of the groups that should map to roles, for example
  `CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com`.
- If the DC uses an internal CA: the CA certificate (PEM).

**Steps**

1. **Settings → Access → Sign-in → External authentication**, Provider:
   **LDAP / Active Directory**.
2. Turn on **Enable LDAP/AD login** and fill in:

   | Field | Example |
   | --- | --- |
   | Server URL(s) | `ldaps://dc1.corp.example.com:636,ldaps://dc2.corp.example.com:636` |
   | CA certificate (PEM) | Paste the internal CA if browsers/servers don't already trust it |
   | Service account DN | `CN=ncc-svc,OU=Service Accounts,DC=corp,DC=example,DC=com` |
   | Service account password | The service account's password (never shown again) |
   | Base DN | `DC=corp,DC=example,DC=com` |
   | Group → role mapping | One per line: `CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin` |

   Leave **User filter**, **Username attribute** and **Group attribute** empty
   for Active Directory; the defaults (`sAMAccountName` or UPN, `memberOf`) fit.
   You can map a group by its full DN or just its CN (`NCC-Admins=admin`).
3. Click **Test connection…** and sign in as a real AD user. The result shows
   whether the bind worked, which groups were found and the role they would get.
4. **Save LDAP configuration.** It applies right away, no restart needed.

With plain `ldap://`, turn on **StartTLS** so passwords are not sent in clear
text. Use **Skip TLS verification** only for a short test.

**A directory user with the same name as a local account can't sign in.**
Local accounts are checked first. If the password doesn't match the local
account, the directory is not used under that name: an AD `admin` gets
"invalid username or password". Rename one of them, or set **Username
attribute** to `userPrincipalName` (see below).

### Several domains or forests

Several **Server URL(s)** in one directory are failover servers for the
*same* directory, tried in order. For more than one domain or forest, pick
one of these:

**Child domains in one forest, one Global Catalog.** A Global Catalog can
find users in every domain of its forest, so one directory is enough:

| Field | Value |
| --- | --- |
| Server URL(s) | A Global Catalog: `ldaps://gc.corp.example.com:3269` (plain GC is port 3268) |
| Base DN | The forest root: `DC=corp,DC=example,DC=com` |
| Username attribute | `userPrincipalName` |

People then sign in with their UPN, `jdoe@emea.corp.example.com`. That is
also the name the app stores, so `jdoe` in two domains are two different
people. If the same short name exists in two domains, signing in with just
`jdoe` fails with "matched 2 entries", and the UPN works. For role mapping use
**universal** groups: a Global Catalog may not show global-group membership
from other domains.

**Separate forests, or domains with their own servers: directory profiles.**
The directory at the top of the form is the *default* directory. Under
**Other domains or forests**, click **Add directory** once for each further
domain or forest, and give it its own server URLs, base DN, service account,
CA certificate and group → role mapping. Each directory claims the login
domains that pick it:

| Field | Example | Picks this directory for |
| --- | --- | --- |
| UPN suffixes | `emea.corp.example.com` | `jdoe@emea.corp.example.com` |
| NetBIOS domains (optional) | `EMEA` | `EMEA\jdoe` |

The default directory can claim its own domains too (for example `CORP`, so
`CORP\jdoe` works as well as `jdoe`).

How a sign-in is routed:

- **With a domain, one directory only.** `jdoe@<suffix>` goes to the
  directory that claims the suffix, and `<NETBIOS>\jdoe` to the directory
  that claims the NetBIOS domain. No other directory is asked. A domain no
  directory claims goes to the default directory, as before profiles existed.
- **A short name (`jdoe`) follows the order of the directories:** the default
  directory first, then the added ones from top to bottom. The app moves to
  the next directory only when the current one has **no such user**. The
  first directory that has the user checks the password and decides:
  - right password: signed in;
  - wrong password, or a locked or disabled account: the sign-in fails, and
    the later directories are **not** tried. One password is never tried
    against every forest, and a typo can't lock out the same name in other
    forests;
  - a directory that can't be reached (or whose service account fails) also
    stops the sign-in, so its users' passwords aren't sent to the next
    directory during an outage.

Example: `user1@dc1.com` lives in the default directory and `user1@dc2.com`
in an added `dc2` directory. Signing in as `user1` always means dc1's user1.
dc2's user1 signs in as `user1@dc2.com` (or `DC2\user1`). A user who exists
only in dc2, say `pat`, can sign in as plain `pat`: dc1 has no `pat`, so dc2
is asked next.

The order is the default directory, then the **Other domains or forests**
list as shown. To change which forest wins a short name, make that forest
the default directory or move it up the list (remove and re-add it). The
**Server URL(s)** inside one directory are still failover servers for that
same directory, tried in order.

Names the app stores, so nobody in one forest can become someone in another:

- Users of an added directory are stored under their **UPN** in lower case
  (`jdoe@emea.corp.example.com`). Their UPN has to end in one of that
  directory's UPN suffixes, so list every suffix its users have (alternative
  UPN suffixes included); otherwise the sign-in is refused.
- Users of the default directory keep the name they had before (their
  `sAMAccountName`, or whatever **Username attribute** says), so existing
  users, tokens and cluster groups keep working.
- A suffix or NetBIOS domain can belong to one directory only; the save is
  refused otherwise.

Click **Test connection…** with each form of the name (`jdoe`,
`jdoe@emea.corp.example.com`, `EMEA\jdoe`): the result names the directory
that answered and the name and role the user would get. Saving checks the
connection of every directory.

In cluster groups, add users of other directories by their UPN (the AD
search offers them that way) and groups by their full DN: two forests can
each have a group called `NCC-Admins`, and a CN alone matches both.

Directory profiles are set in the UI or `PUT /api/v1/settings/ldap`. The
`--ldap-*` startup flags configure the default directory only.

If you'd rather not run several directories here, an identity provider that
already federates the forests (Entra ID, Okta, ADFS) works through OpenID
Connect or SAML.

**OpenLDAP and other directories:** set **User filter** (for example
`(uid=%s)`), **Username attribute** (`uid`) and **Group attribute** to match
your schema.

## OpenID Connect

OIDC works with Microsoft Entra ID, Okta, Google Workspace, Keycloak, Auth0,
Ping and ADFS 2019+. It is the easiest way to get MFA: the provider enforces
its own MFA and Conditional Access rules.

**Steps in the dashboard (first part)**

1. **Settings → Access → Sign-in → External authentication**, Provider:
   **OpenID Connect**.
2. Check **Dashboard URL** is the address people use (it defaults to the one
   you opened). The blue box at the top then shows the **redirect URI**, for
   example `https://ncc.example.com/api/v1/auth/oidc/callback`. Copy it.

**In your provider**, register a *web application* with that redirect URI (see
below), then come back with the **issuer URL**, **client ID** and **client
secret**.

**Steps in the dashboard (second part)**

3. Turn on **Enable OpenID Connect sign-in** and fill in **Issuer URL**,
   **Client ID** and **Client secret**.
4. Click **Test discovery**. It checks the issuer without saving.
5. **Role mapping:** `group=role` pairs, comma-separated, for example
   `ncc-admins=admin,ncc-ops=operator`. **Groups claim** is `groups` for most
   providers.
6. **Role when no group matches:** `viewer`, or `none` to refuse everyone
   outside the mapped groups.
7. **Sign-in button name:** what the button says, for example `Microsoft` gives
   "Sign in with Microsoft".
8. **Save OpenID Connect configuration**, then try it from a private window.

### Microsoft Entra ID

1. Entra admin center → **App registrations → New registration**. Name it
   `NCC Orchestrator`, single tenant.
2. **Redirect URI:** platform **Web**, paste the redirect URI.
3. **Certificates & secrets → New client secret.** Copy the *value*.
4. **Token configuration → Add groups claim →** *Security groups*, ID token.
5. Issuer URL: `https://login.microsoftonline.com/<tenant-id>/v2.0` (the
   tenant ID is on the app's Overview page). Client ID: the *Application
   (client) ID*.
6. Entra sends group **object IDs**, not names, so map those:
   `5f0c…e21=admin,8a3d…b77=operator` (copy each ID from **Groups**).

If a user is in more than 200 groups, Entra leaves groups out of the token;
assign the app to specific groups or use **Groups assigned to the
application** in the claim settings.

### Okta

1. **Applications → Create App Integration →** *OIDC*, *Web Application*.
2. **Sign-in redirect URIs:** paste the redirect URI. Assign the app to your
   users or groups.
3. **Sign On → OpenID Connect ID Token → Groups claim:** name `groups`,
   filter *Starts with* `ncc-` (or *Matches regex* `.*`).
4. Issuer URL: `https://<your-org>.okta.com` (or your custom authorization
   server, e.g. `https://<your-org>.okta.com/oauth2/default`).
5. Role mapping uses group names: `ncc-admins=admin,ncc-ops=operator`.

### Google Workspace

1. Google Cloud console → **APIs & Services → Credentials → Create
   credentials → OAuth client ID →** *Web application*.
2. **Authorized redirect URIs:** paste the redirect URI.
3. Issuer URL: `https://accounts.google.com`.
4. Google does not send groups. Set **Role when no group matches** to the role
   everyone should get (usually `viewer`), and give admins a local or AD
   account. Make the OAuth app *Internal* so only your Workspace users can
   sign in.

### Keycloak

1. Realm → **Clients → Create client**, type *OpenID Connect*, **Client
   authentication** on.
2. **Valid redirect URIs:** paste the redirect URI.
3. **Client scopes → `<client>-dedicated` → Add mapper → Group Membership**,
   token claim name `groups`, **Full group path** off.
4. Issuer URL: `https://<keycloak-host>/realms/<realm>`.

To try OIDC without a real provider: `go run ./tools/fakeoidc` starts a test
provider on `http://127.0.0.1:9800` with three test users.


## SAML

Use SAML when your identity team prefers it or the provider has no OIDC. SAML
needs the dashboard on HTTPS (the default).

**In your identity provider first**, create a SAML application. The values
below use `https://ncc.example.com` as the dashboard address; use yours.

| Setting | Value |
| --- | --- |
| Entity ID / Identifier / Audience | `https://ncc.example.com/saml/metadata` (or your **SP entity ID**, if you set one) |
| Reply URL / ACS URL / Single sign-on URL | `https://ncc.example.com/saml/acs` |
| Sign-on URL (optional) | `https://ncc.example.com/saml/login` |
| NameID | The username, for example `user.userprincipalname` or email |
| Group/role attribute | An attribute carrying the user's groups (see below) |

**Then in the dashboard**

1. **Settings → Access → Sign-in → External authentication**, Provider:
   **SAML single sign-on**.
2. Turn on **Enable SAML SSO** and set **Server root URL** to the dashboard
   address, for example `https://ncc.example.com`.
3. Paste the provider's **IdP metadata URL**, or paste its metadata XML in
   **…or paste IdP metadata XML**.
4. **Role/group attribute:** the *name* of the attribute that carries groups
   (exactly as the provider sends it). **Role mapping:** `value=role` pairs,
   for example `ncc-admins=admin,ncc-ops=operator`.
5. **Default role (no mapping match):** usually `viewer`. To keep people out
   entirely, assign the SAML app only to the right groups in the provider.
6. **Save SSO configuration**. The server creates its own signing key (it
   never leaves the server) and shows the **SP metadata URL**. If your
   provider wants to import SP metadata or the SP certificate, give it that
   URL. A **Sign in with SAML** button (or "single sign-on") now appears on
   the sign-in page.

**Allow IdP-initiated login** lets people start from the provider's app
portal (My Apps, Okta dashboard). Leave it off unless you need it; starting
from the dashboard is safer.

### Entra ID (SAML)

- **Enterprise applications → New application → Create your own →**
  *non-gallery*, then **Single sign-on → SAML**.
- Basic SAML configuration: Identifier and Reply URL from the table above.
- **Attributes & Claims → Add a group claim →** *Security groups*. The
  attribute name is
  `http://schemas.microsoft.com/ws/2008/06/identity/claims/groups` and the
  values are group object IDs. Put that name in **Role/group attribute** and map
  object IDs to roles.
- **SAML Certificates → App Federation Metadata Url:** paste it into **IdP
  metadata URL**.

### Okta (SAML)

- **Applications → Create App Integration → SAML 2.0.**
- Single sign-on URL and Audience URI from the table above; Name ID format
  *EmailAddress* or *Unspecified* with `user.login`.
- **Group Attribute Statements:** name `groups`, filter *Starts with* `ncc-`.
  Put `groups` in **Role/group attribute**.
- **Sign On → Metadata URL:** paste it into **IdP metadata URL**.

### ADFS

- **Add Relying Party Trust →** enter the identifier and endpoint from the
  table above (or, after step 6, import from the SP metadata URL).
- Claim rules: *Send LDAP Attributes as Claims* (UPN → Name ID), and *Send
  Group Membership as a Claim* for each group, outgoing claim type `Role`,
  value e.g. `ncc-admins`. Keep **Role/group attribute** as `Role`.
- IdP metadata URL:
  `https://<adfs-host>/FederationMetadata/2007-06/FederationMetadata.xml`.


For a deployed SAML test provider, build `go run ./tools/fakesaml` with NCC's
SP metadata URL:

```bash
go run ./tools/fakesaml \
  -https \
  -listen 0.0.0.0:9900 \
  -issuer https://ncc.example.com:9900 \
  -sp-metadata-url https://ncc.example.com/saml/metadata \
  -launch-url https://ncc.example.com/saml/login \
  -signing-cert /var/lib/ncc/fakesaml-signing.crt \
  -signing-key /var/lib/ncc/fakesaml-signing.key
```

The provider's landing page has **Launch NCC login** and test users
`alice`/`oscar`/`vera`, all with password `password`. This provider is for
development only; use a trusted certificate or import its self-signed
certificate into the NCC host and browser trust stores. Persist the signing
certificate and key in a systemd deployment; otherwise every provider restart
changes the signing certificate and NCC will reject assertions until its IdP
metadata is reloaded.


## Limit people to some clusters

By default every user sees every cluster their role allows. To split clusters
between teams, use **Settings → Access → Cluster groups**:

1. **Add a group**, name it (for example `Platform`) and pick its clusters.
   You can also add Prism Centrals: every cluster under them joins
   automatically.
2. Add members: local accounts, AD groups, AD users. OIDC groups also count.

Someone in one or more groups only sees those groups' clusters. Someone in no
group still sees everything, and admins always see everything.

## Check it works

- Sign in from a private window as a user from each mapped group and check the
  role in the user menu (top right).
- **System → Audit log** records every sign-in and failure with a reason, for
  example `auth.oidc.login`.
- **Settings → Access → Sign-in → Session** sets how long a sign-in lasts
  (1 minute to 24 hours).

## Troubleshooting

| Problem | What to check |
| --- | --- |
| LDAP test fails with a TLS error | Paste the DC's CA certificate, or check the DC name matches its certificate. |
| LDAP user gets `viewer` instead of `admin` | Run **Test connection…** and look at the groups found; copy the DN exactly into the mapping. Nested groups are not expanded, so map the group the user is directly in. |
| LDAP: "matched 2 entries (tighten user_filter)" | The same short name exists in two domains. Sign in with the UPN, or set **Username attribute** to `userPrincipalName`, or give each domain its own directory (see [Several domains or forests](#several-domains-or-forests)). |
| User of an added directory gets "invalid username or password" but the password is right | Their UPN's suffix isn't in that directory's **UPN suffixes**, or they have no UPN. The audit log shows `upn_suffix_unclaimed` or `upn_missing`; add the suffix. `directory_conflict` means a default-directory user's name ends in a suffix another directory claims. |
| Plain `jdoe` fails, `jdoe@emea.corp.example.com` works | An earlier directory in the order also has a `jdoe`, so it checked the password. Use the UPN or `EMEA\jdoe`, or reorder the directories. |
| `EMEA\jdoe` doesn't work | No directory claims the NetBIOS domain `EMEA`, so the name went to the default directory unchanged. Add `EMEA` to the right directory's **NetBIOS domains**. |
| AD user gets "invalid username or password" but the password is right | A local account has the same name, and directory users can't take it. Rename or delete the local account, or switch to UPN usernames. The server log says "matches a local account". |
| OIDC: "redirect URI mismatch" at the provider | The redirect URI must match exactly, including `https`, host, port and path. Fix **Dashboard URL** or the provider app. |
| OIDC: everyone gets the default role | The token has no groups claim, or the names differ (Entra sends object IDs). Check the provider's groups claim settings. |
| OIDC: "this username belongs to a local account" | A local account has the same name. Rename or delete the local account. |
| SAML: loops back to sign-in, or "no NameID" | Check **Server root URL** is the browser address and on HTTPS, and the provider sends a NameID or set **Username attribute**. |
| SAML: "This name belongs to a local account" | The NameID (or **Username attribute**) equals a local account's name. Rename or delete the local account, or send a different name such as the UPN. |
| SAML: role is always the default | The **Role/group attribute** name must match the attribute name the provider sends (Entra's is the long URL above). |
| Locked out of every admin account | On the server: `ncc-orchestrator v2-reset-password --user admin`. |

More: [`TROUBLESHOOTING.md`](../TROUBLESHOOTING.md).

## Configure with flags instead

If you deploy with automation, LDAP and SAML can also be set with startup
flags (`--ldap-url`, `--ldap-base-dn`, `--saml-root-url`,
`--saml-idp-metadata`, …). Settings set by flags show read-only in the
dashboard. The full list is in
[`FEATURES_AND_CONFIG_FLAGS.md`](../FEATURES_AND_CONFIG_FLAGS.md#211c-rbac-login-and-sso-admin--operator--viewer).
