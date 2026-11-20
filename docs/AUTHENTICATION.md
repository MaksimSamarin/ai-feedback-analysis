# Sign-in

## Local accounts

Password sign-in and self-registration are enabled by default when SSO is off. A new local account joins the system Guest group.

Set `BOOTSTRAP_ADMIN_USERNAME` and `BOOTSTRAP_ADMIN_PASSWORD` before the first start to create a local administrator. Once the account is created, the password can be removed from the deployment environment. Use `LOCAL_REGISTRATION_ENABLED=0` to close self-registration. Set `LOCAL_AUTH_ENABLED=0` to turn off password sign-in.

## OpenID Connect

Register Analyzer as a confidential OpenID Connect client with your identity provider. A provider backed by Active Directory can bring existing directory accounts into Analyzer. Enable Authorization Code Flow with PKCE S256 and register the exact external callback URI with the provider.

```dotenv
SSO_ENABLED=1
SSO_ISSUER_URL=https://login.example.org/realms/analyzer
SSO_CLIENT_ID=analyzer
SSO_CLIENT_SECRET=
SSO_REDIRECT_URL=https://reviews.example.org/api/auth/sso/callback
SESSION_COOKIE_SECURE=1
```

Fill in the client secret in the deployment environment. Analyzer reads the issuer's discovery document to pick a token authentication method. You can set `SSO_TOKEN_AUTH_METHOD` explicitly to `client_secret_basic` or `client_secret_post`.

Use HTTPS for the issuer and callback. For local loopback testing, `SSO_ALLOW_HTTP=1` enables HTTP. If your provider uses a private certificate authority, mount the CA as a PEM file in the backend and set `SSO_CA_FILE` to its container path.

Analyzer identifies an SSO account by issuer and subject. It derives a display login from the signed identity claims. If a local account already uses that login, the SSO account gets a distinct login. Changes to the display login preserve report ownership.

Set `SSO_ADMIN_SUBJECTS` to bootstrap an SSO administrator by subject. The setting accepts a comma-separated list. `SSO_ADMIN_USERNAMES` supports matching a signed login for deployments that use that convention. Once an entry is removed from those lists, an administrator can manage its role in the UI.

With SSO enabled, local self-registration is closed by default. To allow both flows, set `LOCAL_AUTH_ENABLED=1` and `LOCAL_REGISTRATION_ENABLED=1`. New SSO users request access to a group from an administrator or group manager.
