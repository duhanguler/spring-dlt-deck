# Security Policy

## Supported Versions

| Version | Supported |
| :--- | :--- |
| `0.1.x` | :white_check_mark: |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Instead, report vulnerabilities privately via GitHub Security Advisories (`Security -> Advisories -> Report a vulnerability`) on [`duhanguler/spring-dlt-deck`](https://github.com/duhanguler/spring-dlt-deck/security/advisories/new).

We aim to acknowledge reports within **48 hours** and publish coordinated fixes with CVE/advisory details promptly.

## Security Controls Built Into `spring-dlt-deck`

- **JEP 290/415 `ObjectInputFilter`** strict allowlist on `springDeserializerException*` headers
- **Fail-Closed RBAC (`VIEWER` / `OPERATOR` / `APPROVER`)** with `ScopedValue<ActorContext>`
- **Four-Eyes Approval & Frozen `planSpecHash`** (TOCTOU defense)
- **Strict Per-Request 128-bit Nonce CSP** (zero `'unsafe-inline'`, zero external CDN), **CSRF (`X-XSRF-TOKEN`)**, **`Cache-Control: no-store`**
- **HMAC-SHA256 Canonical Keyset Cursors** & **OWASP CSV Formula Injection Neutralization**
- **Checksum-Verified PII Redaction** (TCKN, IBAN, Luhn PAN, JWT, emails, phones) with audited `APPROVER`-only unmasking
- **Compile-Time & ArchUnit-Enforced Fail-Closed MCP** (zero approval or raw PII unmasking paths)
