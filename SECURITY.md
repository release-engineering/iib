# Reporting a Security Vulnerability or Incident for IIB

Please do not report security vulnerabilities or security incidents via public channels (such as GitHub Issues or Pull Requests).

To ensure coordinated disclosure, please email the IIB security team at _exd-guild-hello-operator-admins\[at\]redhat.com_ with a description of the issue, how to reproduce it and any mitigations you are aware of.

If possible please include a proposed severity rating (for example, see [Red Hat's severity ratings](https://access.redhat.com/security/updates/classification)), and other classifying metadata such as a [CWE](https://cwe.mitre.org/) ID or a [CVSS](https://www.first.org/cvss/) score.

## Response Timeline

Our security team will respond within three working days of your email.

## Submission Guidelines

To help us triage and resolve the issue efficiently, please include the following in your report:

- **Title**: A concise, descriptive summary of the issue.
- **Reporter Details**: Your name/handle and affiliation.
- **Technical Description**: Detailed information regarding the vulnerability.
- **Affected Versions**: The specific version(s) or range(s) of software tested.
- **Reproduction Steps**: A minimal, functional example to reproduce the issue.
- **Impact Assessment**: Potential exploit scenarios and perceived severity. (optional)
- **Suggested Fix**: Any proposed patches or mitigations (optional).
- **Disclosure Status**: Whether this has been shared with other parties or published and your plan for future sharing (e.g., at a conference).

## Contact Information

Direct all security questions and vulnerability reports to:

- **Email**: _exd-guild-hello-operator-admins\[at\]redhat.com_

## Security Policy

Full details of Red Hat’s security disclosure and remediation process can be found here: <https://access.redhat.com/articles/red-hat-coordinated-vulnerability-disclosure>

## Incident Response Policy

### Scope

The "Incident Response Policy" covers security incidents affecting this project, including its source code, infrastructure, release pipeline, maintainer accounts, and contributor trust.

Security takes place in two places - before and after something goes wrong. Other processes and policy should be in place to be preventative and cover things like reviewing merge requests before they are accepted, and vetting new contributions and contributors to a project before they are trusted. This "Incident Response Policy", or IRP, is reactive and covers what happens if something has already gone wrong.

### What constitutes an incident

An incident can be many things. In this context we are talking about hostile events that threaten the security of the product.

- Unauthorized code changes (e.g. a malicious commit or compromised maintainer account)
- Leaked secrets, credentials, or signing keys
- A vulnerability being actively exploited in the wild
- Compromised build or release infrastructure
- A compromised endpoint belonging to someone with repository or release access

### Reporting an incident

- Report incidents to _exd-guild-hello-operator-admins\[at\]redhat.com_.
- Never open a public issue for an active exploit or a leaked secret.
- We aim to acknowledge all reports within 3 business days.

### Response process

1. **Triage** — Confirm the incident, assess its scope and severity.
2. **Contain** — Revoke credentials, roll back affected commits, pull releases, rotate keys as needed.
3. **Eradicate** — Fix the root cause and patch the vulnerability.
4. **Recover** — Restore normal operations. If needed, reissue the release on the [IIB releases page](https://github.com/release-engineering/iib/releases).
5. **Disclose** — Notify affected users and publish an advisory, following coordinated disclosure practices.
6. **Review** — Conduct a post-incident review and document follow-up actions to prevent recurrence.

## EU Cyber Resilience Act — Open Source Steward Statement

This project is stewarded by **Red Hat, Inc.**, an open source software steward as defined in Article 3(14) of the [EU Cyber Resilience Act (Regulation 2024/2847)](https://eur-lex.europa.eu/eli/reg/2024/2847/oj/eng).
Contact: [cra-steward@redhat.com](mailto:cra-steward@redhat.com)

Refer to [Red Hat's security practices and vulnerability management policy](https://access.redhat.com/security/) for detailed information.
