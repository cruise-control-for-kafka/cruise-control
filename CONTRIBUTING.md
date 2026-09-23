Contribution Agreement
======================

As a contributor, you represent that the code you submit is your
original work or that of your employer (in which case you represent you
have the right to bind your employer). By submitting code, you (and, if
applicable, your employer) are licensing the submitted code to
the open source community subject to the Apache 2.0 license. 

File Headers
=============

New files and files that are edited should include the following header:

```
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

Existing files that contain the original LinkedIn/BSD header should retain that header (See [Charter, Section 7.a](CHARTER.md).)
For those files, the Apache header should be added below the existing LinkedIn/BSD header.

Developer Certificate of Origin
================================

The project requires all commits to be signed off, indicating that you certify your contribution with the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).
This is required by the project [Charter](CHARTER.md) (Section 7.b.ii).

To sign off on your commits, use the `-s` flag with `git commit`:

```
git commit -s -m "Your commit message"
```

This adds a `Signed-off-by` line to your commit message with your name and email address.
Make sure the name and email match your Git configuration (`user.name` and `user.email`).

Use of Generative AI Tools
==========================

Code and other content generated in whole or in part using AI tools may be
contributed. Follow the Linux Foundation
[guidance on generative AI tools](https://www.linuxfoundation.org/legal/generative-ai).

Before you contribute AI-generated output:

- Confirm that the tool's terms of use do not restrict the output in a way that
  conflicts with this project's Apache 2.0 license, its intellectual property
  policies, or the [Open Source Definition](https://opensource.org/osd).
- If the output includes pre-existing third-party copyrighted material, confirm
  you have permission to contribute it under a license that complies with this
  project's licensing policies, and include notice, attribution, and the
  applicable license terms.
- Follow your employer's policies when they are stricter.

If an AI tool prepared any part of a commit, add a `Co-Authored-By` (or
`Generated-by` / `Assisted-by`) trailer that names the tool and its version:

```
Co-Authored-By: <AI tool name and version>
```

Example:

```
Ensure tests wait for metadata propagation

Executor tests fail when the broker has not yet propagated topic
metadata after topic creation.

Co-Authored-By: Cursor Grok 4.6
Signed-off-by: Jane Developer <jane@example.com>
```

The `Signed-off-by` line from the Developer Certificate of Origin is still
required. The human author remains responsible for the change.

Do not attribute a commit to an AI tool when a human wrote the change
without one.

Coding agents should also follow [AGENTS.md](./AGENTS.md).

Responsible Disclosure of Security Vulnerabilities
==================================================

Please do not file reports on Github for security issues.
See [SECURITY.md](./SECURITY.md) for how to report a vulnerability.

Tips for Getting Your Pull Request (PR) Accepted
===========================================

1. Make sure all new features are tested and the tests pass -- i.e. a submitted PR should have already been tested for 
existing and new unit tests.
2. Bug fixes must include a test case demonstrating the error that it fixes.
3. Open an issue first and seek advice for your change before submitting a PR. Large features which have never been 
discussed are unlikely to be accepted.
4. Do not create a PR with "work-in-progress" (WIP) changes.
5. Use clear and concise titles for submitted PRs and issues.
6. Each PR should be linked to an existing issue corresponding to the PR 
(see [PR template](./.github/pull_request_template.md)), and PRs can be submitted directly when
repository's PR template is filled out with the details.
7. We strongly encourage the use of recommended code-style for the project 
(see [code-style.xml](./docs/code-style.xml)).
8. A pre-commit CheckStyle hook can be run by adding `./checkstyle/checkstyle-pre-commit` to your `.git/hooks/pre-commit` script.

Development Conventions
=======================

### Minor Changes

Bug fixes, enhancements, tests, documentation, and other routine code changes are submitted as pull requests and require a [standard vote](GOVERNANCE.md#voting-process) to merge.

### Major Changes

Any change that is too complex to be adequately discussed within a single GitHub issue or that may introduce breaking changes requires a design review and must pass a [consensus vote](GOVERNANCE.md#voting-process).
A design proposal should be submitted to the "proposals" repository in Cruise Control for Kafka GitHub organization and, when accepted, committed in Markdown format.

Any of the following are considered a major change:

- Configuration changes
- Tooling changes
- UI changes
- Cruise Control REST API changes
- Java interface changes (including addition and removal of implementations)
- Protocol changes (communication between the metric reporter and Cruise
  Control)

### Governance Changes

Changes to project governance, including updates to [GOVERNANCE.md](GOVERNANCE.md), adding or removing Maintainers, and other major
project decisions, require a [majority vote](GOVERNANCE.md#voting-process).

The list of current Maintainers and TSC members is maintained in [GOVERNANCE.md](GOVERNANCE.md).