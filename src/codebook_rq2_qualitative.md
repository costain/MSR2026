# Qualitative Codebook for RQ2

This codebook defines the qualitative categories used to analyze mechanisms underlying
pull request integration outcomes in agent-authored pull requests.

The codebook was informed by prior work on pull request review, coordination breakdowns,
and human–tool interaction, and was finalized after a pilot validation phase.

---

## Coding Rules

- Each pull request (PR) is assigned **exactly one primary code** representing the dominant mechanism explaining the outcome.
- A **secondary code** may be assigned if another factor contributed but was not decisive.
- Codes capture **mechanisms**, not surface-level activity.
- Evidence is drawn from PR descriptions, review comments, timeline events, and CI status.

---

## Primary Codes

### 1. Actionable review loop
**Definition:** Reviewer feedback leads to revisions that converge to an acceptable solution.

**Apply when:**
- Review comments are addressed through follow-up commits
- The PR converges and is merged

**Do not apply when:**
- The PR merges without review
- Review occurs but revisions fail to resolve issues

**Example evidence:**
- Reviewer requests changes; subsequent commits address feedback and PR is merged

---

### 2. Coordination break
**Definition:** Disruption of shared review context impedes collaboration.

**Apply when:**
- Force-push, rebasing, or history rewriting occurs during active review
- Review comments become invalidated

**Do not apply when:**
- Commit history remains stable during review

**Example evidence:**
- Force-push during review invalidates prior comments

---

### 3. Unresponsive / stalled
**Definition:** PR fails due to lack of follow-up after feedback.

**Apply when:**
- Reviewer feedback is given but not acted upon
- Discussion stops without convergence

**Do not apply when:**
- PR actively evolves or is explicitly rejected

**Example evidence:**
- No response after maintainer comments

---

### 4. Scope too large
**Definition:** Review burden prevents effective evaluation.

**Apply when:**
- PR spans many files or combines unrelated changes
- Maintainers cite review complexity

**Do not apply when:**
- Large PR is still reviewed and merged

**Example evidence:**
- Maintainer notes PR is too large to review

---

### 5. Style / convention mismatch
**Definition:** PR violates project-specific coding norms.

**Apply when:**
- Feedback focuses on formatting, naming, or conventions

**Do not apply when:**
- Issues are primarily functional or architectural

**Example evidence:**
- Maintainer requests changes to follow style guidelines

---

### 6. Incorrect / failing CI
**Definition:** PR fails due to technical errors or failing checks.

**Apply when:**
- Tests fail or functionality is incorrect

**Do not apply when:**
- CI passes but PR fails for other reasons

**Example evidence:**
- CI checks failing; tests broken

---

### 7. Redundant / not needed
**Definition:** PR is unnecessary or duplicates existing functionality.

**Apply when:**
- Maintainers state the change is unnecessary or already implemented

**Do not apply when:**
- PR introduces new required functionality

**Example evidence:**
- “This is already handled elsewhere.”

---

### 8. Design/architecture disagreement
**Definition:** Proposed approach conflicts with project design principles.

**Apply when:**
- Maintainers reject the solution approach itself

**Do not apply when:**
- Disagreement concerns minor implementation details

**Example evidence:**
- Maintainer objects to architectural direction

---

### 9. Incomplete solution
**Definition:** PR addresses an issue partially but misses requirements or edge cases.

**Apply when:**
- Reviewers identify missing functionality or correctness gaps

**Do not apply when:**
- PR fully satisfies stated requirements

**Example evidence:**
- “This does not cover all cases.”

---

### 10. Process / policy issue
**Definition:** PR violates contribution or repository policies.

**Apply when:**
- Missing CLA, wrong branch, missing template, etc.

**Do not apply when:**
- Policy issues are resolved and PR proceeds

**Example evidence:**
- CLA not signed; wrong target branch

---

## Validation

The codebook was validated using a pilot analysis of five purposively selected pull requests
spanning merged and non-merged outcomes, with and without human review, including a boundary
case involving a coordination disruption that nevertheless merged. No additional codes were
required, and the codebook was fixed prior to full analysis.

---

## Availability

This codebook is part of the replication package accompanying the paper.
