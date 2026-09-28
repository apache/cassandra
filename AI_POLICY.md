# LLM Usage Policy

This is a moderation policy for how LLMs are used in `rust-lang/rust` and other related repositories.
For additional information about the policy itself, including the list of repositories that have adopted this policy,
see [the appendix](#appendix).
For the rationale on why this policy exists, see [the blog post](https://blog.rust-lang.org/inside-rust/2026/08/05/rust-langrust-is-adopting-an-llm-policy/)
where this policy was announced.

## Overview

Using LLMs while working on `rust-lang/rust`, and other repositories that have adopted this policy,
is conditionally allowed, when done with care.
LLMs are not a substitute for thought,
and we do not allow them to be used in ways that risk losing our shared social and technical understanding of the project,
nor in ways that hurt our goals of creating a strong community.

We are aware that many clauses in this policy are unenforceable.
Our goal is *not* to catch every violation (see [It's not your job to play detective](#its-not-your-job-to-play-detective), below).
Instead, our goal is to remove plausible deniability: to force a choice between following the policy and intentionally violating it.
See [Money laundering and AML compliance](https://www.bitsaboutmoney.com/archive/money-laundering-and-aml-compliance/) for more background about the motivation.

The policy's guidelines are roughly as follows:

> It's fine to use LLMs to answer questions, analyze, distill, refine, check, suggest, review. But not to **create**.

> LLMs work best when used as a tool to write *better*, not *faster*.

> We carve out a space for "experimentation" to inform future revisions to this policy.

### Moderation vs guidance

This document only contains our moderation policy.
For technical guidance and suggestions, see the [rustc-dev-guide][llm-guidance].

[llm-guidance]: https://rustc-dev-guide.rust-lang.org/llm-guidance.html

## Rules
### Legend
- ✅ Allowed.
- ❌ Banned.
- ⚠️ Allowed with caveats. Must disclose that an LLM was used.
- ℹ️ Adds additional detail to the policy. These bullets are normative.
- 💡 Indicates that there are suggestions for this bullet in the dev-guide.
- 🔨 Violating this clause counts as a violation of the Code of Conduct.

### Summary
- ✅ Allowed: Private use.
- ❌ Banned: LLM-created comments, docs, or diagnostics. Replacing human judgement with LLM judgement. Requiring people to use an LLM to contribute.
- ⚠️ Conditionally allowed: Trivial changes, machine translation, LLM reviews and review bots, LLM-created code *under the experiment rules*.
- 🔨 Carries a moderation penalty: Lying.

### Non-exhaustive policy

This policy does not aim to be exhaustive.
If you have a use of LLMs in mind that isn't on this list, talk to people about it, and judge it in the spirit of this overview:
- Using an LLM for your own personal use is likely allowed ✅
- Showing LLM output to another human without solicitation is likely banned ❌
- Making a decision that affects others based on LLM output requires disclosure ⚠️

### ✅ Allowed
The following are allowed.
- Any use of an LLM where you are the only one who sees the output. For example:
    - Asking an LLM questions about an existing codebase.
    - Asking an LLM to summarize comments on an issue or PR.
        - ℹ️ This does not allow reposting the summary publicly. This only includes your own personal use.
    - Asking an LLM to privately review your code or prose.
        - ℹ️ This does not apply to public comments by the LLM. See "review bots" under ⚠️ below.
    - Writing dev-tools for your own personal use using an LLM.
    - Using an LLM to generate possible solutions to an issue, learning from them, and then writing something from scratch in your own style.
- Using an LLM in the creation of clearly experimental code changes that are not meant to be reviewed but must live as PRs on `rust-lang/rust` for tooling reasons, such as to run crater or perf.
    - "Clearly experimental" includes markers such as `S-experimental` labels, `[PERF]` titles, or `r? ghost` comments.
    - We strongly recommend, but do not require, that experimental PRs disclose LLM usage.
      The goal here is to avoid other people picking up the draft work without knowing it's LLM-generated.
    - ℹ️ If a PR is no longer marked as clearly experimental, at that point disclosure is required.

### ❌ Banned
The following are banned.
- Comments that are originally created by an LLM posted from a personal user account.
    - ℹ️ This also applies to issue bodies and PR descriptions.
    - ℹ️ This also applies to voice/video content originally created or scripted by an LLM.
    - ℹ️ This does not apply if the LLM content is clearly quoted and marked; you can post that.
      However, the content of the comment must stand on its own even without the LLM content; it's not a substitute for your own words.
    - ℹ️ See also "machine-translation" in ⚠️ below.
    - ℹ️ See also "Scope" in the appendix below.
- Documentation that is originally created by an LLM.
    - ℹ️ This includes non-trivial source comments, such as doc-comments, safety comments, or multiple paragraphs of non-doc-comments.
    - ℹ️ This includes compiler diagnostics.
      LLMs are conditionally allowed to assist with the *logic* surrounding a diagnostic (see "Experiment: LLM-created code changes" below),
      but they must not be used to create the message itself.
    - ℹ️ This does not include "trivial" changes (see ⚠️ below).
- Policies or processes that are written such that an LLM is required to execute them.
    - For example, you must not *only* document where tests live with an `AGENTS.md`.
      Documentation must be authored for humans primarily, and LLM documentation may only summarize it, not add new detail.
- Treating an LLM review as a sufficient condition to merge or reject a change.
  LLM reviews, if enabled, **must** be advisory-only.
  Teams can have a policy that code can be merged without review, and they can have a policy that code must be reviewed by at least one person,
  but they may not have a policy that an LLM review substitutes for a human review.
    - ℹ️ See "review bots" in ⚠️ below.
    - ℹ️ An LLM review does not substitute for self-review. Authors are expected to review their own code before posting and after each change.

### ⚠️ Allowed with caveats
These uses are allowed on a case-by-case basis, under the rules below.
If you are a new contributor, you should expect to be scrutinized more heavily than existing contributors,
since you haven't yet established trust with your reviewers.

All uses under "⚠️ Allowed with caveats" **must** disclose that an LLM was used.

- Using machine-translation (e.g. Google Translate) from your native language without posting your original message is allowed but discouraged.
  Doing so can introduce new miscommunications that weren't there originally, and prevents someone who speaks the language from providing a better translation.
    - ℹ️ Posting both your original message and the translated version is always ok, but you must still disclose that machine-translation was used.
- "Trivial" code or prose changes.
    - ℹ️ Changes are trivial if there is no other way to write them, or the other ways to write them are nearly identical. For example, the following are all trivial:
        - Typo fixes
        - Markdown links
        - Changing a word to a synonym
        - Type signatures for a trait implementation
    - ℹ️ Be cautious about PRs that consist solely of trivial changes.
      See also [the compiler team's typo fix policy](https://rustc-dev-guide.rust-lang.org/building/compiler-documenting.html#contributing-documentation:~:text=Please%20notice%20that%20we%20don%E2%80%99t%20accept%20typography%2Fspellcheck%20fixes%20to%20internal%20documentation).
    - 💡 See the [dev-guide][llm-guidance] for additional suggestions.
    - For more background about concepts that inspired this policy, see
      [threshold of originality](https://fsfe.org/news/2025/news-20250515-01.en.html)
      and [the Google v Oracle ruling](https://en.wikipedia.org/wiki/Google_LLC_v._Oracle_America,_Inc.) that copying API declarations is fair use.
- Using an LLM to discover bugs, as long as you personally verify the bug.
  Please refer to [our guidelines for fuzzers](https://rustc-dev-guide.rust-lang.org/fuzzing.html#guidelines).
    - ℹ️ This also includes reviewers who use LLMs to discover flaws in unmerged code.
    - ℹ️ See also "Comments \[...\] posted from a personal user account" under ❌ above.
- Using an LLM as a "review bot" for PRs.
    - ℹ️ Review bots that post without being approved by a maintainer will be banned.
      This is a one-time approval; maintainers don't need to approve individual reviews.
    - ℹ️ Review bots **must** have a separate GitHub account that clearly marks them as an LLM.
      You **must not** post (or allow a tool to post) LLM reviews verbatim on your personal account unless clearly quoted with your own personal interpretation of the bot's analysis.
    - ℹ️ Review bot accounts must be blockable by individual users via the standard GitHub user-blocking mechanism. (Note that some GitHub "app" accounts post comments that look like users but cannot be blocked.)
    - ℹ️ LLM comments **must not** be blocking; reviewers must indicate which comments they want addressed.
        - In other words, reviewers must explicitly endorse an LLM comment before blocking a PR. They are responsible for their own analysis of the LLM's comment and cannot treat it as a CI failure.
    - ℹ️ This does not apply to private use of an LLM for reviews; see ✅ above.
    - 💡 See the [dev-guide][llm-guidance] for additional suggestions.

## Experiment: LLM-created code changes intended for review

We leave space open to experiment with LLMs to inform future policies.
This experiment is meant to inform future non-experimental policy, not to serve as the perpetual LLM usage policy.

### Rules

Pre-arranged, non-critical, high-quality, well-tested, and well-reviewed code changes that are originally created by an LLM are allowed, **with disclosure**.

1. "Pre-arranged" means that a reviewer has communicated *ahead of time* that they are willing to review an LLM-created PR.
    - ℹ️ New contributors cannot create a PR using an LLM unless they first talk with a reviewer.
      This must be the *same* reviewer who will be assigned to the PR.
    - Authors are, of course, allowed to start work on a change before finding a reviewer.
      However, they must find a reviewer before opening a PR.
      It is allowed, but discouraged, to open a LLM-created PR with `r? @ghost` before finding a reviewer;
      we suggest instead pushing to your fork and posting a link on Zulip for your prospective reviewer.
      Such PRs still require disclosure.
2. "Non-critical" means that it is extremely unlikely for the PR to cause a [soundness](https://jacko.io/safety_and_soundness.html) regression.
    - ℹ️ Examples:
        - Changes to internal tooling like `tidy`, `x setup`, and `linkchecker` are probably ok.
        - Changes that have a strong soundness impact, like the trait system, MIR building, or the query system are probably not ok.
3. "High-quality" means that it is held to at least the same standard as other code changes.
   Everyone reads code, not just the author and reviewer;
   we are not interested in "vibe-coded" PRs that degrade the quality of the codebase.
4. "Well-tested" means that you have covered all edge-cases that either you or the reviewer can think of.
    - ℹ️ LLM-created PRs will be held to a higher standard than human-created PRs, because LLMs make it easier to write tests.
    - ℹ️ If there is no existing test suite for a section of code, you must either write a new test suite or close the PR.
      There are no exceptions for "writing the tests seems hard".
5. "Well-reviewed" means the author and reviewer both commit to fully understanding the code.
    - ℹ️ All review requirements in [our existing review policy](../compiler/reviews.md#basic-reviewing-requirements) still apply.
    - ℹ️ A review from a project member does not substitute for self-review.
      Authors are expected to review their own code before posting and after each change.
    - 💡 See the [dev-guide][llm-guidance] for additional suggestions.

As an exception, members of the `rust-lang` organization are not bound by the "non-critical" clause;
we trust authors and reviewers to make their own judgements.
However, we *strongly discourage* making use of this provision.
LLMs are very very good at generating plausible-looking code, and soundness is hard to test.

As an exception, PRs written before this policy went into effect are exempt from the "non-critical" clause.

### Procedures

LLM-created PRs must be tagged with a new `llm-assisted` label.
All such PRs will be posted to a new (private) Zulip channel, which will be accessible to all members of the `rust-lang` organization.
The goal of the channel is *not* to act as an additional gatekeeper on LLM-created PRs.
Instead, it's to collect information about *whether this experiment is working*:
Are people doing interesting and useful things with LLMs? Are they learning? Are they making repeat contributions?

Because the new channel is private, it will have higher-than-normal standards for what counts as on-topic.
For example, the following are on-topic:
- Whether a PR meets the criteria for the experiment exception
- Whether a PR follows the policy in general

And the following are off-topic:
- Technical and design discussions. These should be posted directly on the PR or in a public Zulip channel.
- Discussions about effort, communication style, or intent
- General discussions about the LLM policy

### Circuit breaker

To avoid the risk of LLMs "overwhelming" the codebase, or becoming de-facto required, we set a limit on how many LLM-created PRs can be merged.
If more than half of PRs merged in a 6-week window are LLM-created, we disallow merging new LLM-created PRs until we go back below 50%, with a minimum cooldown of 10 days.
This window is chosen to align with our existing release cycle, and the cooldown is to avoid flip-flopping between allowed and disallowed, with a period chosen to align with our FCP process.

The cooldown is intended to encourage discussion:
- How is the experiment going?
- Are we adopting AI sustainably?
  Are we including contributors who choose not to use LLMs?
- Are there any changes we want to make to our policy?

To avoid inconsistent enforcement, and the resulting resentment,
we strongly suggest that this circuit breaker be automated.

## Appendix
### Scope

This policy only applies to the repositories that have adopted it, and only to the teams that have ratified it: compiler, libs, types, rustdoc, bootstrap, and their subteams.

Currently, the following repositories have adopted this policy:
- [`rust-lang/rust`](https://github.com/rust-lang/rust)
- [`rust-lang/rustlings`](https://github.com/rust-lang/rustlings)
- [`rust-lang/mdBook`](https://github.com/rust-lang/mdBook)
- [`rust-lang/cargo`](https://github.com/rust-lang/cargo)
- [`rust-lang/rust-clippy`](https://github.com/rust-lang/rust-clippy)
- [`rust-lang/rustfmt`](https://github.com/rust-lang/rustfmt)

The following are not in scope and are free to set their own policies:
- Other repositories in `rust-lang`
- Submodules, subtrees, and crates.io dependencies
- Teams that have not ratified the policy, such as lang and edition

For example, the following do not fall under the policy:
- Tracking issues for T-lang
- T-lang proposals
- T-lang stabilization reports
- Language documentation
- The style guide
- Names of compiler lints. This only applies to the names themselves; the diagnostic messages are still covered under this policy.
- Direct quotes from any of the above in documentation or diagnostics.

### Motivation and guiding principles

There is not a consensus within the Rust project—and likely never will be—about when/how/where it is acceptable to use AI-based tools.
Many members of the Rust project and community find value in AI;
many others feel that its negative impact on society and the climate are severe enough that no use is acceptable.
Still others are working out their opinion.

Despite these differences, there are many values we all share:

- Building a community of deep experts in our collective projects.
- Building an inclusive community where all feel welcome and respected.

And many facts we agree on:

- Many people find LLM-generated code and writing deeply unpleasant to read or review.
- Many people find LLMs to be a significant aid to learning and discovery.
- LLMs are a new technology, and we are still learning how to use, moderate, and improve them.

With those facts and values in mind, the policy is designed with the following goals:

- Build an intentionally conservative policy that lets us maintain the standard of quality that Rust is known for.
- Limit LLM contributions to the very highest standard of quality, to show that our guideline of "better, not faster" isn't just words.
- Make the policy enforceable and easy to moderate.
- Make the policy consistent and easy to understand and summarize, even for people who haven't read it in detail.
- Avoid making LLMs a requirement to contribute, to be inclusive of contributors who choose not to use them, and to avoid making Rust "pay-to-play".

### Moderation policy
#### It's not your job to play detective
["The optimal amount of fraud is not zero"](https://www.bitsaboutmoney.com/archive/optimal-amount-of-fraud/).
Don't try to be the police for whether someone has used an LLM.
You are not required to "actively look" for whether an LLM was involved.

Style is not evidence, and English-as-a-second-language speakers, neurodivergent people, and over-explainers are the most likely to be accused of writing like an LLM.

If it's clear someone's broken the rules, point them to this policy;
otherwise, strongly prefer [reporting to mods](https://rust-lang.org/policies/code-of-conduct/) over public accusations.
Reporting to moderation is not intended to be a penalty;
the mod team is interested in seeing non-violations as well as violations.
As always, the mod team is free to exercise their own judgement and discretion.

#### Be honest
Conversely, you're expected to be honest about your use of LLMs and the extent of that use.
If you are not sure where something you would like to do falls in this policy, please talk to the [moderation team](mailto:rust-mods@rust-lang.org).
Don't try to hide it.

Deliberately misrepresenting your use of LLMs is not welcome and [may result in moderation action](https://rust-lang.org/policies/code-of-conduct/#moderation).

#### Penalties
The policies marked with a 🔨 follow the same guidelines as the code of conduct:
Violations will first result in a warning, and repeated violations may result in a ban.
- 🔨 Violations of the "Be honest" section

Other violations are left up to the discretion of reviewers and moderators.
For minor violations we recommend telling the author that we can't review the PR until it complies with the policy, with pointers to exactly what they need to do.
For major violations or extractive PRs, we recommend closing the PR or issue.

It is **not** ok to harass a contributor for using an LLM.
All contributors must be treated with respect.
The Code of Conduct applies to *all* conversations in the Rust project.

### Responsibility

Your contributions are your responsibility; you cannot place any blame on an LLM.
- ℹ️ This includes when asking people to address review comments originally created by an LLM. See "review bots" under ⚠️ above.

### The meaning of "originally created by an LLM"

This document uses the phrase "originally created by an LLM" to mean "text that was generated by an LLM (and then possibly edited by a human)".
No amount of editing can change how it was originally created;
the origin sets the initial style, and that style is very hard to change once it's set.

For more background about analogous reasoning, see ["What Colour are your bits?"](https://ansuz.sooke.bc.ca/entry/23).

This policy makes no distinction between LLM output that comes from a chat interface and output that comes from editor auto-completion.
In most cases the output is "trivial" (see above under ⚠️), but regardless, it is not treated specially by this policy.

### Conditions for modification or dissolution
This policy is not set in stone, and we can evolve it as we gain more experience working with LLMs.

Minor changes, such as typo fixes, only require a normal PR approval.
Major changes, such as adding a new rule or canceling an existing rule, require a successful [MCP (Major Change Proposal)](../compiler/proposals-and-stabilization.html#proposals) (2 approvals and no concerns) from each team that ratified the policy.
Changes to the guidance in the rustc-dev-guide have no special requirements for modification.

This policy can be dissolved in a few ways:

- An accepted FCP by the teams that ratified the policy.
- An objective concern raised about active harm the policy is having on the Rust project, with evidence, as decided by a leadership council FCP.
- If the proposed [LLM committee](https://github.com/rust-lang/leadership-council/issues/308) (or any similar dedicated project-wide body) is formed, any policy it sets will take precedence over this policy.

If evolving this policy with the above mechanism proves unworkable, especially if a team ends up significantly hindered in autonomously making decisions concerning its own purview, then these conditions for modification can be adjusted by a leadership council FCP.