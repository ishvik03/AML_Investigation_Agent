# AML Policy Spec v1

Policy Spec v1 serves as the deterministic compliance baseline used to validate and justify LLM-generated decisions. Agent outputs are compared against this policy logic to detect violations, measure alignment, and ensure regulatory consistency.

Philosophy: Balanced

Decision Hierarchy:
CLOSE_NO_ACTION < L1_REVIEW < ESCALATE_L2 < SAR_REVIEW_L2

...