# Reuse one point-leaf plan across commit paths

Type: grilling
Status: open
Blocked by: 02, 03

## Question

How long can one routed point-leaf plan and its observations be reused across direct-commit eligibility, fallback to the logged path, lock grouping, validation, and write-back without trusting stale ownership after a split? Decide which evidence the plan owns, which phases may reuse it, and where fresh rerouting is required.
