# Define domain leaf-group reuse across commit paths

Type: grilling
Status: open
Blocked by: 02, 03

## Question

Which domain-owned leaf groups and observations can be reused across direct-commit eligibility, fallback to the logged path, lock grouping, validation, and write-back without a shared point-leaf plan or trust in stale ownership after a split? Decide which interface owns each value, which later phase may reuse it, and where fresh routing is required.
