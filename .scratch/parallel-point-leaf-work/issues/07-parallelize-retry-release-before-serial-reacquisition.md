# Parallelize retry release before serial reacquisition

Type: grilling
Status: open
Blocked by: 01, 03, 04, 06

## Question

How should retry cleanup release held leaves with bounded parallel work, wait until every started release has retired or reported an error, update held-lock bookkeeping, and preserve deterministic simulation before any normal retry or sorted serial reacquisition begins?
