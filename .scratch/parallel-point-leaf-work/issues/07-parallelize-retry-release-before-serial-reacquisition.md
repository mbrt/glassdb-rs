# Parallelize retry release before serial reacquisition

Type: grilling
Status: open
Blocked by: 01, 03, 04, 06

## Question

How should `KeyLocker` release every held leaf through `join_all_bounded`, interpret all stable ordered results, update held-lock bookkeeping safely on errors or cancellation, and preserve deterministic simulation before any normal retry or sorted serial reacquisition begins?
