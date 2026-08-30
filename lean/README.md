# lean/: machine-checked MOR faithfulness theory

A Lean 4 + Mathlib formalization of when merge-on-read (MOR) materialization of a
CDC changelog is *faithful* (materializes exactly the current version of each key),
and of the structural conditions under which it provably cannot be.

The proofs are axiom-clean: every theorem depends only on the three standard
Mathlib axioms and no `sorry`. See `AXIOM_AUDIT.txt` for the committed audit output,
and the top-level `README.md` for the theorem-name → paper-claim map.

## Module map

| File | Contents |
|---|---|
| `MorFaithful/Model.lean` | Core model: `MOR`, visibility (Def 5), `visibleSet`, `Faithful` (Def 6), `LinearExtension` (Def 7). The visibility *rule* `visible i := SD ≤ s i` is `Model.lean:71`. |
| `MorFaithful/Main.lean` | MAIN theorem, both directions. `faithful_of_linear` (⟸ holds); `main_necessity_fails` (⟹ fails for final-state Faithful, machine-checked counterexample). |
| `MorFaithful/MainPrefix.lean` | The corrected MAIN: `prefixFaithful_iff_linear` (per-prefix faithfulness ↔ linear extension). |
| `MorFaithful/Corollaries.lean` | `cor1_single_writer`, `cor2_not_faithful` / `cor2_card` (FLINK-38450 duplication), `cor3_compaction`. |
| `MorFaithful/Global.lean` | Global vs local coherence; `prefixFaithful_iff_globalCoherent`, `local_coherence_insufficient`. |
| `MorFaithful/LocalImpossible.lean` | Claim B: no purely-local ordering scheme can guarantee faithfulness (`local_scheme_admits_unfaithful_config` / `_prefix`). |
| `MorFaithful/UpdatesModel.lean` | A-del-all reduction (all-versions ≡ updates-only) and where injectivity is required (`del_reduction_needs_inj`). |
| `MorFaithful/Zset.lean` | Z-set (`distinct`) helpers. |
| `MorFaithful/AxiomCheck.lean` | `#print axioms` over the 15 headline theorems (produces `AXIOM_AUDIT.txt`). |
| `REPORT.md`, `PURELY_LOCAL_IMPOSSIBILITY_REPORT_(CLAIM_B).md` | Prose write-ups of the model and the Claim B impossibility argument. |

## Build

Toolchain and Mathlib revision are pinned:

- `lean-toolchain`: `leanprover/lean4:v4.31.0`
- `lake-manifest.json`: Mathlib `v4.31.0` (rev `fabf563a7c95a166b8d7b6efca11c8b4dc9d911f`)

```bash
cd lean
lake exe cache get      # fetch prebuilt Mathlib oleans (first build only)
lake build              # builds the MorFaithful library
lake env lean MorFaithful/AxiomCheck.lean   # reproduces AXIOM_AUDIT.txt
```

`.lake/` (build outputs and the ~7 GB Mathlib cache) is intentionally not included;
`lake exe cache get` restores it.
