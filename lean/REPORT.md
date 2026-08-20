# MOR changelog-materialization faithfulness — machine-check report

**Location:** the `lean/` package of this repository.
**Toolchain:** Lean 4 `v4.31.0`, mathlib pinned to tag `v4.31.0` (exact-match, cached).
**Build:** `lake build` succeeds (8565 jobs). ~700 lines of theory across 6 files.
**Axioms:** every headline theorem depends only on `propext, Classical.choice, Quot.sound`
— the standard mathlib axioms — and **not `sorryAx`** (verified in `MorFaithful/AxiomCheck.lean`).

## DONE criteria

- [x] lakefile builds, mathlib resolves
- [x] defs 1–7 compile (`Zset.lean`, `Model.lean`)
- [~] MAIN theorem, both directions, no `sorry` — **see the headline finding below**
- [x] COR1, COR2, COR3 compile, no `sorry` (`Corollaries.lean`)
- [x] global-coherence theorem compiles, no `sorry` (`Global.lean`)
- [x] list of every added assumption (below)

## Headline finding: MAIN `⟹` is FALSE for def-6 (final-state) Faithful

The informal MAIN `Faithful(k) ↔ LinearExtension(seq, k)` does **not** hold in the `⟹`
(necessity) direction with the final-state `Faithful` of def 6.

* `⟸` holds: `MOR.faithful_of_linear` (needs *no* injectivity).
* `⟹` is refuted by a **compiled counterexample**, `main_necessity_fails`:
  versions `d = [0,1,2]`, seqs `s = [5,1,10]`.  Only `d 2` (current) has seq `= max = 10`,
  so `distinct(Zphys) = {d 2 ↦ 1}` — Faithful — yet `s 0 = 5 > 1 = s 1`, so
  `¬LinearExtension`.

**Why:** since `s i ≤ sup s` always, `visible i ↔ s i = SD`.  So final-state `Faithful`
says exactly "the current version is the unique seq-maximum" (`faithful_iff_visibleSet`).
It places **no** constraint on the order of the *stale* versions.  This is not a
count-vs-set issue — a count-based definition fails here too; it is a
**final-state-vs-per-prefix** issue.

**Resolution (added, not silently substituted).** I kept def 6 as written and *added*
`PrefixFaithful` (faithful after every update, i.e. every logical prefix `0..m`
reconstructs to `{d m}`).  Then, in full and with no `sorry`:

```
MOR.prefixFaithful_iff_linear : Function.Injective M.d → (M.PrefixFaithful ↔ M.LinearExtension)
```

This keeps your `LinearExtension` (def 7) unchanged and makes MAIN a genuine `↔`.
The `⟹` proof (`linear_of_prefixFaithful`) genuinely uses **set-equality**: from the
prefix ending at `j` it reads `i ∉ visibleSet ⇒ s i < SD` for every `i < j`, i.e. it
reasons about the *support* of `distinct(Zphys)`, not a multiplicity count.

> **Decision for you.** Your paper's MAIN should be stated with `PrefixFaithful`
> ("faithful after every update"), or you keep def 6 and state MAIN as `⟸` plus the
> weaker exact necessary condition "current is the unique seq-maximum". I implemented the
> first (recommended) additively; both are in the sources.

## Corollaries

* **COR1** `cor1_single_writer : StrictMono M.s → M.Faithful` (final-state), plus
  `cor1_single_writer_prefix` for the per-prefix version. (`LinearExtension` is *defeq*
  `StrictMono M.s`, see `linearExtension_iff_strictMono`.)
* **COR2 (FLINK-38450)** `Mviol a b := ⟨1, ![a,b], ![7,7]⟩` (stale `a`, current `b`, and
  the delete all at seq 7):
  * `cor2_not_faithful : a ≠ b → ¬ (Mviol a b).Faithful`
  * `cor2_card : a ≠ b → (distinct (Mviol a b).Zphys).support.card = 2`
* **COR3 (compaction)** `cor3_compaction`: if a rewrite preserves `distinct(Zphys)` (the
  deduplicated visible content) and `cur` (the current version), then `Faithful` is
  preserved in both directions.

## Global coherence (hardest part)

Producers `p : Fin (n+1) → ℕ`.  `LocalCoherent` = each producer's own versions increase
in seq; `GlobalCoherent` = a single global linear extension (`= LinearExtension`).

* **Necessity** `faithful_requires_global_coherence : PrefixFaithful → GlobalCoherent`.
* **Sufficiency** `global_coherence_suffices : GlobalCoherent → PrefixFaithful`.
* **Local is insufficient (the false step, compiled)** `local_coherence_insufficient`:
  `Mglob := ⟨2, ![0,1,2], ![1,10,2]⟩` with producers `![0,1,0]` is `LocalCoherent`
  (producer A: `d0,d2` at seqs `1<2`) but **not** `GlobalCoherent` and **not** `Faithful`
  — producer B's seq 10 exceeds A's later version's seq 2, so `d 1` suppresses the current
  `d 2`.  Hence faithfulness across subtasks cannot be guaranteed by per-subtask
  monotonicity; a single cross-subtask linear extension is required.

## Every assumption added beyond the informal statement

1. **(A-inj) distinct version values** — `Function.Injective M.d`.  Needed for the
   reduction lemma (`faithful_iff_visibleSet`), hence for MAIN `⟹`, COR2's `card = 2`, and
   global necessity.  **Not** needed for MAIN `⟸`.  Reality caveat: if a key revisits a
   value (`d_i = d_j`), set-equality Faithfulness can hold with a duplicate present, so
   this is a genuine assumption, not a tautology.

2. **(A-prefix) per-prefix Faithfulness for MAIN `⟹`** — the strengthening above.  The
   single most important added hypothesis; final-state Faithfulness is provably too weak
   for the `⟹` direction.

3. **(A-del-all) every version emits a delete at its own seq** — so `S_D = sup_i s i` over
   *all* versions (including the initial insert `d 0`), rather than only updates `1..n`.
   I argue (and it holds in all theorems) that restricting to `1..n` changes nothing,
   because `s 0` is never the decisive maximum in any Faithful case and `LinearExtension`
   forbids `s 0` being maximal; but it is a modeling simplification.

4. **(interpretation) COR3's hypothesis** — "compaction preserving visible(k)" is
   formalized as "preserves `distinct(Zphys)` and `cur`".  That is the precise invariant
   the informal phrase names (deduplicated visible content + current version).

5. **(scope/mechanization)** single key `k` throughout; `Zset` via mathlib `Finsupp`
   (`noncomputable`, so `Classical.choice` appears in the axiom list — standard for
   mathlib); producer ids as `ℕ`.  `Faithful` is a **set-equality** as required; it was
   **not** weakened to a count anywhere.

## What was NOT weakened

`Faithful` (def 6) remains `distinct(Zphys) = single (d n) 1`, a set-equality.  The `⟹`
gap was surfaced with a compiled counterexample and fixed by *adding* a hypothesis
(`PrefixFaithful`), never by relaxing the goal to a count.
