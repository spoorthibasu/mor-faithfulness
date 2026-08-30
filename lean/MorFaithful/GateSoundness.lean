import MorFaithful.UpdatesModel

/-!
# Sequence separation: every discarded version sits at a strictly lower seq than every survivor

This file mechanises the one fact the metadata gate's inversion test actually needs, so that the
gate's comparison can be justified from the model rather than from an informal argument.

## Why it is asked

The implemented gate reads per-data-file ordering bounds and data sequence numbers, sorts the files
of a rewrite group by sequence number, tracks a running maximum ordering upper bound, and reports a
possible stale-win when some later file's ordering LOWER bound falls below it.  Because it sorts
files rather than sequence numbers, it also compares files that share a sequence number against one
another.  A hash-partitioned CDC sink writes each commit as several files whose ordering intervals
all span the whole commit window, so those same-sequence comparisons fire on perfectly ordered data
and the gate clears nothing (measured: `probe_gate_filelayout.py`).

The question is whether those same-sequence comparisons were ever load-bearing.  If a stale-win
always involves two DISTINCT sequence numbers, they were not, and the gate may group files by
sequence number and compare only across distinct sequences without losing soundness.

## What is proved

`discarded_seq_lt_visible_seq` (all-versions delete model) and `discarded_seq_lt_visible_seq'`
(updates-only model): if `i` is visible and `j` is not, then `M.s j < M.s i`.

Both are immediate from the mechanised suppression rule, and neither needs `Injective d`, a linear
extension, or any other hypothesis — they are facts about `visibleSet` alone.  The corollaries
`staleWin_distinct_seq` / `staleWin_distinct_seq'` restate this in the form the gate uses: a
survivor and a discarded version never share a sequence number.

## What this does NOT say

It does not say that a discarded version's ordering value exceeds its survivor's — that is what
makes a violation a violation, and it is a property of the data, not a theorem.  The claim here is
only about seq separation, which is what licenses the gate to ignore same-sequence pairs.
-/

namespace Mor
namespace MOR

variable {V : Type*} (M : MOR V)

/-! ### All-versions delete model (`SD = ⨆ᵢ sᵢ`) -/

/-- **Sequence separation.**  A version that survives all equality-deletes has a strictly greater
seq than any version that does not.

Proof is direct from `Model.lean`: `visibleSet` is `filter (SD ≤ s ·)`, so `j ∉ visibleSet` gives
`s j < SD`, and `i ∈ visibleSet` gives `SD ≤ s i`. -/
theorem discarded_seq_lt_visible_seq {i j : Fin (M.n + 1)}
    (hi : i ∈ M.visibleSet) (hj : j ∉ M.visibleSet) : M.s j < M.s i := by
  rw [visibleSet, Finset.mem_filter] at hi hj
  push_neg at hj
  exact lt_of_lt_of_le (hj (Finset.mem_univ j)) hi.2

/-- The form the gate uses: a survivor and a discarded version never share a data sequence number,
so an "inversion" between two files at the SAME sequence number cannot witness a stale-win. -/
theorem staleWin_distinct_seq {i j : Fin (M.n + 1)}
    (hi : i ∈ M.visibleSet) (hj : j ∉ M.visibleSet) : M.s j ≠ M.s i :=
  ne_of_lt (M.discarded_seq_lt_visible_seq hi hj)

/-- Two versions sharing the maximum seq are BOTH visible: the FLINK-38450 shape. Same-sequence
co-residency is therefore not an ordering relation at all — neither version suppresses the other. -/
theorem same_seq_both_visible {i j : Fin (M.n + 1)}
    (hi : i ∈ M.visibleSet) (h : M.s j = M.s i) : j ∈ M.visibleSet := by
  rw [visibleSet, Finset.mem_filter] at hi ⊢
  exact ⟨Finset.mem_univ j, h ▸ hi.2⟩

/-! ### Updates-only delete model (`SD' = ⨆_{i>0} sᵢ`)

The two models are equivalent for faithfulness (`faithful_iff_faithful'`), but the gate argument
should not depend on which of them one adopts, so it is proved separately here.

`visibleSet'` is defined in `UpdatesModel.lean` under a `[DecidableEq V]` section variable, so the
statements below carry that instance.  It is a decidability side-condition on the value type, not a
new assumption about the workload. -/

section UpdatesOnly
variable [DecidableEq V]

/-- Sequence separation in the updates-only model. -/
theorem discarded_seq_lt_visible_seq' {i j : Fin (M.n + 1)}
    (hi : i ∈ M.visibleSet') (hj : j ∉ M.visibleSet') : M.s j < M.s i := by
  rw [mem_visibleSet'] at hi
  rw [mem_visibleSet'] at hj
  push_neg at hj
  exact lt_of_lt_of_le hj hi

/-- The gate-facing form, updates-only model. -/
theorem staleWin_distinct_seq' {i j : Fin (M.n + 1)}
    (hi : i ∈ M.visibleSet') (hj : j ∉ M.visibleSet') : M.s j ≠ M.s i :=
  ne_of_lt (M.discarded_seq_lt_visible_seq' hi hj)

/-! ### The converse direction, stated so the limit of the result is explicit

Two versions sharing the maximum seq are BOTH visible.  This is the FLINK-38450 shape, and it is
why "same sequence number" is a genuine co-residency rather than an ordering relation: neither
suppresses the other. -/

theorem same_seq_both_visible' {i j : Fin (M.n + 1)}
    (hi : i ∈ M.visibleSet') (h : M.s j = M.s i) : j ∈ M.visibleSet' := by
  rw [mem_visibleSet'] at hi ⊢
  exact h ▸ hi

end UpdatesOnly

end MOR
end Mor
