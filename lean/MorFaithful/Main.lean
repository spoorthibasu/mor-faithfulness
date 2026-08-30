import MorFaithful.Model

/-!
# MAIN theorem — and a machine-checked finding about its `⟹` direction

The informal MAIN is `Faithful(k) ↔ LinearExtension(seq, k)`.

* **`⟸` (sufficiency / the cut argument):** `LinearExtension → Faithful`.
  This holds, and needs **no** injectivity hypothesis (see `faithful_of_linear`).

* **`⟹` (necessity):** `Faithful → LinearExtension` **DOES NOT HOLD** for the
  final-state `Faithful` of def 6.  `Faithful` (final state) only forces the current
  version to be the unique seq-maximum; it says nothing about the relative order of
  the *stale* versions.  `main_necessity_fails` machine-checks a witness:
  versions `d = [0,1,2]` with seqs `s = [5,1,10]` is Faithful (only `d 2` is visible)
  yet violates strict monotonicity (`s 0 = 5 > 1 = s 1`).

  This is exactly the place the spec warned about.  The missing hypothesis is
  **per-prefix faithfulness** — see `MorFaithful/MainPrefix.lean`, where
  `PrefixFaithful ↔ LinearExtension` is proved in full.
-/

namespace Mor

open Finset

variable {V : Type*}

/-! ### `⟸` : LinearExtension ⟹ Faithful (holds; no injectivity needed) -/

/-- Under a strict linear extension, the last version is the **unique** visible one. -/
theorem MOR.visibleSet_of_linear (M : MOR V) (h : M.LinearExtension) :
    M.visibleSet = {Fin.last M.n} := by
  have hle : ∀ i, M.s i ≤ M.s (Fin.last M.n) := by
    intro i
    rcases (Fin.le_last i).lt_or_eq with hlt | heq
    · exact (h i (Fin.last M.n) hlt).le
    · rw [heq]
  have hSD : M.SD = M.s (Fin.last M.n) :=
    le_antisymm (Finset.sup_le (fun i _ => hle i)) (M.le_SD _)
  ext i
  rw [M.mem_visibleSet, mem_singleton, hSD]
  constructor
  · intro hsi
    by_contra hne
    exact absurd hsi (ne_of_lt (h i (Fin.last M.n) (lt_of_le_of_ne (Fin.le_last i) hne)))
  · intro he; rw [he]

/-- **MAIN, `⟸` direction.**  `LinearExtension ⟹ Faithful`.
No injectivity needed: only the last version is visible, so `Zphys` is a single. -/
theorem MOR.faithful_of_linear [DecidableEq V] (M : MOR V) (h : M.LinearExtension) :
    M.Faithful := by
  rw [Faithful, Zphys, M.visibleSet_of_linear h, Finset.sum_singleton]
  show distinct (Finsupp.single (M.d (Fin.last M.n)) 1) = Finsupp.single (M.d (Fin.last M.n)) 1
  apply distinct_of_indicator
  intro x
  rw [Finsupp.single_apply]
  by_cases hx : M.d (Fin.last M.n) = x <;> simp [hx]

/-! ### `⟹` : the counterexample (final-state Faithful does NOT imply LinearExtension) -/

/-- Versions `d = [0,1,2]`, seqs `s = [5,1,10]`.  `d 2` (the current version) is the
strict seq-maximum, so the final state is Faithful; but `s 0 = 5 > 1 = s 1`, so the
seq is **not** a linear extension of logical order. -/
def Mcex : MOR ℕ := ⟨2, ![0, 1, 2], ![5, 1, 10]⟩

theorem Mcex_inj : Function.Injective Mcex.d := by decide

theorem Mcex_faithful : Mcex.Faithful := by
  rw [Mcex.faithful_iff_visibleSet Mcex_inj]
  have hle : ∀ i, Mcex.s i ≤ Mcex.s (Fin.last 2) := by decide
  have hSD : Mcex.SD = Mcex.s (Fin.last 2) :=
    le_antisymm (Finset.sup_le (fun i _ => hle i)) (Mcex.le_SD _)
  ext i
  rw [Mcex.mem_visibleSet, hSD, mem_singleton]
  fin_cases i <;> decide

theorem Mcex_not_linear : ¬ Mcex.LinearExtension := by
  intro h
  exact absurd (h 0 1 (by decide)) (by decide)

/-- **MAIN, `⟹` direction FAILS** (machine-checked): there is an injective, Faithful
`MOR` whose seq is not a linear extension.  Hence `Faithful ↔ LinearExtension`
(with the final-state `Faithful` of def 6) is **false**; only `⟸` holds. -/
theorem main_necessity_fails :
    ∃ M : MOR ℕ, Function.Injective M.d ∧ M.Faithful ∧ ¬ M.LinearExtension :=
  ⟨Mcex, Mcex_inj, Mcex_faithful, Mcex_not_linear⟩

end Mor
