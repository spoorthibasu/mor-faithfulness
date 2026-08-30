import MorFaithful.Corollaries

/-!
# Global coherence across producers (the hardest part)

Now `seq` is assigned by **multiple producers** (subtasks).  Each version `d i` is
written by producer `p i`.  Two natural coherence notions:

* **`LocalCoherent`** — each producer keeps *its own* versions in increasing seq order.
* **`GlobalCoherent`** — a *single* seq order that is a linear extension of the global
  logical order (this is exactly `LinearExtension`).

**Claim proved here (the global-coherence theorem):**

1. `faithful_requires_global_coherence` — per-prefix Faithfulness across all producers
   forces a **single** global linear extension (`PrefixFaithful → GlobalCoherent`).
2. `global_coherence_suffices` — conversely, a single global linear extension gives
   per-prefix Faithfulness.
3. `global_implies_local` — global coherence implies local coherence (it is strictly
   stronger).
4. **`local_coherence_insufficient`** — the false step, machine-checked: there is a
   two-producer layout that is `LocalCoherent` (each producer locally monotone) yet
   **not Faithful**.  So per-subtask coherence is *not enough*; global coherence is
   genuinely required.

The witness: producer `A` writes `d 0, d 2` with seqs `1, 2` (locally increasing);
producer `B` writes `d 1` with seq `10`.  `B`'s seq `10` exceeds `A`'s *later* version's
seq `2`, so the global max delete seq is `10`, only `d 1` survives, and the current
version `d 2` is wrongly suppressed.
-/

namespace Mor

open Finset

namespace MOR

variable {V : Type*} (M : MOR V)

/-- Each producer keeps its own versions in increasing seq order. -/
def LocalCoherent (p : Fin (M.n + 1) → ℕ) : Prop :=
  ∀ i j, i < j → p i = p j → M.s i < M.s j

/-- A single global seq order that is a linear extension of logical order.
This is exactly `LinearExtension`. -/
def GlobalCoherent : Prop := M.LinearExtension

variable [DecidableEq V]

/-- **Necessity.**  Per-prefix Faithfulness across all producers forces a single global
linear extension of the logical order. -/
theorem faithful_requires_global_coherence (inj : Function.Injective M.d) :
    M.PrefixFaithful → M.GlobalCoherent :=
  M.linear_of_prefixFaithful inj

/-- **Sufficiency.**  A single global linear extension gives per-prefix Faithfulness. -/
theorem global_coherence_suffices : M.GlobalCoherent → M.PrefixFaithful :=
  M.prefixFaithful_of_linear

/-- Per-prefix Faithfulness ⟺ a single global linear extension. -/
theorem prefixFaithful_iff_globalCoherent (inj : Function.Injective M.d) :
    M.PrefixFaithful ↔ M.GlobalCoherent :=
  M.prefixFaithful_iff_linear inj

/-- Global coherence is strictly stronger than local coherence: it implies it for **every**
producer assignment. -/
theorem global_implies_local (p : Fin (M.n + 1) → ℕ) (h : M.GlobalCoherent) :
    M.LocalCoherent p :=
  fun i j hij _ => h i j hij

end MOR

/-! ### The false step: local coherence is insufficient -/

/-- Two producers.  Versions `d = [0,1,2]`, seqs `s = [1,10,2]`. -/
def Mglob : MOR ℕ := ⟨2, ![0, 1, 2], ![1, 10, 2]⟩

/-- Producer assignment: `A` (=0) writes versions 0 and 2; `B` (=1) writes version 1. -/
def pglob : Fin 3 → ℕ := ![0, 1, 0]

theorem Mglob_inj : Function.Injective Mglob.d := by decide

/-- Each producer is locally monotone: `A`'s versions `d 0, d 2` have seqs `1 < 2`. -/
theorem Mglob_localCoherent : Mglob.LocalCoherent pglob := by
  unfold MOR.LocalCoherent; decide

/-- But the seq is **not** a global linear extension: `d 1 < d 2` logically, yet
`s 1 = 10 > 2 = s 2`. -/
theorem Mglob_not_globalCoherent : ¬ Mglob.GlobalCoherent := by
  intro h
  exact absurd (h 1 2 (by decide)) (by decide)

/-- **The false step, machine-checked.**  `Mglob` is `LocalCoherent` yet **not Faithful**:
the stale version `d 1` (seq 10) suppresses the current version `d 2` (seq 2). -/
theorem Mglob_not_faithful : ¬ Mglob.Faithful := by
  rw [Mglob.faithful_iff_visibleSet Mglob_inj]
  intro hcontra
  have hmem : Fin.last Mglob.n ∈ Mglob.visibleSet := by
    rw [hcontra]; exact Finset.mem_singleton_self _
  rw [Mglob.mem_visibleSet] at hmem
  have hSD : Mglob.SD = 10 := by
    apply le_antisymm
    · apply Finset.sup_le; intro i _; fin_cases i <;> decide
    · exact le_trans (by decide) (Mglob.le_SD 1)
  rw [hSD] at hmem
  have hs2 : Mglob.s (Fin.last Mglob.n) = 2 := by decide
  omega

/-- **Global-coherence theorem (headline).**  There is a two-producer layout that is
`LocalCoherent` (each subtask monotone) but not Faithful.  Hence faithfulness across
producers cannot be guaranteed by per-subtask coherence — it requires a single linear
extension coherent across all subtasks (`GlobalCoherent`). -/
theorem local_coherence_insufficient :
    ∃ (M : MOR ℕ) (p : Fin (M.n + 1) → ℕ),
      Function.Injective M.d ∧ M.LocalCoherent p ∧ ¬ M.GlobalCoherent ∧ ¬ M.Faithful :=
  ⟨Mglob, pglob, Mglob_inj, Mglob_localCoherent, Mglob_not_globalCoherent, Mglob_not_faithful⟩

end Mor
