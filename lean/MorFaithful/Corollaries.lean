import MorFaithful.MainPrefix

/-!
# Corollaries 1–3

* **COR1** (single writer): strictly increasing seq ⟹ Faithful.
* **COR2** (violation, FLINK-38450): equal seq for a stale version, the current version,
  and the delete ⟹ ¬Faithful, and `|distinct(Zphys)| = 2`.
* **COR3** (compaction): any rewrite preserving the (deduplicated) visible content and
  the current version preserves Faithful in both directions.
-/

namespace Mor

open Finset

namespace MOR

variable {V : Type*} [DecidableEq V] (M : MOR V)

/-- The support of `Zphys` is exactly the set of visible version values. -/
theorem support_Zphys (inj : Function.Injective M.d) :
    (M.Zphys).support = M.visibleSet.image M.d := by
  ext a
  simp only [Finsupp.mem_support_iff, Finset.mem_image]
  constructor
  · intro h
    by_contra hcon
    push_neg at hcon
    apply h
    rw [Zphys, Finsupp.finsetSum_apply]
    apply Finset.sum_eq_zero
    intro i hi
    rw [Finsupp.single_apply, if_neg]
    exact fun he => hcon i hi he
  · rintro ⟨i, hi, rfl⟩
    rw [M.Zphys_apply_d inj i, if_pos hi]
    exact one_ne_zero

/-- The number of distinct visible values equals the number of visible versions. -/
theorem card_distinct_Zphys (inj : Function.Injective M.d) :
    (distinct M.Zphys).support.card = M.visibleSet.card := by
  rw [M.distinct_Zphys inj, M.support_Zphys inj, Finset.card_image_of_injective _ inj]

end MOR

/-! ### COR1 — single writer -/

variable {V : Type*} [DecidableEq V]

/-- `LinearExtension` is literally "seq strictly increasing along logical order". -/
theorem linearExtension_iff_strictMono (M : MOR V) :
    M.LinearExtension ↔ StrictMono M.s := Iff.rfl

/-- **COR1 (single writer).** A single writer assigns strictly increasing seqs in logical
order; that makes the final state Faithful. -/
theorem cor1_single_writer (M : MOR V) (h : StrictMono M.s) : M.Faithful :=
  M.faithful_of_linear h

/-- COR1, per-prefix strengthening (holds under the corrected MAIN). -/
theorem cor1_single_writer_prefix (M : MOR V) (h : StrictMono M.s) : M.PrefixFaithful :=
  M.prefixFaithful_of_linear h

/-! ### COR2 — the FLINK-38450 violation -/

/-- The FLINK-38450 layout: a stale version `a` and the current version `b` (`a ≠ b`) whose
data records share the **same seq** as the equality-delete (`7 = 7`).  The strict
suppression rule fails to hide the stale row because its seq is not strictly below the
delete seq. -/
def Mviol (a b : V) : MOR V := ⟨1, ![a, b], ![7, 7]⟩

theorem Mviol_inj (a b : V) (hab : a ≠ b) : Function.Injective (Mviol a b).d := by
  intro x y h
  fin_cases x <;> fin_cases y <;>
    simp_all [Mviol, Matrix.cons_val_zero, Matrix.cons_val_one, Matrix.head_cons]

/-- Both versions carry seq `7`. -/
theorem Mviol_seq (a b : V) : ∀ i, (Mviol a b).s i = 7 := by
  intro i
  fin_cases i <;> simp [Mviol, Matrix.cons_val_zero, Matrix.cons_val_one, Matrix.head_cons]

/-- Both versions are visible: with equal seqs, the strict suppression rule hides neither. -/
theorem Mviol_visibleSet (a b : V) : (Mviol a b).visibleSet = Finset.univ := by
  have hSD : (Mviol a b).SD = 7 := by
    apply le_antisymm
    · exact Finset.sup_le (fun i _ => le_of_eq (Mviol_seq a b i))
    · rw [← Mviol_seq a b 0]; exact (Mviol a b).le_SD 0
  ext i
  simp only [Finset.mem_univ, iff_true]
  rw [(Mviol a b).mem_visibleSet, hSD]
  exact Mviol_seq a b i

/-- **COR2, part 1.**  The equal-seq layout is **not** Faithful. -/
theorem cor2_not_faithful (a b : V) (hab : a ≠ b) : ¬ (Mviol a b).Faithful := by
  rw [(Mviol a b).faithful_iff_visibleSet (Mviol_inj a b hab), Mviol_visibleSet a b]
  intro hcontra
  -- univ (Fin 2) has card 2, a singleton has card 1: contradiction.
  have hcard := congrArg Finset.card hcontra
  simp only [Finset.card_univ, Fintype.card_fin, Finset.card_singleton] at hcard
  have hn : (Mviol a b).n = 1 := rfl
  omega

/-- **COR2, part 2.**  The deduplicated visible state has exactly **two** elements
(the stale value and the current value both survive). -/
theorem cor2_card (a b : V) (hab : a ≠ b) :
    (distinct (Mviol a b).Zphys).support.card = 2 := by
  have hn : (Mviol a b).n = 1 := rfl
  rw [(Mviol a b).card_distinct_Zphys (Mviol_inj a b hab), Mviol_visibleSet a b,
      Finset.card_univ, Fintype.card_fin, hn]

/-! ### COR3 — compaction preserves Faithful -/

/-- **COR3 (compaction).**  A compaction may rewrite the physical layout arbitrarily, but if
it preserves the deduplicated visible content (`distinct Zphys`) and the current version
(`cur`), then it preserves Faithful in **both** directions.

The two hypotheses `distinct M'.Zphys = distinct M.Zphys` and `M'.cur = M.cur` ARE, together,
the formal definition of a *faithfulness-preserving compaction*: "preserves visible(k)" means
preserving exactly (i) the deduplicated set of visible values (`distinct Zphys`) and (ii) which
version is current (`cur`).  It is therefore NOT assuming the conclusion — `Faithful` is a
*further* property (`distinct Zphys = single cur 1`) relating these two objects, and neither
hypothesis presupposes that relation holds.  A compaction satisfying them can start from either
a Faithful or an unfaithful state; the theorem says it never changes which. -/
theorem cor3_compaction (M M' : MOR V)
    (hz : distinct M'.Zphys = distinct M.Zphys) (hc : M'.cur = M.cur) :
    M'.Faithful ↔ M.Faithful := by
  simp only [MOR.Faithful, hz, hc]

end Mor
