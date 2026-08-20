import MorFaithful.Global

/-!
# The A-del-all reduction, mechanized

The main development uses the **all-versions** delete model: every version (including the
initial insert `d 0`) emits an equality-delete at its own seq, so `S_D = supᵢ sᵢ`.

Here we define the **updates-only** model, in which only versions `1..n` emit deletes
(`d 0`, the insert, does not), so `S_D' = sup over {i : 0 < i} of s`.  We prove the two
models are **fully equivalent**:

* `faithful_iff_faithful'` : `Faithful ↔ Faithful'`  (under `Injective d`)
* `prefixFaithful_iff_prefixFaithful'` : `PrefixFaithful ↔ PrefixFaithful'`  (under `Injective d`)
* `LinearExtension` is model-independent (it never mentions deletes — literally the same
  predicate), and sufficiency transfers **without** injectivity (`faithful'_of_linear`).

**Reported form: FULL model-equivalence.**  The equivalence uses `Injective M.d` — this is
**not a new hypothesis**; it is the same (A-inj) already load-bearing for every all-versions
necessity result (`prefixFaithful_iff_linear`, COR2, global necessity).  It is genuinely
required: `del_reduction_needs_inj` exhibits a non-injective layout where the two models
disagree.  Sufficiency (COR1) transfers injectivity-free, exactly as in the all-versions
model.
-/

namespace Mor

open Finset

namespace MOR

variable {V : Type*} [DecidableEq V] (M : MOR V)

/-! ### Generic facts about indicator sums `∑_{i∈F} single (d i) 1`
(these generalize the `Zphys` lemmas in `Model.lean` to an arbitrary index set `F`). -/

theorem sum_single_apply_d (inj : Function.Injective M.d)
    (F : Finset (Fin (M.n + 1))) (i₀ : Fin (M.n + 1)) :
    (∑ i ∈ F, Finsupp.single (M.d i) (1 : ℤ)) (M.d i₀) = if i₀ ∈ F then 1 else 0 := by
  rw [Finsupp.finsetSum_apply]
  have hterm : ∀ i, (Finsupp.single (M.d i) (1 : ℤ)) (M.d i₀) = if i = i₀ then 1 else 0 := by
    intro i
    rw [Finsupp.single_apply]
    by_cases h : i = i₀
    · subst h; simp
    · rw [if_neg h, if_neg (fun hh => h (inj hh))]
  simp_rw [hterm]
  simp [Finset.sum_ite_eq']

theorem sum_single_zero_or_one (inj : Function.Injective M.d)
    (F : Finset (Fin (M.n + 1))) (a : V) :
    (∑ i ∈ F, Finsupp.single (M.d i) (1 : ℤ)) a = 0 ∨
      (∑ i ∈ F, Finsupp.single (M.d i) (1 : ℤ)) a = 1 := by
  rw [Finsupp.finsetSum_apply]
  simp_rw [Finsupp.single_apply]
  rw [Finset.sum_boole]
  have hle : (F.filter (fun i => M.d i = a)).card ≤ 1 := by
    apply Finset.card_le_one.2
    intro x hx y hy
    simp only [mem_filter] at hx hy
    exact inj (hx.2.trans hy.2.symm)
  have hc : (F.filter (fun i => M.d i = a)).card = 0 ∨
      (F.filter (fun i => M.d i = a)).card = 1 := by omega
  rcases hc with h0 | h1
  · left; rw [h0]; simp
  · right; rw [h1]; simp

theorem distinct_sum_single (inj : Function.Injective M.d) (F : Finset (Fin (M.n + 1))) :
    distinct (∑ i ∈ F, Finsupp.single (M.d i) (1 : ℤ)) =
      ∑ i ∈ F, Finsupp.single (M.d i) (1 : ℤ) :=
  distinct_of_indicator _ (fun a => M.sum_single_zero_or_one inj F a)

theorem sum_single_eq_single_iff (inj : Function.Injective M.d)
    (F : Finset (Fin (M.n + 1))) (k : Fin (M.n + 1)) :
    (∑ i ∈ F, Finsupp.single (M.d i) (1 : ℤ)) = Finsupp.single (M.d k) 1 ↔ F = {k} := by
  constructor
  · intro h
    ext i
    rw [mem_singleton]
    have hval : (∑ j ∈ F, Finsupp.single (M.d j) (1 : ℤ)) (M.d i) =
        (Finsupp.single (M.d k) 1) (M.d i) := by rw [h]
    rw [M.sum_single_apply_d inj F i, Finsupp.single_apply] at hval
    have hd : (M.d k = M.d i) ↔ (i = k) := ⟨fun he => (inj he).symm, fun he => by rw [he]⟩
    have hval2 : (if i ∈ F then (1 : ℤ) else 0) = if i = k then 1 else 0 := by
      rw [hval]; exact if_congr hd rfl rfl
    constructor
    · intro hi; by_contra he; rw [if_pos hi, if_neg he] at hval2; exact one_ne_zero hval2
    · intro he; by_contra hi; rw [if_neg hi, if_pos he] at hval2; exact zero_ne_one hval2
  · intro h
    rw [h, Finset.sum_singleton]

/-! ### The updates-only model -/

/-- `S_D'`: max delete seq when only updates `1..n` emit deletes (`d 0` does not). -/
def SD' : ℕ := (univ.filter (fun i => 0 < i.val)).sup M.s

/-- Visible indices under the updates-only suppression rule. -/
def visibleSet' : Finset (Fin (M.n + 1)) := univ.filter (fun i => M.SD' ≤ M.s i)

/-- Materialized Z-set in the updates-only model. -/
noncomputable def Zphys' : Zset V := ∑ i ∈ M.visibleSet', Finsupp.single (M.d i) 1

/-- Faithful in the updates-only model. -/
def Faithful' : Prop := distinct M.Zphys' = Finsupp.single M.cur 1

theorem mem_visibleSet' (i : Fin (M.n + 1)) : i ∈ M.visibleSet' ↔ M.SD' ≤ M.s i := by
  rw [visibleSet', mem_filter]; exact ⟨fun h => h.2, fun h => ⟨mem_univ i, h⟩⟩

/-- Reduction for the updates-only model. -/
theorem faithful'_iff_visibleSet' (inj : Function.Injective M.d) :
    M.Faithful' ↔ M.visibleSet' = {Fin.last M.n} := by
  rw [show M.Faithful' = (distinct M.Zphys' = Finsupp.single M.cur 1) from rfl,
      show M.Zphys' = ∑ i ∈ M.visibleSet', Finsupp.single (M.d i) 1 from rfl,
      M.distinct_sum_single inj M.visibleSet',
      show M.cur = M.d (Fin.last M.n) from rfl,
      M.sum_single_eq_single_iff inj M.visibleSet' (Fin.last M.n)]

/-! ### Both `visibleSet = {last}` conditions reduce to "current is the strict unique max" -/

theorem visibleSet_eq_last_iff :
    M.visibleSet = {Fin.last M.n} ↔
      ∀ i, i ≠ Fin.last M.n → M.s i < M.s (Fin.last M.n) := by
  constructor
  · intro h i hi
    have hlast : Fin.last M.n ∈ M.visibleSet := by rw [h]; exact mem_singleton_self _
    have hinot : i ∉ M.visibleSet := by rw [h, mem_singleton]; exact hi
    rw [mem_visibleSet] at hlast hinot
    have hle := M.le_SD i
    rw [← hlast] at hle hinot
    exact lt_of_le_of_ne hle hinot
  · intro h
    have hSD : M.SD = M.s (Fin.last M.n) := by
      apply le_antisymm
      · apply Finset.sup_le; intro i _
        by_cases hiL : i = Fin.last M.n
        · rw [hiL]
        · exact (h i hiL).le
      · exact M.le_SD _
    ext i
    rw [mem_visibleSet, mem_singleton, hSD]
    exact ⟨fun hsi => by by_contra hne; exact absurd hsi (ne_of_lt (h i hne)),
           fun he => by rw [he]⟩

theorem visibleSet'_eq_last_iff :
    M.visibleSet' = {Fin.last M.n} ↔
      ∀ i, i ≠ Fin.last M.n → M.s i < M.s (Fin.last M.n) := by
  constructor
  · intro h i hi
    have hlast : Fin.last M.n ∈ M.visibleSet' := by rw [h]; exact mem_singleton_self _
    have hinot : i ∉ M.visibleSet' := by rw [h, mem_singleton]; exact hi
    rw [mem_visibleSet'] at hlast hinot
    push_neg at hinot
    exact lt_of_lt_of_le hinot hlast
  · intro h
    have hSD'le : M.SD' ≤ M.s (Fin.last M.n) := by
      apply Finset.sup_le; intro i _
      by_cases hiL : i = Fin.last M.n
      · rw [hiL]
      · exact (h i hiL).le
    ext i
    rw [mem_visibleSet', mem_singleton]
    constructor
    · intro hsi
      by_cases hn : 0 < M.n
      · have hLmem : Fin.last M.n ∈ Finset.univ.filter (fun j : Fin (M.n + 1) => 0 < j.val) := by
          rw [mem_filter]; exact ⟨mem_univ _, by rw [Fin.val_last]; exact hn⟩
        have hSD'ge : M.s (Fin.last M.n) ≤ M.SD' := Finset.le_sup hLmem
        by_contra hne
        exact absurd (le_trans hSD'ge hsi) (not_le.mpr (h i hne))
      · apply Fin.ext
        have hi := i.isLt
        rw [Fin.val_last]
        omega
    · intro he; subst he; exact hSD'le

/-! ### Full model-equivalence (uses A-inj) -/

/-- **Model-equivalence for `Faithful`.**  Requires `Injective M.d` (A-inj). -/
theorem faithful_iff_faithful' (inj : Function.Injective M.d) :
    M.Faithful ↔ M.Faithful' := by
  rw [M.faithful_iff_visibleSet inj, M.faithful'_iff_visibleSet' inj,
      M.visibleSet_eq_last_iff, M.visibleSet'_eq_last_iff]

/-- Updates-only per-prefix faithfulness. -/
def PrefixFaithful' : Prop := ∀ m : Fin (M.n + 1), (M.take m).Faithful'

/-- **Model-equivalence for `PrefixFaithful`.**  Requires `Injective M.d` (A-inj). -/
theorem prefixFaithful_iff_prefixFaithful' (inj : Function.Injective M.d) :
    M.PrefixFaithful ↔ M.PrefixFaithful' := by
  constructor
  · intro h m
    exact ((M.take m).faithful_iff_faithful' (M.take_d_injective m inj)).mp (h m)
  · intro h m
    exact ((M.take m).faithful_iff_faithful' (M.take_d_injective m inj)).mpr (h m)

/-! ### Headline theorems transfer identically -/

/-- Corrected MAIN holds identically in the updates-only model. -/
theorem prefixFaithful'_iff_linear (inj : Function.Injective M.d) :
    M.PrefixFaithful' ↔ M.LinearExtension := by
  rw [← M.prefixFaithful_iff_prefixFaithful' inj]; exact M.prefixFaithful_iff_linear inj

/-- Sufficiency (COR1) transfers to the updates-only model with **no** injectivity. -/
theorem faithful'_of_linear (h : M.LinearExtension) : M.Faithful' := by
  have hSM : ∀ i, i ≠ Fin.last M.n → M.s i < M.s (Fin.last M.n) :=
    fun i hi => h i (Fin.last M.n) (lt_of_le_of_ne (Fin.le_last i) hi)
  have hvis : M.visibleSet' = {Fin.last M.n} := (M.visibleSet'_eq_last_iff).mpr hSM
  rw [show M.Faithful' = (distinct M.Zphys' = Finsupp.single M.cur 1) from rfl,
      show M.Zphys' = ∑ i ∈ M.visibleSet', Finsupp.single (M.d i) 1 from rfl, hvis,
      Finset.sum_singleton]
  show distinct (Finsupp.single (M.d (Fin.last M.n)) 1) = Finsupp.single (M.d (Fin.last M.n)) 1
  apply distinct_of_indicator
  intro x
  rw [Finsupp.single_apply]
  by_cases hx : M.d (Fin.last M.n) = x <;> simp [hx]

/-- Global-coherence necessity holds identically in the updates-only model. -/
theorem prefixFaithful'_iff_globalCoherent (inj : Function.Injective M.d) :
    M.PrefixFaithful' ↔ M.GlobalCoherent :=
  M.prefixFaithful'_iff_linear inj

end MOR

/-! ### A-inj is genuinely necessary for the model-equivalence

Without distinct version values the two delete models disagree, so `faithful_iff_faithful'`
cannot drop `Injective M.d`. -/

/-- `d 0 = d 2 = 0` (a no-op identity revisit of value `0`), `s = [10,5,3]`. -/
def Mnoninj : MOR ℕ := ⟨2, ![0, 1, 0], ![10, 5, 3]⟩

theorem Mnoninj_not_inj : ¬ Function.Injective Mnoninj.d := by
  intro h
  exact absurd (h (show Mnoninj.d 0 = Mnoninj.d 2 by decide)) (by decide)

/-- All-versions model: `d 0` (seq 10, the strict max) is the only visible record, and it
equals the current version, so the final state is Faithful. -/
theorem Mnoninj_faithful : Mnoninj.Faithful := by
  have hSD : Mnoninj.SD = 10 := by
    apply le_antisymm
    · apply Finset.sup_le; intro i _; fin_cases i <;> decide
    · exact le_trans (by decide) (Mnoninj.le_SD 0)
  have hvis : Mnoninj.visibleSet = {0} := by
    ext i; rw [Mnoninj.mem_visibleSet, hSD, mem_singleton]; fin_cases i <;> decide
  rw [show Mnoninj.Faithful = (distinct Mnoninj.Zphys = Finsupp.single Mnoninj.cur 1) from rfl,
      show Mnoninj.Zphys = ∑ i ∈ Mnoninj.visibleSet, Finsupp.single (Mnoninj.d i) 1 from rfl,
      hvis, Finset.sum_singleton]
  have h0 : Mnoninj.d 0 = 0 := by decide
  have hc : Mnoninj.cur = 0 := by decide
  rw [h0, hc]
  apply distinct_of_indicator
  intro x; rw [Finsupp.single_apply]; by_cases hx : (0 : ℕ) = x <;> simp [hx]

/-- Updates-only model: `d 0` (seq 10) and `d 1` (seq 5) are both visible (only `d 1, d 2`
emit deletes, with max delete seq 5), so the stale value `1` survives and the state is
NOT Faithful. -/
theorem Mnoninj_not_faithful' : ¬ Mnoninj.Faithful' := by
  have hf : (Finset.univ.filter (fun i : Fin 3 => 0 < i.val)) = {1, 2} := by
    ext i; fin_cases i <;> decide
  have hSD' : Mnoninj.SD' = 5 := by
    rw [show Mnoninj.SD' = (Finset.univ.filter (fun i : Fin 3 => 0 < i.val)).sup Mnoninj.s from rfl,
        hf]
    decide
  have hvis' : Mnoninj.visibleSet' = {0, 1} := by
    ext i; rw [Mnoninj.mem_visibleSet', hSD']; fin_cases i <;> decide
  have hZ1 : Mnoninj.Zphys' 1 = 1 := by
    rw [show Mnoninj.Zphys' = ∑ i ∈ Mnoninj.visibleSet', Finsupp.single (Mnoninj.d i) 1 from rfl,
        hvis', Finsupp.finsetSum_apply, Finset.sum_insert (by decide), Finset.sum_singleton]
    simp [Mnoninj, Finsupp.single_apply]
  intro h
  have h' : distinct Mnoninj.Zphys' = Finsupp.single Mnoninj.cur 1 := h
  have hval : (distinct Mnoninj.Zphys') 1 = (Finsupp.single Mnoninj.cur 1) 1 := by rw [h']
  rw [distinct_apply, hZ1] at hval
  have hc : Mnoninj.cur = 0 := by decide
  rw [hc, Finsupp.single_apply] at hval
  simp at hval

/-- **A-inj is necessary.**  There is a non-injective layout that is Faithful under the
all-versions model but not under the updates-only model — so `faithful_iff_faithful'`
genuinely requires `Injective M.d`. -/
theorem del_reduction_needs_inj :
    ∃ M : MOR ℕ, ¬ Function.Injective M.d ∧ M.Faithful ∧ ¬ M.Faithful' :=
  ⟨Mnoninj, Mnoninj_not_inj, Mnoninj_faithful, Mnoninj_not_faithful'⟩

end Mor
