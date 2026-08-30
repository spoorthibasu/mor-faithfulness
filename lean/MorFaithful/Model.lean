import MorFaithful.Zset

/-!
# MOR model: keyed changelog, physical layout, visibility, Faithful, LinearExtension
(defs 3–7)

We model **one key `k`** throughout (all objects are implicitly for a fixed `k`).

The analysis object is `MOR`, bundling
* a keyed changelog (def 3): versions `d 0, …, d n` in logical order, `d n` current;
* a sequence assignment `s : Fin (n+1) → ℕ` giving each version's data-record seq.

Under the merge-on-read (MOR) equality-delete write rule, writing version `d i`
emits a data record `(d i, s i)` **and** an equality-delete carrying its own
version's seq `s i` (def 7's "each update's delete carries its own version's seq").
Hence the multiset of delete seqs is `{ s i }`, and the max delete seq is
`S_D = sup_i s i` (def 5).  The standalone `KeyedChangelog` / `PhysicalLayout`
structures below record defs 3–4 literally; `MOR.changelog` / `MOR.layout` show
`MOR` realizes them.

Modeling choices flagged for the report:
* **(A-inj)** version values are pairwise distinct (`Function.Injective d`).
* **(A-del-all)** every version (including the initial insert `d 0`) contributes a
  delete at its own seq, so `S_D = sup_i s i`.  Restricting deletes to updates
  `1..n` changes nothing in any theorem below (see report).
-/

namespace Mor

open Finset

/-- **Def 3.** A keyed changelog: the versions `d 0 … d n` for one key, in logical order. -/
structure KeyedChangelog (V : Type*) where
  n : ℕ
  d : Fin (n + 1) → V

/-- **Def 4.** A physical layout for one key: data records `(value, seq)` and
equality-delete seqs. -/
structure PhysicalLayout (V : Type*) where
  data : List (V × ℕ)
  dels : List ℕ

/-- The analysis object: a changelog plus a per-version seq assignment. -/
structure MOR (V : Type*) where
  n : ℕ
  /-- def 3: versions in logical order; `d (Fin.last n)` is the current version. -/
  d : Fin (n + 1) → V
  /-- def 4/7: seq of each version's data record (its equality-delete carries the same seq). -/
  s : Fin (n + 1) → ℕ

namespace MOR

variable {V : Type*} (M : MOR V)

/-- `MOR` realizes def 3. -/
def changelog : KeyedChangelog V := ⟨M.n, M.d⟩

/-- `MOR` realizes def 4: each version → one data record + one equality-delete at its own seq. -/
def layout : PhysicalLayout V :=
  { data := List.ofFn (fun i => (M.d i, M.s i))
    dels := List.ofFn (fun i => M.s i) }

/-- The current version `d n`. -/
def cur : V := M.d (Fin.last M.n)

/-- **Def 5.** `S_D(k)` = max delete seq = `sup_i s i` (deletes carry own-version seq). -/
def SD : ℕ := univ.sup M.s

/-- **Def 5.** version `i` is *visible* under the strict suppression rule: it survives
all equality-deletes iff its seq is `≥` the max delete seq. -/
def visible (i : Fin (M.n + 1)) : Prop := M.SD ≤ M.s i

/-- The (decidable) set of visible version indices. -/
def visibleSet : Finset (Fin (M.n + 1)) := univ.filter (fun i => M.SD ≤ M.s i)

/-- **Def 4/materialization.** The physical Z-set for key `k`: `+1` per visible data record. -/
noncomputable def Zphys : Zset V := ∑ i ∈ M.visibleSet, Finsupp.single (M.d i) 1

/-- **Def 6.** Faithful: `distinct(Zphys)` is exactly the singleton `{ d n ↦ 1 }`.
This is a **set-equality** of Z-sets, not a count. -/
def Faithful : Prop := distinct M.Zphys = Finsupp.single M.cur 1

/-- **Def 7.** LinearExtension: seq is strictly increasing along logical order.
(The "delete carries its own version's seq" clause is baked into `SD`.) -/
def LinearExtension : Prop := ∀ i j : Fin (M.n + 1), i < j → M.s i < M.s j

/-! ### Basic facts about visibility and `SD` -/

theorem le_SD (i : Fin (M.n + 1)) : M.s i ≤ M.SD := Finset.le_sup (mem_univ i)

/-- A version is visible **iff its seq equals the global max seq** (is an argmax). -/
theorem mem_visibleSet (i : Fin (M.n + 1)) : i ∈ M.visibleSet ↔ M.s i = M.SD := by
  rw [visibleSet, mem_filter]
  constructor
  · rintro ⟨-, h⟩; exact le_antisymm (M.le_SD i) h
  · intro h; exact ⟨mem_univ i, h.ge⟩

/-! ### The reduction lemma: `Faithful ↔ visibleSet = {last}`

**The (A-inj) assumption `Function.Injective M.d` is introduced here** and threaded through the
lemmas below.  It says the version values `d 0, …, d n` are pairwise distinct.

* It is **load-bearing** for: necessity (`Zphys_eq_single_iff` ⟹, hence `main_necessity` and
  the ⟹ half of `prefixFaithful_iff_linear`), COR2's `|distinct(Zphys)| = 2`
  (`card_distinct_Zphys`), the global-coherence necessity (`faithful_requires_global_coherence`),
  and the all-vs-updates delete-model equivalence (`faithful_iff_faithful'`; see
  `del_reduction_needs_inj` for a compiled witness that it cannot be dropped).
* It is **NOT needed for sufficiency**: `faithful_of_linear` (⟸ / COR1) and `faithful'_of_linear`
  hold without it, because under a linear extension only the current version is visible, so
  `Zphys` is already a single point.
* **Real-world reading:** two logically distinct versions with byte-identical values are
  indistinguishable to the store (they are the same Z-set element).  A-inj therefore excludes
  only *no-op identity updates* — a key overwritten with a value equal to its current one.  It
  is a statement about values, not about update history. -/

variable [DecidableEq V]

/-- Evaluating `Zphys` at a version value `d i₀` counts (via injectivity) whether `i₀`
is visible. -/
theorem Zphys_apply_d (inj : Function.Injective M.d) (i₀ : Fin (M.n + 1)) :
    M.Zphys (M.d i₀) = if i₀ ∈ M.visibleSet then 1 else 0 := by
  rw [Zphys, Finsupp.finset_sum_apply]
  have hterm : ∀ i, (Finsupp.single (M.d i) (1 : ℤ)) (M.d i₀) = if i = i₀ then 1 else 0 := by
    intro i
    rw [Finsupp.single_apply]
    by_cases h : i = i₀
    · subst h; simp
    · rw [if_neg h, if_neg (fun hh => h (inj hh))]
  simp_rw [hterm]
  simp [Finset.sum_ite_eq']

/-- `Zphys` is `0/1`-valued (an indicator), so it is fixed by `distinct`. -/
theorem Zphys_zero_or_one (inj : Function.Injective M.d) (a : V) :
    M.Zphys a = 0 ∨ M.Zphys a = 1 := by
  rw [Zphys, Finsupp.finset_sum_apply]
  simp_rw [Finsupp.single_apply]
  rw [Finset.sum_boole]
  have hle : (M.visibleSet.filter (fun i => M.d i = a)).card ≤ 1 := by
    apply Finset.card_le_one.2
    intro x hx y hy
    simp only [mem_filter] at hx hy
    exact inj (hx.2.trans hy.2.symm)
  have hc : (M.visibleSet.filter (fun i => M.d i = a)).card = 0 ∨
      (M.visibleSet.filter (fun i => M.d i = a)).card = 1 := by omega
  rcases hc with h0 | h1
  · left; rw [h0]; simp
  · right; rw [h1]; simp

theorem distinct_Zphys (inj : Function.Injective M.d) : distinct M.Zphys = M.Zphys :=
  distinct_of_indicator _ (M.Zphys_zero_or_one inj)

/-- Key structural lemma: `Zphys = {d n ↦ 1}` exactly when the last version is the
unique visible one.  Uses injectivity (distinct version values). -/
theorem Zphys_eq_single_iff (inj : Function.Injective M.d) :
    M.Zphys = Finsupp.single M.cur 1 ↔ M.visibleSet = {Fin.last M.n} := by
  constructor
  · intro h
    ext i
    rw [mem_singleton]
    have hval : M.Zphys (M.d i) = (Finsupp.single M.cur 1) (M.d i) := by rw [h]
    rw [M.Zphys_apply_d inj i, cur, Finsupp.single_apply] at hval
    -- hval : (if i ∈ visibleSet then 1 else 0) = if (d last) = (d i) then 1 else 0
    have hd : (M.d (Fin.last M.n) = M.d i) ↔ (i = Fin.last M.n) :=
      ⟨fun he => (inj he).symm, fun he => by rw [he]⟩
    have hval2 : (if i ∈ M.visibleSet then (1 : ℤ) else 0)
        = if i = Fin.last M.n then 1 else 0 := by
      rw [hval]; exact if_congr hd rfl rfl
    constructor
    · intro hi; by_contra he
      rw [if_pos hi, if_neg he] at hval2; exact one_ne_zero hval2
    · intro he; by_contra hi
      rw [if_neg hi, if_pos he] at hval2; exact zero_ne_one hval2
  · intro h
    rw [Zphys, h, Finset.sum_singleton, cur]

/-- **Reduction lemma.** Faithful ⟺ the current version is the unique visible one. -/
theorem faithful_iff_visibleSet (inj : Function.Injective M.d) :
    M.Faithful ↔ M.visibleSet = {Fin.last M.n} := by
  rw [Faithful, M.distinct_Zphys inj, M.Zphys_eq_single_iff inj]

end MOR

end Mor
