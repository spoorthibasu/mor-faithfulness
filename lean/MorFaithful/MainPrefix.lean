import MorFaithful.Main

/-!
# Corrected MAIN: `PrefixFaithful ↔ LinearExtension`

`main_necessity_fails` (in `Main.lean`) shows the informal MAIN is false in the `⟹`
direction for the **final-state** `Faithful` of def 6.  The missing hypothesis is
**per-prefix faithfulness**: the materialization must be correct after *every* update,
not only at the end.

Here we add `PrefixFaithful` (faithful on every logical prefix `0..m`) and prove
`PrefixFaithful ↔ LinearExtension` in full, both directions, no `sorry`.  This keeps
your `LinearExtension` (def 7) exactly and turns MAIN into a genuine `↔`.

The `⟸` direction reuses `faithful_of_linear` on each prefix; the `⟹` direction reads
off, from the prefix ending at `j`, that every earlier version `i < j` must be strictly
below `j` in seq — i.e. strict monotonicity across all pairs.
-/

namespace Mor

open Finset

namespace MOR

variable {V : Type*} (M : MOR V)

/-- Embed a prefix index `i : Fin (m+1)` back into the full index set `Fin (n+1)`. -/
def emb (m : Fin (M.n + 1)) (i : Fin (m.val + 1)) : Fin (M.n + 1) :=
  ⟨i.val, by have := m.isLt; have := i.isLt; omega⟩

@[simp] theorem emb_val (m : Fin (M.n + 1)) (i : Fin (m.val + 1)) :
    (M.emb m i).val = i.val := rfl

/-- The prefix of the changelog and seqs up to (and including) version `m`. -/
def take (m : Fin (M.n + 1)) : MOR V where
  n := m.val
  d := fun i => M.d (M.emb m i)
  s := fun i => M.s (M.emb m i)

@[simp] theorem take_n (m : Fin (M.n + 1)) : (M.take m).n = m.val := rfl
@[simp] theorem take_d (m : Fin (M.n + 1)) (i) : (M.take m).d i = M.d (M.emb m i) := rfl
@[simp] theorem take_s (m : Fin (M.n + 1)) (i) : (M.take m).s i = M.s (M.emb m i) := rfl

theorem emb_last (m : Fin (M.n + 1)) : M.emb m (Fin.last m.val) = m := by
  apply Fin.ext; simp [emb, Fin.val_last]

theorem emb_injective (m : Fin (M.n + 1)) : Function.Injective (M.emb m) := by
  intro a b h
  apply Fin.ext
  have := congrArg Fin.val h
  simpa [emb] using this

theorem take_d_injective (m : Fin (M.n + 1)) (inj : Function.Injective M.d) :
    Function.Injective (M.take m).d := by
  intro a b h
  simp only [take_d] at h
  exact M.emb_injective m (inj h)

/-- The prefix of a linear extension is again a linear extension. -/
theorem take_linear (m : Fin (M.n + 1)) (h : M.LinearExtension) :
    (M.take m).LinearExtension := by
  intro i j hij
  simp only [take_s]
  apply h
  rw [Fin.lt_def] at hij ⊢
  simpa [emb] using hij

variable [DecidableEq V]

/-- **PrefixFaithful.** The materialization is faithful after **every** update: for each
`m`, the reconstruction from versions `0..m` equals the singleton `{ d m ↦ 1 }`. -/
def PrefixFaithful : Prop := ∀ m : Fin (M.n + 1), (M.take m).Faithful

/-- **Corrected MAIN, `⟸`.**  `LinearExtension ⟹ PrefixFaithful`. -/
theorem prefixFaithful_of_linear (h : M.LinearExtension) : M.PrefixFaithful :=
  fun m => (M.take m).faithful_of_linear (M.take_linear m h)

/-- **Corrected MAIN, `⟹`.**  `PrefixFaithful ⟹ LinearExtension` (needs injective versions). -/
theorem linear_of_prefixFaithful (inj : Function.Injective M.d) (h : M.PrefixFaithful) :
    M.LinearExtension := by
  intro i j hij
  have hij' : i.val < j.val := by rwa [Fin.lt_def] at hij
  have hfaith := h j
  rw [(M.take j).faithful_iff_visibleSet (M.take_d_injective j inj)] at hfaith
  -- hfaith : (M.take j).visibleSet = {Fin.last (M.take j).n}
  have hib : i.val < (M.take j).n + 1 := by simp only [take_n]; omega
  have hiP_ne : (⟨i.val, hib⟩ : Fin ((M.take j).n + 1)) ≠ Fin.last (M.take j).n := by
    intro hc
    rw [Fin.ext_iff] at hc
    simp only [Fin.val_last, take_n] at hc
    omega
  have hi_not : (⟨i.val, hib⟩ : Fin ((M.take j).n + 1)) ∉ (M.take j).visibleSet := by
    rw [hfaith, mem_singleton]; exact hiP_ne
  have hlast_in : Fin.last (M.take j).n ∈ (M.take j).visibleSet := by
    rw [hfaith]; exact mem_singleton_self _
  rw [(M.take j).mem_visibleSet] at hi_not hlast_in
  have hlt : (M.take j).s ⟨i.val, hib⟩ < (M.take j).SD :=
    lt_of_le_of_ne ((M.take j).le_SD ⟨i.val, hib⟩) hi_not
  have e1 : (M.take j).s ⟨i.val, hib⟩ = M.s i := rfl
  have e2 : (M.take j).s (Fin.last (M.take j).n) = M.s j := rfl
  rw [← hlast_in, e1, e2] at hlt
  exact hlt

/-- **Corrected MAIN.**  `PrefixFaithful ↔ LinearExtension` (with distinct version values). -/
theorem prefixFaithful_iff_linear (inj : Function.Injective M.d) :
    M.PrefixFaithful ↔ M.LinearExtension :=
  ⟨M.linear_of_prefixFaithful inj, M.prefixFaithful_of_linear⟩

end MOR

end Mor
