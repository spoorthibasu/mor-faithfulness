import Mathlib

/-!
# Z-sets (defs 1–2)

Ported from `tchajed/database-stream-processing-theory` (`src/zset.lean`), which is
Lean 3 and therefore not importable here. That file models `Z[A]` as a
finitely-supported function `A → ℤ` (via `dfinsupp`) with
`distinct m a = if m a > 0 then 1 else 0`.

We reuse the *design* but express it with mathlib4's `Finsupp` (`V →₀ ℤ`), as the
project spec permits ("or reuse mathlib Finsupp").
-/

namespace Mor

/-- **Def 1.** A Z-set over `V`: a finitely supported function to `ℤ`.
Positive multiplicities are duplicates/insertions, negative are retractions. -/
abbrev Zset (V : Type*) := V →₀ ℤ

/-- **Def 2.** `distinct`: collapse positive multiplicities to `1`, everything else to `0`.
Matches DBSP `distinct m a = if m a > 0 then 1 else 0`. -/
noncomputable def distinct {V : Type*} (m : Zset V) : Zset V :=
  Finsupp.onFinset (m.support.filter (fun a => 0 < m a))
    (fun a => if 0 < m a then 1 else 0)
    (by
      intro a ha
      have h : 0 < m a := by by_contra h; simp [h] at ha
      simp only [Finset.mem_filter, Finsupp.mem_support_iff]
      exact ⟨h.ne', h⟩)

@[simp]
theorem distinct_apply {V : Type*} (m : Zset V) (a : V) :
    distinct m a = if 0 < m a then 1 else 0 :=
  Finsupp.onFinset_apply

/-- `distinct` only ever takes values `0` or `1`. -/
theorem distinct_zero_or_one {V : Type*} (m : Zset V) (a : V) :
    distinct m a = 0 ∨ distinct m a = 1 := by
  rw [distinct_apply]; split <;> simp

/-- The support of `distinct m` is exactly the set of strictly-positive-weight points. -/
theorem support_distinct {V : Type*} (m : Zset V) :
    (distinct m).support = m.support.filter (fun a => 0 < m a) := by
  ext a
  simp only [Finsupp.mem_support_iff, distinct_apply, Finset.mem_filter,
    Finsupp.mem_support_iff]
  constructor
  · intro h
    have hpos : 0 < m a := by
      by_contra hc
      rw [if_neg hc] at h
      exact h rfl
    exact ⟨hpos.ne', hpos⟩
  · rintro ⟨-, h⟩
    rw [if_pos h]; norm_num

/-- If a Z-set is already `0/1`-valued and nonnegative, `distinct` is the identity. -/
theorem distinct_of_indicator {V : Type*} (m : Zset V)
    (h : ∀ a, m a = 0 ∨ m a = 1) : distinct m = m := by
  ext a
  rw [distinct_apply]
  rcases h a with h0 | h1
  · rw [h0]; simp
  · rw [h1]; norm_num

end Mor
