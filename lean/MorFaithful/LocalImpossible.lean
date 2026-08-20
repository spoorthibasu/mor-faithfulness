import MorFaithful.UpdatesModel

/-!
# Claim B — the impossibility of purely-local ordering (coordination is *necessary*)

`local_coherence_insufficient` (in `Global.lean`) exhibits **one** two-producer layout
that is locally coherent yet not Faithful.  This file strengthens it into a genuine
**impossibility**, quantified over *every* ordering scheme that uses only per-writer-local
information:

> **No purely-local ordering scheme can guarantee global faithfulness.**

Precisely: for *any* rule by which each writer stamps its own events using only its own
local data (no shared/global sequencer, no cross-writer agreement), there is a multi-writer
configuration whose resulting physical order is **not** a global linear extension of logical
order — and hence (by the main theorem) not faithful.

## What "purely-local" means here (the crux)

A configuration (`Config`) is the versions `d 0 … d n` of one key in global logical order,
each tagged with the id `w i` of the writer that produced it.  Writer `x`'s *only* handle
on the world is:

* `localView C x` — the list of values *it itself* wrote, in its own order; and
* `localPos C i` — which of *its own* events version `i` is (its 0-based local index).

A **`LocalScheme`** is a single function `f : ℕ → List V → ℕ → ℕ`.  The seq stamped on
version `i` is `f (w i) (localView C (w i)) (localPos C i)`.  Locality is enforced *by the
domain of `f`*: its arguments are the writer's own id, the writer's own value-list, and the
local index — and **nothing else**.  There is no argument carrying another writer's events,
the global interleaving of the streams, or a shared counter.  Quantifying over all `f`
therefore quantifies over every scheme buildable from per-writer-local information alone
(local counters, per-writer static offsets, hashes of the local payload, unsynchronised
local clocks, …), while excluding exactly the schemes that need cross-writer coordination
or a global sequencing authority.

Handing `f` the writer's *entire* local list (not merely the prefix up to `localPos`) only
makes it *more* powerful; the impossibility below is correspondingly stronger.

## The witness (obliviousness to interleaving)

Two writers, `0` and `1`, writing values `a` and `b` (`a ≠ b`).  Consider the two
configurations that are *locally indistinguishable* to both writers:

* `C₁`: logical order `a, b` (writer `0` then writer `1`);  current version `= b`.
* `C₂`: logical order `b, a` (writer `1` then writer `0`);  current version `= a`.

In BOTH, writer `0`'s local view is `[a]` at local position `0`, and writer `1`'s is `[b]`
at local position `0`.  So `f` stamps writer `0`'s event with the *same* seq `σₐ := f 0 [a] 0`
and writer `1`'s with the *same* `σ_b := f 1 [b] 0` in both configs — it *cannot* see which
came first globally.  Faithfulness of `C₁` needs `σₐ < σ_b` (stale `a` below current `b`);
faithfulness of `C₂` needs `σ_b < σₐ`.  These contradict, so at least one config is
unfaithful.  Whichever it is, its seq is also not a linear extension — the connection the
main theorem makes.
-/

namespace Mor

open Finset

/-- A **multi-writer configuration** for one key: the versions `d 0 … d n` in global logical
order (`d n` current), each tagged with the id `w i` of the writer that produced it.  This is
exactly the `MOR` changelog data (`n`, `d`) together with a producer assignment `w` (the `p`
of `Global.lean`). -/
structure Config (V : Type*) where
  n : ℕ
  d : Fin (n + 1) → V
  w : Fin (n + 1) → ℕ

namespace Config

variable {V : Type*} (C : Config V)

/-- Writer `x`'s **local view**: the values of the versions *it* produced, in logical order.
This is the ONLY event data a purely-local scheme may read for writer `x`; it never contains
another writer's versions nor any record of the global interleaving. -/
def localView (x : ℕ) : List V :=
  (List.finRange (C.n + 1)).filterMap (fun i => if C.w i = x then some (C.d i) else none)

/-- The **local position** of version `i` within its own writer's stream: how many
logically-earlier versions share its writer (the 0-based index of `i` among writer `w i`'s
events). -/
def localPos (i : Fin (C.n + 1)) : ℕ :=
  (univ.filter (fun j : Fin (C.n + 1) => j < i ∧ C.w j = C.w i)).card

end Config

/-- A **purely-local ordering scheme**: the single rule every writer runs to stamp its own
events.  Writer `w`, holding its own local value-stream `L`, stamps its `k`-th local event
with seq `f w L k`.

Locality is enforced by the *domain*: the arguments are the writer's own id, its own list of
values, and the local index — there is deliberately no argument for other writers' events,
the global interleaving, or a shared/global counter. -/
def LocalScheme (V : Type*) := ℕ → List V → ℕ → ℕ

/-- The `MOR` obtained by running a purely-local scheme `f` on a configuration `C`: every
version `i` is stamped `f (its writer) (its writer's local view) (its local index)`. -/
def LocalScheme.run {V : Type*} (f : LocalScheme V) (C : Config V) : MOR V where
  n := C.n
  d := C.d
  s := fun i => f (C.w i) (C.localView (C.w i)) (C.localPos i)

@[simp] theorem LocalScheme.run_n {V : Type*} (f : LocalScheme V) (C : Config V) :
    (f.run C).n = C.n := rfl
@[simp] theorem LocalScheme.run_d {V : Type*} (f : LocalScheme V) (C : Config V) :
    (f.run C).d = C.d := rfl
theorem LocalScheme.run_s {V : Type*} (f : LocalScheme V) (C : Config V) (i) :
    (f.run C).s i = f (C.w i) (C.localView (C.w i)) (C.localPos i) := rfl

/-! ### The two locally-indistinguishable configurations -/

variable {V : Type*}

/-- `C₁`: logical order `a, b`; writer `0` writes `a`, writer `1` writes `b`. -/
def cfgAB (a b : V) : Config V := ⟨1, ![a, b], ![0, 1]⟩

/-- `C₂`: logical order `b, a`; writer `1` writes `b`, writer `0` writes `a`. -/
def cfgBA (a b : V) : Config V := ⟨1, ![b, a], ![1, 0]⟩

/-! Local views and positions are identical per writer across the two configs: writer `0`
always sees `[a]` at position `0`, writer `1` always sees `[b]` at position `0`.  Hence a
purely-local `f` stamps the same seqs in both. -/

theorem run_cfgAB_s0 (f : LocalScheme V) (a b : V) :
    (f.run (cfgAB a b)).s 0 = f 0 [a] 0 := rfl
theorem run_cfgAB_s1 (f : LocalScheme V) (a b : V) :
    (f.run (cfgAB a b)).s 1 = f 1 [b] 0 := rfl
theorem run_cfgBA_s0 (f : LocalScheme V) (a b : V) :
    (f.run (cfgBA a b)).s 0 = f 1 [b] 0 := rfl
theorem run_cfgBA_s1 (f : LocalScheme V) (a b : V) :
    (f.run (cfgBA a b)).s 1 = f 0 [a] 0 := rfl

/-! ### Structural facts common to both configs -/

theorem cfgAB_inj (f : LocalScheme V) (a b : V) (hab : a ≠ b) :
    Function.Injective (f.run (cfgAB a b)).d := by
  simp only [LocalScheme.run_d]
  intro x y h
  fin_cases x <;> fin_cases y <;>
    simp_all [cfgAB, Matrix.cons_val_zero, Matrix.cons_val_one]

theorem cfgBA_inj (f : LocalScheme V) (a b : V) (hab : a ≠ b) :
    Function.Injective (f.run (cfgBA a b)).d := by
  simp only [LocalScheme.run_d]
  intro x y h
  fin_cases x <;> fin_cases y <;>
    simp_all [cfgBA, Matrix.cons_val_zero, Matrix.cons_val_one]

/-- In `cfgAB` the two versions have *different* writers, so local coherence
(each writer monotone) holds vacuously — the failure is *not* a local-coherence violation. -/
theorem cfgAB_localCoherent (f : LocalScheme V) (a b : V) :
    (f.run (cfgAB a b)).LocalCoherent (cfgAB a b).w := by
  intro i j hij hw
  have hwinj : Function.Injective (cfgAB a b).w := by
    intro x y h
    fin_cases x <;> fin_cases y <;>
      simp_all [cfgAB, Matrix.cons_val_zero, Matrix.cons_val_one]
  exact absurd (hwinj hw) (ne_of_lt hij)

theorem cfgBA_localCoherent (f : LocalScheme V) (a b : V) :
    (f.run (cfgBA a b)).LocalCoherent (cfgBA a b).w := by
  intro i j hij hw
  have hwinj : Function.Injective (cfgBA a b).w := by
    intro x y h
    fin_cases x <;> fin_cases y <;>
      simp_all [cfgBA, Matrix.cons_val_zero, Matrix.cons_val_one]
  exact absurd (hwinj hw) (ne_of_lt hij)

/-! ### The impossibility theorem -/

variable [DecidableEq V]

/-- **Claim B — impossibility of purely-local ordering.**

For *every* purely-local ordering scheme `f`, there is a two-writer configuration `C` such
that the physically-assigned order `f.run C`:

* has distinct version values (`Injective`, so the (A-inj) hypothesis of the main theorem
  holds);
* is `LocalCoherent` — every writer is even individually monotone (the failure is genuinely
  about the *lack of cross-writer coordination*, not about any writer misbehaving locally);
* is **not** `GlobalCoherent`, i.e. not a global linear extension of logical order; and
* is **not** `Faithful`.

Hence no scheme that assigns ordering values from per-writer-local information alone can
guarantee global faithfulness: coordination (a shared/global sequencer) is necessary. -/
theorem local_scheme_admits_unfaithful_config (a b : V) (hab : a ≠ b) (f : LocalScheme V) :
    ∃ C : Config V,
      Function.Injective (f.run C).d ∧
      (f.run C).LocalCoherent C.w ∧
      ¬ (f.run C).GlobalCoherent ∧
      ¬ (f.run C).Faithful := by
  rcases Nat.lt_or_ge (f 1 [b] 0) (f 0 [a] 0) with hcase | hcase
  · -- `σ_b < σₐ`: in `cfgAB` the current version `b` (seq `σ_b`) is dominated by stale `a`.
    refine ⟨cfgAB a b, cfgAB_inj f a b hab, cfgAB_localCoherent f a b, ?_, ?_⟩
    · -- ¬ GlobalCoherent : a linear extension would need `s 0 < s 1`, i.e. `σₐ < σ_b`
      intro hG
      have hlt := hG 0 1 (by show (0 : Fin 2) < (1 : Fin 2); decide)
      rw [run_cfgAB_s0, run_cfgAB_s1] at hlt
      exact absurd hlt (not_lt.mpr (le_of_lt hcase))
    · -- ¬ Faithful : the stale version 0 (`a`, seq `σₐ`) is not below the current one (`b`, `σ_b`)
      rw [(f.run (cfgAB a b)).faithful_iff_visibleSet (cfgAB_inj f a b hab),
          MOR.visibleSet_eq_last_iff]
      intro hall
      have hslast : (f.run (cfgAB a b)).s (Fin.last (f.run (cfgAB a b)).n) = f 1 [b] 0 := rfl
      have hlt := hall 0 (by show (0 : Fin 2) ≠ (1 : Fin 2); decide)
      rw [run_cfgAB_s0, hslast] at hlt
      exact absurd hlt (not_lt.mpr (le_of_lt hcase))
  · -- `σₐ ≤ σ_b`: in `cfgBA` the current version `a` (seq `σₐ`) is dominated by stale `b`.
    refine ⟨cfgBA a b, cfgBA_inj f a b hab, cfgBA_localCoherent f a b, ?_, ?_⟩
    · -- ¬ GlobalCoherent : a linear extension would need `s 0 < s 1`, i.e. `σ_b < σₐ`
      intro hG
      have hlt := hG 0 1 (by show (0 : Fin 2) < (1 : Fin 2); decide)
      rw [run_cfgBA_s0, run_cfgBA_s1] at hlt
      exact absurd hlt (not_lt.mpr hcase)
    · -- ¬ Faithful : the stale version 0 (`b`, seq `σ_b`) is not below the current one (`a`, `σₐ`)
      rw [(f.run (cfgBA a b)).faithful_iff_visibleSet (cfgBA_inj f a b hab),
          MOR.visibleSet_eq_last_iff]
      intro hall
      have hslast : (f.run (cfgBA a b)).s (Fin.last (f.run (cfgBA a b)).n) = f 0 [a] 0 := rfl
      have hlt := hall 0 (by show (0 : Fin 2) ≠ (1 : Fin 2); decide)
      rw [run_cfgBA_s0, hslast] at hlt
      exact absurd hlt (not_lt.mpr hcase)

/-- **Connection to the corrected MAIN theorem.**  Reading the impossibility through
`PrefixFaithful ↔ GlobalCoherent`: the bad configuration, being not a global linear
extension, is not per-prefix faithful either.  This is the "not-global-linear-extension ⟹
not-faithful" step routed explicitly through the main equivalence. -/
theorem local_scheme_admits_unfaithful_prefix (a b : V) (hab : a ≠ b) (f : LocalScheme V) :
    ∃ C : Config V,
      Function.Injective (f.run C).d ∧
      ¬ (f.run C).GlobalCoherent ∧
      ¬ (f.run C).PrefixFaithful := by
  obtain ⟨C, hinj, _, hng, _⟩ := local_scheme_admits_unfaithful_config a b hab f
  exact ⟨C, hinj, hng,
    fun hpf => hng (((f.run C).prefixFaithful_iff_globalCoherent hinj).mp hpf)⟩

/-! ### Non-vacuity: the class of purely-local schemes is inhabited and nontrivial

The impossibility is `∀ f, …`, so it would be vacuous if no `LocalScheme` existed.  It is
richly inhabited; here are two canonical instances, and the theorem applied to each yields a
concrete failing configuration. -/

/-- The **per-writer local counter**: writer `w` stamps its `k`-th local event with `k`.
(The natural "just use a local sequence number" scheme.) -/
def localCounter (V : Type*) : LocalScheme V := fun _ _ k => k

/-- A **per-writer static offset** scheme: writer `w` stamps every one of its events with a
fixed number `g w` decided in advance (a pre-agreed partition of the seq space by writer id —
the strongest "coordinate once, offline" scheme).  Even this fails. -/
def staticOffset (V : Type*) (g : ℕ → ℕ) : LocalScheme V := fun w _ _ => g w

/-- The local-counter scheme cannot guarantee faithfulness. -/
example : ∃ C : Config ℕ,
    ¬ (LocalScheme.run (localCounter ℕ) C).GlobalCoherent ∧
    ¬ (LocalScheme.run (localCounter ℕ) C).Faithful := by
  obtain ⟨C, _, _, hng, hnf⟩ :=
    local_scheme_admits_unfaithful_config (0 : ℕ) 1 (by decide) (localCounter ℕ)
  exact ⟨C, hng, hnf⟩

/-- Any static per-writer-offset scheme (offline seq-space partition) cannot guarantee
faithfulness either. -/
example (g : ℕ → ℕ) : ∃ C : Config ℕ,
    ¬ (LocalScheme.run (staticOffset ℕ g) C).GlobalCoherent ∧
    ¬ (LocalScheme.run (staticOffset ℕ g) C).Faithful := by
  obtain ⟨C, _, _, hng, hnf⟩ :=
    local_scheme_admits_unfaithful_config (0 : ℕ) 1 (by decide) (staticOffset ℕ g)
  exact ⟨C, hng, hnf⟩

end Mor
