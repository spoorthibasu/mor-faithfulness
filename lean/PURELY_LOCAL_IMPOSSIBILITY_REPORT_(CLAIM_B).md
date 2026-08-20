# Claim B: impossibility of purely-local ordering (coordination is necessary)

Machine-checked in `MorFaithful/LocalImpossible.lean`. Wired into `MorFaithful.lean` and
`MorFaithful/AxiomCheck.lean`. Full `lake build` is green; sorry-free; same axiom profile as
every other result.

**Result: it holds as stated.** No purely-local ordering scheme can guarantee global
faithfulness.

## 1. The precise definition of "purely-local" (the crux)

A multi-writer configuration for one key (`Config`) is the existing `MOR` changelog data plus
a producer tag on each version:

```lean
structure Config (V) where
  n : ℕ
  d : Fin (n+1) → V      -- versions in global logical order (d n current)
  w : Fin (n+1) → ℕ      -- writer id of each version  (= the `p` of Global.lean)
```

Writer `x`'s only observable local data:

* `localView C x` = the list of values writer `x` itself wrote, in its own order (a
  `filterMap` over the versions, keeping those with `w i = x`);
* `localPos C i` = the 0-based index of version `i` within its own writer's stream.

A **purely-local ordering scheme** is a single function

```lean
def LocalScheme (V) := ℕ → List V → ℕ → ℕ   -- (writer id) (its local value-list) (local index) → seq
def LocalScheme.run f C : MOR V :=
  { n := C.n, d := C.d, s := fun i => f (C.w i) (C.localView (C.w i)) (C.localPos i) }
```

Locality is enforced by the domain of `f`. Its only inputs are the writer's own id, the
writer's own list of values, and the local index. There is deliberately no argument carrying
another writer's events, the global interleaving of the streams, or a shared counter.
Quantifying over all `f` ranges over every scheme built from per-writer-local information
alone (local counters, per-writer static offsets, hashes of the local payload, unsynchronized
local clocks), and excludes exactly the schemes that need cross-writer agreement or a global
sequencer.

Why this is the right definition and not a straw man:

* **Maximally-powerful local definition.** `f` receives the writer's entire local value-list
  (not just the prefix up to `localPos`), so it may even peek at its own future local events.
  Any real local scheme has less information, so the impossibility is only stronger.
* **Permits per-writer-id dependence** (`w` is an argument), so it even covers "coordinate
  once, offline" schemes that statically partition the seq space by writer id. Those fail too.
* **`localView`/`localPos` are projections, not leakage.** The config `C` is ground-truth
  global reality; `f`'s access to it is mediated entirely through the per-writer projection.
  That obliviousness is machine-checked, not assumed: for the two witness configs,
  `run_cfgAB_s0` and `run_cfgBA_s1` both reduce to `f 0 [a] 0` by `rfl`. Same scheme, two
  different global interleavings, identical output, because the local projections coincide.
* **Non-vacuous.** The type is richly inhabited; `localCounter` and `staticOffset` are
  defined, and the theorem is instantiated on each.

## 2. The theorem statement (proved)

```lean
theorem local_scheme_admits_unfaithful_config
    {V} [DecidableEq V] (a b : V) (hab : a ≠ b) (f : LocalScheme V) :
    ∃ C : Config V,
      Function.Injective (f.run C).d ∧      -- distinct values, so (A-inj) of the main theorem holds
      (f.run C).LocalCoherent C.w ∧          -- every writer is even individually monotone
      ¬ (f.run C).GlobalCoherent ∧           -- NOT a global linear extension of logical order
      ¬ (f.run C).Faithful                   -- and NOT faithful (final-state, def 6)
```

`LocalCoherent` is in the conjunction on purpose: it strictly strengthens the existing
`local_coherence_insufficient`. The failure is genuinely about missing cross-writer
coordination, not any writer misbehaving locally.

Connection to the main theorem (the "not-linear-extension therefore not-faithful" step, routed
explicitly through `PrefixFaithful ↔ GlobalCoherent`):

```lean
theorem local_scheme_admits_unfaithful_prefix ... :
    ∃ C, Function.Injective (f.run C).d ∧ ¬ (f.run C).GlobalCoherent ∧ ¬ (f.run C).PrefixFaithful
-- proof: ¬GlobalCoherent ⟹ ¬PrefixFaithful via (f.run C).prefixFaithful_iff_globalCoherent
```

The main theorem's `⟹` direction holds for `PrefixFaithful`, not final-state `Faithful` (that
gap is `main_necessity_fails`). So the honest chain is bad-config `¬GlobalCoherent` therefore
`¬PrefixFaithful`. The witness additionally happens to be unfaithful at the final state
(`¬Faithful`), which is strictly stronger, so both are reported.

## 3. The counterexample-witness (obliviousness)

Two writers `0`, `1` writing distinct values `a`, `b`, and the two locally-indistinguishable
configs:

| config  | logical order | writers | current | `s` from `f`  |
|---------|---------------|---------|---------|---------------|
| `cfgAB` | `a, b`        | `0, 1`  | `b`     | `![σₐ, σ_b]`  |
| `cfgBA` | `b, a`        | `1, 0`  | `a`     | `![σ_b, σₐ]`  |

with `σₐ := f 0 [a] 0` and `σ_b := f 1 [b] 0`. In both configs writer `0` sees `[a]` at
position `0` and writer `1` sees `[b]` at position `0`, so `f` produces the same two numbers;
it cannot see which came first globally. Faithfulness of `cfgAB` needs `σₐ < σ_b`;
faithfulness of `cfgBA` needs `σ_b < σₐ`. Contradictory, so the proof case-splits on
`Nat.lt_or_ge σ_b σₐ` and returns whichever config is bad. The values carry no cross-writer
ordering info: value `a` sits at global position 0 in `cfgAB` and position 1 in `cfgBA`. The
equal-seq subcase (`σₐ = σ_b`) is also covered: both versions become visible (a COR2-style
equal-seq failure), still `¬Faithful`.

## 4. Axiom audit

`local_scheme_admits_unfaithful_config` and `local_scheme_admits_unfaithful_prefix` each:

```
depends on axioms: [propext, Classical.choice, Quot.sound]
```

Identical to `local_coherence_insufficient` and every main result. No `sorryAx`, no
`ofReduceBool` (no `native_decide`), no new axioms. Sorry-free, zero warnings in the new file,
full build green. Audit lines are in `AxiomCheck.lean`.

## Summary

Claim B is true and machine-checked at the same rigor as the rest of the development.
"Purely-local" is defined as a scheme whose domain is exactly per-writer-local data, made
deliberately maximally powerful (full local history, per-writer-id dependence) so the
impossibility is as strong as possible, and shown non-vacuous by inhabiting instances. The
result generalizes the single-witness `local_coherence_insufficient` into a universal
impossibility over all such schemes, and connects to faithfulness through the corrected main
theorem.
