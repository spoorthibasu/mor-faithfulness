import MorFaithful.GateSoundness

/-!
# Layer 0: file groups, group-relative visibility, and co-residency

`Model.lean` reconciles a key against **all** of its versions.  The audit mechanism does not: it
runs inside a bin-pack rewrite, which sees one **file group** at a time and reconciles only the
records that group happens to contain.  Two properties that measurement forced on the
implementation cannot even be *stated* in the single-key, no-groups model:

* one-sidedness of the capture is conditional on **co-residency** — every surviving version of the
  key being in the group under rewrite;
* the single-survivor guard must count survivors **globally**, not within the group.

This file adds the smallest vocabulary that makes both statable.  Nothing here modifies
`Model.lean`, `UpdatesModel.lean` or `GateSoundness.lean`; the global notions are reused verbatim.

## The distinction being introduced

`M.visibleSet` (from `Model.lean`) is visibility against the key's whole history: `i` survives iff
`s i` reaches the global max delete seq `SD`.  A rewrite group holding only *part* of the history
computes a different thing — it reconciles against the max delete seq **of its own contents** —
and that is `groupVisibleSet`.  The two coincide exactly when the group is co-resident
(`groupVisible_eq_visible_of_coResident`), and the point of the constructions in this file is that
without co-residency they come apart in a way that produces false positives.

## Ordering values

The model carries logical order as the version index: `d 0 … d n` are in logical order, so `i < j`
means version `j` is logically later, i.e. carries the higher ordering value.  A *stale win* is a
survivor that is out-ordered by a version that was discarded, which is written here as an index
comparison rather than a separate ordering column.

## The theorem list

* **T1a** soundness: under co-residency, a group-witnessed stale win is genuine.
  `groupStaleWin_genuine_of_coResident`.
* **T1b** the converse fails: a genuine stale win can be invisible to a *co-resident* group,
  because the discarded version lies outside it.  `coResident_may_miss`.
* **T2** necessity of co-residency.  `coResidency_necessary`.
* **T3** the guard on the GLOBAL visible count: *if the key has at most one globally visible
  version, a group-witnessed stale win is genuine at any group size, with no co-residency
  hypothesis.*  **This is FALSE as stated** — refuted by T2's own witness, which has exactly one
  globally visible version.  See `global_single_survivor_guard_insufficient` and the discussion
  above it for the hypothesis it actually needs.  The provable fragment is
  `not_visible_of_not_groupVisible`.
* **T3′** the same guard on the LOCAL count is false.  `local_single_survivor_guard_false`.
* **T4** abstention: with more than one group and cross-group off, recording no verdict preserves
  one-sidedness unconditionally.  **BLOCKED** — needs cover vocabulary that does not exist yet.
* **T5** the gate lift: if a group's per-sequence union intervals are non-decreasing, the group
  contains no stale win, so skipping it is sound.
* **T6** cross-group completeness: merging per-key partials across a cover restores co-residency by
  construction.  **BLOCKED** — needs cover vocabulary that does not exist yet.

T4 and T6 both quantify over a *family* of groups covering the key's versions.  Nothing here
relates one group to another: `FileGroup` is a single `Finset` with no notion of a cover, so
neither statement can be written yet.
-/

namespace Mor

open Finset

namespace MOR

variable {V : Type*} (M : MOR V)

/-! ### Layer 0 definitions -/

/-- **Layer 0.1 — a file group, seen through one key.**  The set of the key's version indices
whose data records the group contains.  A rewrite group physically holds files spanning many keys;
projected onto a single key it is exactly a set of that key's versions, which is what the model
needs.  In this single-key model the group *is* the set of the key's versions inside it, so no
separate "versions of the key in the group" operation is required — it is `G`. -/
abbrev FileGroup : Type := Finset (Fin (M.n + 1))

/-- **Layer 0.2 — the group's own max delete seq.**  The rewrite reconciles a group against the
equality deletes it can see, so the suppression threshold inside the group is the max seq over the
group's contents, not the global `SD`. -/
def groupSD (G : M.FileGroup) : ℕ := G.sup M.s

/-- **Layer 0.3 — group-relative visibility.**  The versions the group would report as surviving
if
it reconciled only what it holds.  This is the notion `Model.lean` cannot express: it is visibility
against `groupSD G` rather than against the global `SD`. -/
def groupVisibleSet (G : M.FileGroup) : Finset (Fin (M.n + 1)) :=
  G.filter (fun i => M.groupSD G ≤ M.s i)

/-- **Layer 0.4 — co-residency.**  Every globally visible version of the key lies in the group.
This is the hypothesis the mechanism's one-sidedness is conditional on, named here rather than
baked into any definition. -/
def CoResident (G : M.FileGroup) : Prop := M.visibleSet ⊆ G

/-! ### Stale wins, group-relative and genuine -/

/-- A stale win **as the group would witness it**: some version the group reports as surviving is
out-ordered by some version the group discarded. -/
def GroupStaleWin (G : M.FileGroup) : Prop :=
  ∃ i ∈ M.groupVisibleSet G, ∃ j ∈ G, j ∉ M.groupVisibleSet G ∧ i < j

/-- A **genuine** stale win: some globally surviving version is out-ordered by a version that is
globally discarded.  This is the property an audit is supposed to detect. -/
def GlobalStaleWin : Prop :=
  ∃ i ∈ M.visibleSet, ∃ j, j ∉ M.visibleSet ∧ i < j

/-! ### Adequacy of the definitions

The definitions are only worth anything if group-relative and global visibility genuinely can
differ, and if they provably agree in the case the implementation relies on.  Both are recorded
here, before any of the numbered theorems. -/

/-- `SD` is attained: some version carries the global max delete seq, so the globally visible set
is never empty. -/
theorem visibleSet_nonempty : M.visibleSet.Nonempty := by
  obtain ⟨i, -, hi⟩ := Finset.exists_mem_eq_sup univ univ_nonempty M.s
  exact ⟨i, (M.mem_visibleSet i).2 hi.symm⟩

/-- Under co-residency the group's threshold is the global one. -/
theorem groupSD_eq_SD_of_coResident {G : M.FileGroup} (h : M.CoResident G) :
    M.groupSD G = M.SD := by
  obtain ⟨i, hi⟩ := M.visibleSet_nonempty
  refine le_antisymm (Finset.sup_le fun j _ => M.le_SD j) ?_
  calc M.SD = M.s i := ((M.mem_visibleSet i).1 hi).symm
    _ ≤ G.sup M.s := Finset.le_sup (h hi)

/-- **Adequacy, positive half.**  When the group is co-resident, reconciling the group's contents
gives exactly the globally correct answer.  This is why the mechanism is sound at all. -/
theorem groupVisible_eq_visible_of_coResident {G : M.FileGroup} (h : M.CoResident G) :
    M.groupVisibleSet G = M.visibleSet := by
  ext i
  rw [groupVisibleSet, Finset.mem_filter, M.groupSD_eq_SD_of_coResident h, M.mem_visibleSet]
  constructor
  · intro hmem
    exact le_antisymm (M.le_SD i) hmem.2
  · intro he
    exact ⟨h ((M.mem_visibleSet i).2 he), he.ge⟩

/-! ### T1a — soundness under co-residency -/

/-- **T1a.**  Under co-residency a stale win witnessed inside the group is a genuine one.  This is
the half of one-sidedness the mechanism relies on: it never reports a violation that is not there,
*provided* every surviving version of the key is in the group being rewritten. -/
theorem groupStaleWin_genuine_of_coResident {G : M.FileGroup} (h : M.CoResident G)
    (hsw : M.GroupStaleWin G) : M.GlobalStaleWin := by
  obtain ⟨i, hi, j, -, hj, hij⟩ := hsw
  rw [M.groupVisible_eq_visible_of_coResident h] at hi hj
  exact ⟨i, hi, j, hj, hij⟩

/-- A version the group discards is globally discarded too, with **no hypothesis at all** — not
co-residency, not a survivor count.  This is the group-relative echo of
`discarded_seq_lt_visible_seq`: the group's threshold is below the global one, so anything failing
to reach it fails globally.

It is also the entire provable fragment of T3 (see below): the group is always right about which
versions are *discarded*, and it is the identity of the *survivor* that needs co-residency. -/
theorem not_visible_of_not_groupVisible {G : M.FileGroup} {i j : Fin (M.n + 1)}
    (hi : i ∈ M.groupVisibleSet G) (hjG : j ∈ G) (hj : j ∉ M.groupVisibleSet G) :
    j ∉ M.visibleSet := by
  rw [groupVisibleSet, Finset.mem_filter] at hi hj
  have hnot : ¬ (M.groupSD G ≤ M.s j) := fun hle => hj ⟨hjG, hle⟩
  have hlt : M.s j < M.s i := lt_of_lt_of_le (not_le.1 hnot) hi.2
  rw [M.mem_visibleSet]
  exact fun hEq => absurd (hEq ▸ M.le_SD i) (not_le.2 (hEq ▸ hlt))

end MOR

/-! ### T2 — one-sidedness is conditional on co-residency

The witness.  Four versions with seqs `s = [1, 5, 3, 9]`.  Globally the last version carries the
max seq, so it alone survives and it out-orders everything discarded: there is no genuine stale
win.  The group `{1, 2}` omits that survivor.  Reconciled on its own contents the group's threshold
is `max(5, 3) = 5`, so it reports version `1` as surviving and version `2` as discarded — and `2`
is
logically later than `1`.  The group therefore witnesses a stale win that does not exist. -/

/-- Four versions, seqs `[1, 5, 3, 9]`. -/
def Mfp : MOR ℕ := ⟨3, ![0, 1, 2, 3], ![1, 5, 3, 9]⟩

/-- The group holding versions `1` and `2` — and not the global survivor `3`. -/
def Gfp : Mfp.FileGroup := {1, 2}

theorem Mfp_SD : Mfp.SD = 9 := by decide

theorem Mfp_visibleSet : Mfp.visibleSet = {3} := by decide

theorem Mfp_groupSD : Mfp.groupSD Gfp = 5 := by decide

theorem Mfp_groupVisibleSet : Mfp.groupVisibleSet Gfp = {1} := by decide

/-- The group is not co-resident: the only globally surviving version, `3`, is outside it. -/
theorem Mfp_not_coResident : ¬ Mfp.CoResident Gfp := by
  unfold MOR.CoResident; decide

/-- The group witnesses a stale win: it reports `1` as surviving while discarding the logically
later `2`. -/
theorem Mfp_groupStaleWin : Mfp.GroupStaleWin Gfp := by
  unfold MOR.GroupStaleWin; decide

/-- There is no genuine stale win: the sole global survivor out-orders every discarded version. -/
theorem Mfp_not_globalStaleWin : ¬ Mfp.GlobalStaleWin := by
  unfold MOR.GlobalStaleWin; decide

/-- **T2 (necessity of co-residency).**  Without co-residency the group-relative verdict is not
one-sided: there is a model and a group that is not co-resident, in which the group witnesses a
stale win although the key has none.  The false positive is constructible. -/
theorem coResidency_necessary :
    ∃ (M : MOR ℕ) (G : M.FileGroup),
      ¬ M.CoResident G ∧ M.GroupStaleWin G ∧ ¬ M.GlobalStaleWin :=
  ⟨Mfp, Gfp, Mfp_not_coResident, Mfp_groupStaleWin, Mfp_not_globalStaleWin⟩

/-! ### T1b — co-residency does not make the capture complete

T1a says a co-resident group never reports a violation that is not there.  It does **not** say a
co-resident group sees every violation that is there, and this is the witness that it does not.

Three versions with seqs `[9, 1, 2]`.  The first version alone reaches the max seq, so it is the
only survivor and the two logically later versions were both discarded: a genuine stale win.  The
group `{0}` contains every surviving version, so it is co-resident — but it contains no discarded
version at all, so reconciling it produces one survivor, nothing suppressed, and no verdict.

The asymmetry is structural rather than accidental: `CoResident` constrains where the *visible*
versions live, and says nothing about the discarded one that makes a stale win a stale win. -/

/-- Three versions, seqs `[9, 1, 2]`: only version `0` survives, and it is out-ordered by both
versions that did not. -/
def Mmiss : MOR ℕ := ⟨2, ![0, 1, 2], ![9, 1, 2]⟩

/-- The group holding exactly the surviving version — co-resident, and blind. -/
def Gmiss : Mmiss.FileGroup := {0}

theorem Mmiss_visibleSet : Mmiss.visibleSet = {0} := by decide

theorem Mmiss_groupVisibleSet : Mmiss.groupVisibleSet Gmiss = {0} := by decide

/-- Every globally surviving version is in the group. -/
theorem Mmiss_coResident : Mmiss.CoResident Gmiss := by
  unfold MOR.CoResident; decide

/-- There is a genuine stale win: the survivor `0` is out-ordered by the discarded `1`. -/
theorem Mmiss_globalStaleWin : Mmiss.GlobalStaleWin := by
  unfold MOR.GlobalStaleWin; decide

/-- The group cannot witness it: it holds no discarded version. -/
theorem Mmiss_not_groupStaleWin : ¬ Mmiss.GroupStaleWin Gmiss := by
  unfold MOR.GroupStaleWin; decide

/-- **T1b (co-residency is not completeness).**  There is a co-resident group and a genuine stale
win that the group does not witness.  Together with T1a this makes one-sidedness a characterisation
rather than a claim: under co-residency the mechanism's verdicts are sound and may be incomplete,
and the incompleteness is a proved fact, not a caveat. -/
theorem coResident_may_miss :
    ∃ (M : MOR ℕ) (G : M.FileGroup),
      M.CoResident G ∧ M.GlobalStaleWin ∧ ¬ M.GroupStaleWin G :=
  ⟨Mmiss, Gmiss, Mmiss_coResident, Mmiss_globalStaleWin, Mmiss_not_groupStaleWin⟩

/-! ### T3′ — the single-survivor guard, as it was written

The guard exists to exclude the same-sequence duplicate shape (FLINK-38450), where two versions
share the maximum seq and are therefore *both* visible: that is a duplicate, not a stale win.  The
implementation tested the survivor count **within the group**.  The statement below says that test
is enough, and it is false.

The witness is the duplicate shape itself: two versions at the same seq `7`, so both are globally
visible, inside a group that holds only one of them.  Locally the key has exactly one survivor and
the guard passes; globally it has two and the key should have been excluded.  This is the shape
behind a run reporting 400,000 violations on a table containing 380,000. -/

/-- Two versions sharing seq `7`: the duplicate shape, both globally visible. -/
def Mdup : MOR ℕ := ⟨1, ![0, 1], ![7, 7]⟩

/-- A group holding only one of the two co-visible versions. -/
def Gdup : Mdup.FileGroup := {0}

theorem Mdup_visibleSet : Mdup.visibleSet = {0, 1} := by decide

theorem Mdup_groupVisibleSet : Mdup.groupVisibleSet Gdup = {0} := by decide

/-- Within the group the key has exactly one surviving version. -/
theorem Mdup_local_card : (Mdup.groupVisibleSet Gdup).card = 1 := by decide

/-- Globally it has two: the guard's premise is satisfied locally and false globally. -/
theorem Mdup_global_card : Mdup.visibleSet.card = 2 := by decide

/-- **T3′ (the guard as it was, refuted).**  Counting the key's surviving versions inside the
group
does not establish that the key has one survivor.  The local single-survivor guard is unsound. -/
theorem local_single_survivor_guard_false :
    ¬ (∀ (M : MOR ℕ) (G : M.FileGroup),
        (M.groupVisibleSet G).card = 1 → M.visibleSet.card = 1) := by
  intro h
  have := h Mdup Gdup Mdup_local_card
  rw [Mdup_global_card] at this
  exact absurd this (by decide)

/-- The same fact in positive form: a group can report exactly one survivor for a key that has two,
which is the duplicate shape the guard was meant to exclude. -/
theorem local_guard_admits_duplicate :
    ∃ (M : MOR ℕ) (G : M.FileGroup),
      (M.groupVisibleSet G).card = 1 ∧ 2 ≤ M.visibleSet.card :=
  ⟨Mdup, Gdup, Mdup_local_card, Mdup_global_card.ge⟩

/-! ### T3 — the guard on the GLOBAL count, and why it is not the thing that makes a verdict sound

T3 was to say: *if the key has at most one globally visible version, then a group-witnessed stale
win is genuine, for any `G`, with no co-residency hypothesis.*  **That is false**, and the
counterexample is T2's own witness, which needs no new construction: `Mfp` has exactly one globally
visible version (`visibleSet = {3}`), the group `Gfp` witnesses a stale win, and there is no
genuine one.

Why it fails is worth stating exactly, because it locates what the guard does and does not do.
Given `GroupStaleWin G` with witnesses `i` (the group's survivor) and `j` (the group's discarded
version), `not_visible_of_not_groupVisible` already gives `j ∉ visibleSet` unconditionally: the
group is always right that `j` lost.  What is missing is a *globally* visible version out-ordered
by `j`.  The group offers `i`, but `i` need not be globally visible — in `Mfp` it is not.
Bounding
the global survivor count says how many survivors exist, not where they are relative to the group,
so it cannot supply the missing fact.  The hypothesis that does is `i ∈ visibleSet`, which is what
co-residency delivers (`groupVisible_eq_visible_of_coResident`), and with it the count bound is not
needed at all — that is T1a.

**What T3 and T3′ together establish.**  The guard's correctness depends on which count it is
evaluated against, and neither reading rescues soundness on its own:

* on the **local** count (T3′) the guard is unsound in the sharpest way — it admits exactly the
  case
  it exists to exclude.  A group holding one of two co-visible versions reports a single survivor,
  the guard passes, and a same-sequence duplicate is classified as a stale win.  That is the shape
  behind a run reporting 400,000 violations on a table containing 380,000;
* on the **global** count (T3) the guard is sound *about duplicates* — two co-visible versions are
  never a stale win — but it is not sufficient for genuineness, as `Mfp` shows.

So the guard is a duplicate filter, not a soundness argument.  Soundness comes from co-residency
(T1a), and the guard's job is only to keep the FLINK-38450 shape out of the reported class. -/

theorem Mfp_visibleSet_card : Mfp.visibleSet.card = 1 := by decide

/-- **T3, refuted.**  A bound on the *global* survivor count does not make a group-witnessed stale
win genuine.  Witness: `Mfp` from T2, which satisfies the hypothesis. -/
theorem global_single_survivor_guard_insufficient :
    ¬ (∀ (M : MOR ℕ) (G : M.FileGroup),
        M.visibleSet.card ≤ 1 → M.GroupStaleWin G → M.GlobalStaleWin) := by
  intro h
  exact Mfp_not_globalStaleWin (h Mfp Gfp Mfp_visibleSet_card.le Mfp_groupStaleWin)

/-- The hypothesis T3 actually needs, stated with nothing extra: the group's reported survivor must
itself be globally visible.  No count bound and no co-residency — those are two different ways of
supplying this one fact, and co-residency is the one the implementation can check. -/
theorem groupStaleWin_genuine_of_survivor_visible {M : MOR ℕ} {G : M.FileGroup}
    {i j : Fin (M.n + 1)} (hi : i ∈ M.groupVisibleSet G) (hiv : i ∈ M.visibleSet)
    (hjG : j ∈ G) (hj : j ∉ M.groupVisibleSet G) (hij : i < j) : M.GlobalStaleWin :=
  ⟨i, hiv, j, M.not_visible_of_not_groupVisible hi hjG hj, hij⟩

end Mor
