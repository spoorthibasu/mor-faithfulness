import MorFaithful.Global
import MorFaithful.UpdatesModel
import MorFaithful.LocalImpossible
import MorFaithful.GateSoundness

/-!
Axiom audit.  Each `#print axioms` must show only the standard mathlib axioms
(`propext`, `Classical.choice`, `Quot.sound`) and NOT `sorryAx`.
-/

namespace Mor

-- MAIN, ⟸ direction (final-state Faithful)
#print axioms MOR.faithful_of_linear
-- MAIN, ⟹ direction FAILS (machine-checked counterexample)
#print axioms main_necessity_fails
-- Corrected MAIN: PrefixFaithful ↔ LinearExtension
#print axioms MOR.prefixFaithful_iff_linear
-- COR1
#print axioms cor1_single_writer
-- COR2 (FLINK-38450)
#print axioms cor2_not_faithful
#print axioms cor2_card
-- COR3
#print axioms cor3_compaction
-- Global coherence
#print axioms MOR.prefixFaithful_iff_globalCoherent
#print axioms local_coherence_insufficient
-- Claim B: impossibility of purely-local ordering
#print axioms local_scheme_admits_unfaithful_config
#print axioms local_scheme_admits_unfaithful_prefix
-- A-del-all reduction (all-versions ≡ updates-only)
#print axioms MOR.faithful_iff_faithful'
#print axioms MOR.prefixFaithful_iff_prefixFaithful'
#print axioms MOR.faithful'_of_linear
#print axioms del_reduction_needs_inj
-- Sequence separation (licenses the metadata gate to ignore same-sequence file pairs)
#print axioms MOR.discarded_seq_lt_visible_seq
#print axioms MOR.staleWin_distinct_seq
#print axioms MOR.same_seq_both_visible
#print axioms MOR.discarded_seq_lt_visible_seq'
#print axioms MOR.staleWin_distinct_seq'
#print axioms MOR.same_seq_both_visible'

end Mor
