-- MOR changelog-materialization faithfulness: machine-checked theory.
import MorFaithful.Zset          -- defs 1–2 (Z-set, distinct)
import MorFaithful.Model         -- defs 3–7 (changelog, layout, visible, Faithful, LinearExtension)
import MorFaithful.Main          -- MAIN ⟸ ; ⟹ counterexample (final-state Faithful)
import MorFaithful.MainPrefix    -- corrected MAIN: PrefixFaithful ↔ LinearExtension
import MorFaithful.Corollaries   -- COR1, COR2 (FLINK-38450), COR3
import MorFaithful.Global        -- multi-producer global-coherence theorem
import MorFaithful.UpdatesModel  -- A-del-all reduction: all-versions ≡ updates-only delete model
import MorFaithful.LocalImpossible -- Claim B: impossibility of purely-local ordering (coordination necessary)
