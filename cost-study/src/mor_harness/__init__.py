"""mor_harness: a CDC-to-lakehouse workload harness.

One measurement instrument, two studies:
  * sensitivity study   — violation rate vs controlled ordering imperfections
  * enforcement-cost study — storage-engine cost of enforcing safe ordering

See DESIGN.md. The instrument is the runner (`runner.run`) + per-format adapters;
both studies are sweeps over the same runner producing the same run-record schema.
"""

__version__ = "0.1.0"
