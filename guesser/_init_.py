"""guesser - classical (no ML) calibrated digit guesser for the donkey-finisher bot.

Layers: experts (incremental count-table hypotheses) -> mixture (log-space Bayesian model averaging with an explicit
uniform null + anytime-valid e-process) -> evidence (tiers, e-BH) -> calibrate (prequential set-probability calibrator)
-> policy (pure functions: contract menu, Kelly, bold play, cadence).  Nothing here does I/O; engine.py is the glue.
Everything is behind config.GUESSER_MODE (default 0).
"""
