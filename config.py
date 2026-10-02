import os

# ── GENERAL ───────────────────────────────────────────────────
LOG_LEVEL = "INFO"
DEBUG     = False
VERSION   = "1.1.0"

# ── DERIV API ─────────────────────────────────────────────────
DERIV_API_TOKEN   = os.environ.get("DERIV_API_TOKEN", "")
DERIV_APP_ID      = os.environ.get("DERIV_APP_ID", "1089")
# "real" or "demo" — picks which Options account _fetch_otp_ws_url() mints
# an OTP for (deriv_client.py). Was previously never read from env, so it
# silently defaulted to "demo" no matter what was set on Render.
DERIV_ACCOUNT_MODE = os.environ.get("DERIV_ACCOUNT_MODE", "demo").strip().lower()

# ── SERVER ────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 10000))
SELF_URL = os.environ.get("SELF_URL", os.environ.get("RENDER_EXTERNAL_URL", ""))
KEEP_ALIVE_INTERVAL = 600  # seconds between self-ping requests

# ── ALL DERIV SYNTHETIC INDICES ──────────────────────────────

# Standard Volatility (2s tick)
VOLATILITY_STANDARD = [
    "R_10","R_25","R_50","R_75","R_100",
]

# 1-Second Volatility (faster tick)
# 1HZ150V / 1HZ200V / 1HZ250V removed — confirmed OfferingsInvalidSymbol
# by a real contracts_for audit (symbol_audit.py, 2026-07-31). These were
# added in an earlier pass "by pattern" (same family as 1HZ10V-100V, and
# already had MULTIPLIER_MAP/STOP_LOSS_MAP entries) without empirical
# verification, and were silently failing every buy attempt — this was a
# live bug in the currently-active mean_reversion strategy (they were
# already in RISE_FALL_SYMBOLS / MEAN_REVERSION_SYMBOLS), not a dormant
# one. Do not re-add without a fresh audit confirming they exist on this
# account.
VOLATILITY_1S = [
    "1HZ10V","1HZ25V","1HZ50V",
    "1HZ75V","1HZ100V",
]

# Boom & Crash
# ACTIVATED: BOOM500/BOOM1000/CRASH500/CRASH1000 confirmed MULTUP/MULTDOWN
# by the 2026-07-31 audit (x100-400 / x100-500 — see MULTIPLIER_MAP), all
# four already sit in MULTIPLIER_SYMBOLS, and buy_multiplier() exists in
# deriv_client.py. That was everything needed to trade them — this was the
# last deliberate switch (previously left empty on purpose, see git
# history / prior comment). Routes to evaluate_boom_crash() via
# BOOM_CRASH_SYMBOLS below, dispatched to buy_multiplier() by bot_engine.py.
# CALL/PUT support for these four was never re-tested post the
# currency-param fix — doesn't matter here since they only trade via
# Multipliers, but don't assume Rise/Fall works for them without a fresh
# check if that path is ever wanted.
# NOT included: BOOM300N/CRASH300N (OfferingsInvalidSymbol — likely a
# naming bug, config's un-suffixed BOOM300/CRASH300 was never queried) and
# BOOM150/CRASH150 (never queried at all). Re-run the audit against the
# un-suffixed codes before adding any of the four.
BOOM_CRASH = ["BOOM500", "BOOM1000", "CRASH500", "CRASH1000"]

# Step Index
STEP = ["stpRNG"]

# Jump Indices
# Old note here claimed Jump indices don't support CALL/PUT
# (OfferingsValidationError) — a real contracts_for audit (symbol_audit.py,
# 2026-07-31) contradicts that: JD10-JD100 all confirmed to support
# CALL/PUT Rise/Fall, plus Multipliers and digit contracts. The earlier
# failure was most likely the currency-param bug in contracts_for() that
# was fixed alongside this audit run, not an actual product limitation —
# treat the old "OfferingsValidationError" conclusion as stale.
# Confirmed Multiplier ranges (for the later buy_multiplier() wiring pass —
# NOT used yet; MULTIPLIER_SYMBOLS below intentionally doesn't include
# these until buy_multiplier() exists in deriv_client.py):
#   JD10: x100-x1000   JD25: x50-x500   JD50: x20-x200
#   JD75: x15-x150     JD100: x10-x100
JUMP = ["JD10", "JD25", "JD50", "JD75", "JD100"]

# Range Break
# There is no confirmed true Range Break product on this account. RDBULL/
# RDBEAR were previously (wrongly) filed under this category with a
# "MT5-only, not reachable" note — a real contracts_for audit (2026-07-31)
# disproves that (see the Bear/Bull section below, where they've been
# moved). RANGE_BREAK stays empty/disabled unless a genuine Range Break
# symbol code is ever confirmed for this account.
RANGE_BREAK = []
RANGE_BREAK_ENABLED = False

# Drift Switch
DRIFT = ["DSHIFT10","DSHIFT20","DSHIFT30"]

# ── BEAR/BULL ("DAILY RESET") MARKET SYMBOLS ─────────────────
# Confirmed via a real contracts_for audit (symbol_audit.py, 2026-07-31):
# RDBEAR and RDBULL both offer CALL/PUT, Touch/No Touch, digit contracts,
# and Range/Up-or-Down — no MULTUP/MULTDOWN. So they're reachable via this
# bot's existing Rise/Fall (CALL/PUT) path right now, with no dependency
# on buy_multiplier() being built. Routed to evaluate_trend_shift() via
# BEAR_BULL_SYMBOLS below (see signal_engine.py's dispatcher) and also
# added to RISE_FALL_SYMBOLS so they're actually initialised/scanned
# (ALL_TRADE_SYMBOLS is derived from RISE_FALL_SYMBOLS — see note below).
BEAR_BULL_SYMBOLS = ["RDBEAR", "RDBULL"]
BEAR_BULL_TREND_SHIFT_MINS = 20     # 10 / 20 / 30 — unchanged default

# Implementation Brief v3, finding #4: each Daily Reset index holds ONE
# fixed characteristic trend for its entire 24h cycle (Bull always up,
# Bear always down, per Deriv's own product description) — this is a
# static fact, not something signal_engine.py should derive from EMAs or
# alternate at each reset. evaluate_trend_shift() reads this map directly;
# BEAR_BULL_TREND_SHIFT_MINS above is used only to gate entry timing
# (skip trading until the post-reset window closes), never to pick a side.
BEAR_BULL_DIRECTION = {"RDBULL": "LONG", "RDBEAR": "SHORT"}

# Symbols confirmed via contracts_for to support CALL/PUT Rise/Fall on
# this account. Last empirically verified: symbol_audit.py run, 2026-07-31.
#   - 1HZ150V/1HZ200V/1HZ250V removed — confirmed OfferingsInvalidSymbol,
#     see VOLATILITY_1S note above.
#   - JD10/JD25/JD50/JD75/JD100 added — confirmed CALL/PUT support; also
#     routed to JUMP_BUILDUP_SYMBOLS below for evaluate_jump_buildup().
#   - RDBEAR/RDBULL added — confirmed CALL/PUT support (previously wrongly
#     assumed MT5-only); also routed to BEAR_BULL_SYMBOLS below for
#     evaluate_trend_shift().
# Still NOT verified / not added: BOOM300N, CRASH300N (came back
# OfferingsInvalidSymbol — likely a naming bug, config.py's MULTIPLIER_MAP
# uses "BOOM300"/"CRASH300" with no "N" suffix; re-run the audit against
# the un-suffixed names before adding), DSHIFT10/20/30 (not in symbols.py's
# SYNTHETIC list at all, so never queried — add them there first).
RISE_FALL_SYMBOLS = [
    "R_10","R_25","R_50","R_75","R_100",
    "1HZ10V","1HZ25V","1HZ50V","1HZ75V","1HZ100V",
    "stpRNG",
    "JD10","JD25","JD50","JD75","JD100",
    "RDBEAR","RDBULL",
]

# Digit (Match/Differ/Over/Under/Even/Odd) — evaluate_digit() is built and
# waiting for symbols, but stays empty this pass. Two separate reasons:
#   1. The 2026-07-31 audit only confirmed digit-contract support for
#      JD10-JD100 and RDBEAR/RDBULL (bundled under "digit contracts" in the
#      contracts_for results) — it never tested R_10-R_100, 1HZ10V-100V, or
#      stpRNG, which are the symbols you'd actually want a digit strategy
#      on. Don't add those on a guess; re-run the audit against them
#      specifically.
#   2. Even the confirmed ones (JD*, RDBEAR/RDBULL) can't go here anyway
#      without a decision: they're already committed to JUMP_BUILDUP_SYMBOLS
#      and BEAR_BULL_SYMBOLS respectively, and "every traded symbol routes
#      to exactly one strategy evaluator" (see STRATEGY ROUTING below) — so
#      adding them to DIGIT_SYMBOLS too would double-route them. That's a
#      strategy call, not a data-confirmation one; get explicit sign-off
#      before reassigning a symbol off its current strategy.
# signal_engine.py checks `if symbol in config.DIGIT_SYMBOLS`; empty means
# that branch is always skipped.
DIGIT_SYMBOLS = []

# ── STRATEGY ROUTING (signal_engine.py) ──────────────────────
# Every traded symbol is routed to exactly one strategy evaluator.
MEAN_REVERSION_SYMBOLS = VOLATILITY_STANDARD + VOLATILITY_1S  # all 7 vol indices
RANGE_BREAK_SYMBOLS    = []                                      # disabled — no genuine Range Break
                                                                   # symbol has ever been confirmed on
                                                                   # this account (RDBEAR/RDBULL are
                                                                   # Bear/Bull, not Range Break — see
                                                                   # RANGE_BREAK note above)
BOOM_CRASH_SYMBOLS     = BOOM_CRASH                              # BOOM500/BOOM1000/CRASH500/CRASH1000
                                                                   # — ACTIVATED this pass, see
                                                                   # BOOM_CRASH note above
STEP_SYMBOLS           = STEP                                    # stpRNG
JUMP_BUILDUP_SYMBOLS   = JUMP                                    # JD10-JD100 — see JUMP note above
JUMP_SYMBOLS            = JUMP                                    # alias — symbol_manager.py's
                                                                   # is_in_session() reads this name
DIGIT_PARITY_SYMBOLS   = []                                      # evaluate_digit_parity() built, no
                                                                   # symbols wired — same two blockers
                                                                   # as DIGIT_SYMBOLS above (audit
                                                                   # never tested the actually-free
                                                                   # candidates for digit contracts;
                                                                   # the confirmed ones are already
                                                                   # claimed by other strategies)
DRIFT_FADE_SYMBOLS     = []                                      # evaluate_drift_fade() built, no
                                                                   # symbols wired — DSHIFT10/20/30
                                                                   # aren't in symbols.py's SYNTHETIC
                                                                   # list, so the 2026-07-31 audit
                                                                   # never queried them at all. Add
                                                                   # them there, re-run the audit,
                                                                   # then populate this.
# BEAR_BULL_SYMBOLS is defined earlier, in its own section above (RDBEAR/
# RDBULL — routed via Rise/Fall, NOT Multipliers; audit confirmed no
# MULTUP/MULTDOWN support on either, so despite this task's original
# assumption that Bear/Bull strategies only reach symbols via
# buy_multiplier(), these two only work through the CALL/PUT path — leave
# them exactly as already wired above).

# bot_engine.py's _execute() checks `if symbol in config.MULTIPLIER_SYMBOLS`
# to decide whether a symbol routes to buy_multiplier() instead of
# buy_contract(). buy_multiplier() now exists in deriv_client.py and is
# callable. Populated here ONLY with symbols that have a real, confirmed
# contracts_for audit result AND no conflicting existing route:
#   BOOM500/BOOM1000/CRASH500/CRASH1000 — confirmed MULTUP/MULTDOWN support
#   (x100-400 / x100-500 respectively) by the 2026-07-31 audit, not traded
#   any other way (BOOM_CRASH_SYMBOLS is now populated too — see STRATEGY
#   ROUTING above — so these are fully wired end-to-end: MULTIPLIER_SYMBOLS
#   routes them to buy_multiplier(), BOOM_CRASH_SYMBOLS gets them into
#   ALL_TRADE_SYMBOLS and evaluate_boom_crash()).
#
# Deliberately left OUT, each for a different reason — do not add without
# resolving the specific blocker noted:
#   JD10/JD25/JD50/JD75/JD100 — audit confirmed these DO support
#     Multipliers, but they're also confirmed for CALL/PUT and are
#     ALREADY live via RISE_FALL_SYMBOLS today. Adding them here would
#     silently reroute them off a currently-working Rise/Fall path onto
#     Multipliers (_execute()'s branch is if/else, not both) — that's a
#     strategy decision, not just a data-confirmation one. Get explicit
#     sign-off before switching.
#   DSHIFT10/DSHIFT20/DSHIFT30 — never audited at all (not in symbols.py's
#     SYNTHETIC list, so symbol_audit.py never queried them). The values
#     already sitting in MULTIPLIER_MAP for these predate any real check.
#     Add DSHIFT10/20/30 to symbols.py, re-run the audit, then reconsider.
#   BOOM300/CRASH300/BOOM150/CRASH150 (un-suffixed) — audit only tested
#     BOOM300N/CRASH300N (OfferingsInvalidSymbol — believed to be a naming
#     bug) and never queried BOOM150/CRASH150 at all. Re-run the audit
#     against the correct un-suffixed codes before adding.
#   RDBEAR/RDBULL — audit explicitly confirmed NO MULTUP/MULTDOWN support
#     on either. Will never belong here; they trade via Rise/Fall only.
#
MULTIPLIER_SYMBOLS = ["BOOM500", "BOOM1000", "CRASH500", "CRASH1000"]

# bot_engine._init_all_symbols() reads ALL_TRADE_SYMBOLS (falling back to
# ALL_SYMBOLS) as the ONLY list of symbols that ever get initialised or
# scanned — a symbol missing from here never runs through ANY strategy,
# regardless of being listed in JUMP_BUILDUP_SYMBOLS / BEAR_BULL_SYMBOLS /
# etc. This used to just alias RISE_FALL_SYMBOLS, which silently meant
# nothing outside plain Rise/Fall could ever be scanned even if wired
# elsewhere. Now a real union of every populated strategy list, so newly
# routed Jump/Bear-Bull symbols actually get scanned.
# DIGIT_PARITY_SYMBOLS / DRIFT_FADE_SYMBOLS are now explicitly defined
# above (both still [] — see STRATEGY ROUTING section) and included below
# so nothing needs to change here the day either one gets real symbols.
ALL_TRADE_SYMBOLS = list(dict.fromkeys(
    RISE_FALL_SYMBOLS + MEAN_REVERSION_SYMBOLS + RANGE_BREAK_SYMBOLS
    + BOOM_CRASH_SYMBOLS + STEP_SYMBOLS + JUMP_BUILDUP_SYMBOLS
    + BEAR_BULL_SYMBOLS + DIGIT_SYMBOLS + DIGIT_PARITY_SYMBOLS
    + DRIFT_FADE_SYMBOLS
))
ALL_SYMBOLS        = ALL_TRADE_SYMBOLS
VOLATILITY_SYMBOLS = ALL_TRADE_SYMBOLS  # alias for compatibility with bot_engine.py

# ── MULTIPLIER SETTINGS ──────────────────────────────────────
# Higher volatility = higher multiplier potential
MULTIPLIER_MAP = {
    # Low volatility — moderate multiplier
    "R_10":    100, "1HZ10V":  100,
    "R_25":    200, "1HZ25V":  200,
    # Medium volatility
    "R_50":    300, "1HZ50V":  300,
    "R_75":    500, "1HZ75V":  500,
    # High volatility — maximum multiplier
    "R_100":   500, "1HZ100V": 500,
    # 1HZ150V/1HZ200V/1HZ250V entries removed — confirmed invalid symbols,
    # see VOLATILITY_1S note above.
    # Boom/Crash — moderate (spike risk).
    # BOOM500/BOOM1000/CRASH500/CRASH1000: confirmed via the 2026-07-31
    # audit (real ranges x100-400 / x100-500), now in MULTIPLIER_SYMBOLS
    # above, values below (100) sit safely within range. buy_multiplier()
    # exists, so the method is no longer the blocker — but they still
    # won't actually trade until BOOM_CRASH_SYMBOLS is populated (still
    # empty, see BOOM_CRASH note up top) so they land in ALL_TRADE_SYMBOLS
    # and get routed to a strategy evaluator.
    # BOOM150/BOOM300/CRASH150/CRASH300 (un-suffixed): still UNAUDITED —
    # not in MULTIPLIER_SYMBOLS, values below are unverified guesses.
    "BOOM150": 100, "BOOM300": 100,
    "BOOM500": 100, "BOOM1000":100,
    "CRASH150":100, "CRASH300":100,
    "CRASH500":100, "CRASH1000":100,
    # Others
    "stpRNG":  200,
    # JD10-JD100: not yet in MULTIPLIER_SYMBOLS (Rise/Fall is used for
    # these right now, see RISE_FALL_SYMBOLS above; see the note by
    # MULTIPLIER_SYMBOLS for why they haven't been switched over).
    # Confirmed real ranges (2026-07-31 audit): JD10 x100-1000, JD25
    # x50-500, JD50 x20-200, JD75 x15-150, JD100 x10-100. JD50/JD75/JD100
    # below were capped down from 300/400/500 — those older values sat
    # outside the confirmed valid range and would have been rejected by
    # Deriv the moment Multiplier routing was ever turned on for them.
    "JD10":    100, "JD25":    200,
    "JD50":    200, "JD75":    150,
    "JD100":   100,
    # RDBULL/RDBEAR entries removed — audit confirmed no MULTUP/MULTDOWN
    # support on either; they're traded via Rise/Fall (CALL/PUT) instead,
    # see BEAR_BULL_SYMBOLS above. STOP_LOSS_MAP has no entry for them
    # either, so they fall back to DEFAULT_STOP_LOSS_PCT.
    "DSHIFT10":200, "DSHIFT20":200,
    "DSHIFT30":200,
}
DEFAULT_MULTIPLIER = 100

# ── STOP LOSS AND TAKE PROFIT (% of stake) ──────────────────
# Stop loss as % of stake — caps maximum loss per trade
STOP_LOSS_MAP = {
    # Low vol — tighter SL
    "R_10":    30.0,  "1HZ10V":  30.0,
    "R_25":    40.0,  "1HZ25V":  40.0,
    # Medium vol
    "R_50":    50.0,  "1HZ50V":  50.0,
    "R_75":    60.0,  "1HZ75V":  60.0,
    # High vol — wider SL to avoid noise
    "R_100":   70.0,  "1HZ100V": 70.0,
    # 1HZ150V/1HZ200V/1HZ250V entries removed — confirmed invalid symbols,
    # see VOLATILITY_1S note above.
    # Boom/Crash — wide SL due to spikes
    "BOOM150": 50.0,  "BOOM300": 50.0,
    "BOOM500": 50.0,  "BOOM1000":50.0,
    "CRASH150":50.0,  "CRASH300":50.0,
    "CRASH500":50.0,  "CRASH1000":50.0,
}
DEFAULT_STOP_LOSS_PCT = 50.0

# ── VOLATILITY / STEP FAMILY → MULTIPLIER MIGRATION ──────────
# Implementation Brief v4. Confirmed via symbol_audit.py, Render
# "symbol-audit" service, Aug 2026. Real contracts_for multiplier ranges —
# do not guess new values without re-running that audit; Deriv will reject
# anything outside [min, max].
#
# Moves these 11 symbols off Rise/Fall (CALL/PUT) and off the
# MEAN_REVERSION strategy entirely, onto Multiplier contracts
# (MULTUP/MULTDOWN) with the new VOL_BREAKOUT / VOL_REV_MULT strategy set
# (see signal_engine.py's evaluate_vol_regime() dispatcher). Boom/Crash
# (already on Multipliers) and Jump/Bear-Bull (deliberately staying on
# Rise/Fall — see notes above) are out of scope, untouched here.
VOL_MULTIPLIER_SYMBOLS = [
    "R_10", "R_25", "R_50", "R_75", "R_100",
    "1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V",
    "stpRNG",
]

# Confirmed ranges (min, max) — for validation / future dynamic sizing.
VOL_MULTIPLIER_RANGES = {
    "R_10":    (400, 4000), "1HZ10V":  (400, 4000),
    "R_25":    (160, 1600), "1HZ25V":  (160, 1600),
    "R_50":    (80,  800),  "1HZ50V":  (80,  800),
    "R_75":    (50,  500),  "1HZ75V":  (50,  500),
    "R_100":   (40,  400),  "1HZ100V": (40,  400),
    "stpRNG":  (750, 7500),
}

# Phase 1 default = the confirmed FLOOR of each range (least leverage this
# account allows). Do not raise these without also validating the dynamic
# stop-loss sizing below handles the new multiplier correctly — the floor
# is the only value verified safe by hand in the brief.
for _sym, (_lo, _hi) in VOL_MULTIPLIER_RANGES.items():
    MULTIPLIER_MAP[_sym] = _lo

# Wire into the existing Multiplier routing gate — bot_engine._execute()
# already does `if symbol in config.MULTIPLIER_SYMBOLS: buy_multiplier(...)`.
# This is the ONLY change needed to stop these 11 symbols trading Rise/Fall.
# NOTE: must run before EXIT_ENGINE_SYMBOLS = list(MULTIPLIER_SYMBOLS)
# further down this file, so the Adaptive Exit Engine automatically picks
# up all 11 new symbols with zero additional wiring — it does, this block
# sits well before that line.
MULTIPLIER_SYMBOLS = list(dict.fromkeys(MULTIPLIER_SYMBOLS + VOL_MULTIPLIER_SYMBOLS))

# ── SIX DEDICATED PER-SYMBOL EVALUATORS (handoff, Sep 15 2026) ───────────
# Replaces evaluate_popular_indicator() for exactly these nine symbols —
# signal_engine.py's SignalEngine.evaluate() checks these five lists
# BEFORE its general `if symbol in MULTIPLIER_SYMBOLS` branch, so a
# symbol listed here never reaches evaluate_popular_indicator() again.
# Deliberately NOT subtracted from VOL_MULTIPLIER_SYMBOLS / BOOM_CRASH /
# MULTIPLIER_SYMBOLS above — those still govern execution routing
# (buy_multiplier()), MULTIPLIER_MAP, STOP_LOSS_MAP, and
# EXIT_ENGINE_SYMBOLS (defined further below as list(MULTIPLIER_SYMBOLS))
# for these nine exactly as before; only evaluator selection changes.
# See signal_engine.py's evaluate_pullback_trend / evaluate_fast_mean_reversion
# / evaluate_spike_catch_1000 / evaluate_spike_catch_500 / evaluate_step_grid.
PULLBACK_TREND_SYMBOLS    = ["R_75", "1HZ75V"]                # Row 1
FAST_MEAN_REV_SYMBOLS     = ["R_100", "1HZ100V"]              # Row 2
SPIKE_CATCH_1000_SYMBOLS  = ["BOOM1000", "CRASH1000"]         # Row 3
SPIKE_CATCH_500_SYMBOLS   = ["BOOM500", "CRASH500"]           # Row 4
STEP_GRID_SYMBOLS         = ["stpRNG"]                         # Row 5
# Row 6 (Jump/JD75, price-action breakout) deliberately NOT built —
# flagged back to the user per the handoff's own instruction rather than
# risking a silent conflict with JD75's existing, currently-live
# evaluate_jump_buildup() digit-contract strategy. JUMP_BUILDUP_SYMBOLS
# is untouched; add a JD75-breakout list + evaluator here only once the
# user has confirmed how (or whether) it should coexist.

# ── Row 1: Trend-following with pullback entries (R_75, 1HZ75V) ─────────
PULLBACK_TREND_MIN_BARS          = 40
PULLBACK_EMA_FAST_PERIOD         = 20
PULLBACK_EMA_SLOW_PERIOD         = 50
PULLBACK_RSI_PERIOD              = 14
PULLBACK_RSI_OVERSOLD            = 30.0   # uptrend pullback: RSI must cross back ABOVE this, not just touch it
PULLBACK_RSI_OVERBOUGHT          = 70.0   # downtrend pullback: RSI must cross back BELOW this
PULLBACK_SWING_LOOKBACK          = 8      # bars searched for the pullback swing low/high
PULLBACK_EMA_TOUCH_ATR_MULT      = 1.0    # how close (in ATRs) the swing must come to the fast EMA to count as "pulled back to it"
PULLBACK_STOP_BUFFER_ATR_MULT    = 0.25   # stop = swing low/high +/- this many ATRs (buffer beyond the swing itself)
PULLBACK_TREND_RR_RATIO          = 2.0    # target = entry +/- this multiple of the entry-to-stop risk distance

# ── Row 2: Fast mean-reversion scalping (R_100, 1HZ100V) ────────────────
SCALP_MIN_BARS                   = 25
SCALP_BB_PERIOD                  = 14     # shorter than the popular-indicator BB period — "fast" scalp read
SCALP_BB_STD                     = 1.5
SCALP_RSI_PERIOD                 = 7      # fast RSI, per the handoff's "fast RSI extreme"
SCALP_RSI_OVERSOLD               = 20.0
SCALP_RSI_OVERBOUGHT             = 80.0
SCALP_STOP_BUFFER_ATR_MULT       = 0.15   # deliberately tight — "tight stops, small targets, high frequency"
# Target = the Bollinger mid-band at fire time (the mean itself) — see
# evaluate_fast_mean_reversion(), not a config constant since it's read
# live off bb_mid, not a fixed ratio.

# ── Rows 3/4: Boom/Crash spike-catching (drift-exhaustion entry) ────────
# Worked mechanism: track the small-tick drift between spikes (Boom
# drifts down between up-spikes, Crash drifts up between down-spikes);
# once that drift shows exhaustion/consolidation (ind.find_consolidation)
# and no spike has printed within the cooldown window, position counter
# to the drift (buy Boom, sell Crash) for the next spike. Row 4
# (BOOM500/CRASH500) uses a shorter cooldown (faster re-arm — this pair
# spikes more often) and a tighter stop buffer than row 3.
SPIKE_CATCH_1000_MIN_BARS              = 30
SPIKE_CATCH_1000_CONS_LOOKBACK         = 15
SPIKE_CATCH_1000_CONS_AVG_LOOKBACK     = 50
SPIKE_CATCH_1000_CONS_RATIO            = 0.4
SPIKE_CATCH_1000_DRIFT_LOOKBACK        = 20
SPIKE_CATCH_1000_MIN_DRIFT_ATR_RATIO   = 0.10
SPIKE_CATCH_1000_COOLDOWN_BARS         = 10
SPIKE_CATCH_1000_STOP_BUFFER_ATR_MULT  = 0.30
SPIKE_CATCH_1000_RR_RATIO              = 3.0   # spikes are typically large vs. the consolidation range — wider target multiple than rows 1/5

SPIKE_CATCH_500_MIN_BARS               = 30
SPIKE_CATCH_500_CONS_LOOKBACK          = 12    # shorter — this pair's spikes/consolidations cycle faster
SPIKE_CATCH_500_CONS_AVG_LOOKBACK      = 40
SPIKE_CATCH_500_CONS_RATIO             = 0.4
SPIKE_CATCH_500_DRIFT_LOOKBACK         = 15
SPIKE_CATCH_500_MIN_DRIFT_ATR_RATIO    = 0.10
SPIKE_CATCH_500_COOLDOWN_BARS          = 5     # faster re-arm than row 3's 10
SPIKE_CATCH_500_STOP_BUFFER_ATR_MULT   = 0.15  # tighter stop management than row 3's 0.30
SPIKE_CATCH_500_RR_RATIO               = 3.0

# ── Row 5: Step Index indicator-grid entry (stpRNG) ──────────────────────
# Hard AND-gate — all four must agree, this is not a scored/weighted pick:
#   long:  EMA10>EMA20 AND price>EMA20 AND RSI>STEP_GRID_RSI_LONG_MIN AND MACD_line>MACD_signal
#   short: EMA10<EMA20 AND price<EMA20 AND RSI<STEP_GRID_RSI_SHORT_MAX AND MACD_line<MACD_signal
STEP_GRID_MIN_BARS               = 30
STEP_GRID_EMA_FAST_PERIOD        = 10
STEP_GRID_EMA_SLOW_PERIOD        = 20
STEP_GRID_RSI_PERIOD             = 14
STEP_GRID_RSI_LONG_MIN           = 55.0
STEP_GRID_RSI_SHORT_MAX          = 45.0
STEP_GRID_MACD_FAST              = 12
STEP_GRID_MACD_SLOW              = 26
STEP_GRID_MACD_SIGNAL            = 9
STEP_GRID_RANGE_LOOKBACK         = 20     # bars searched for the range extreme the stop sits outside of
STEP_GRID_STOP_BUFFER_ATR_MULT   = 0.30
STEP_GRID_RR_RATIO               = 2.0

# Retire Mean Reversion entirely — MEAN_REVERSION_SYMBOLS was exactly
# VOLATILITY_STANDARD + VOLATILITY_1S, i.e. these same 10 symbols (stpRNG
# was never in it). Emptying this list retires the strategy globally
# without deleting evaluate_mean_reversion() from signal_engine.py (left
# in place, unrouted, in case it's wanted again later).
MEAN_REVERSION_SYMBOLS = []

# Pull all 11 out of RISE_FALL_SYMBOLS and out of STEP_SYMBOLS (for
# stpRNG specifically) so they don't double-route — stpRNG now trades
# under VOL_MULTIPLIER_SYMBOLS's new evaluator instead of STEP's.
RISE_FALL_SYMBOLS = [s for s in RISE_FALL_SYMBOLS if s not in VOL_MULTIPLIER_SYMBOLS]
STEP_SYMBOLS = [s for s in STEP_SYMBOLS if s not in VOL_MULTIPLIER_SYMBOLS]

# ALL_TRADE_SYMBOLS is a derived union (see original definition above) —
# recomputed here now that RISE_FALL_SYMBOLS / MEAN_REVERSION_SYMBOLS /
# STEP_SYMBOLS have changed, and with VOL_MULTIPLIER_SYMBOLS added so
# these 11 symbols keep being scanned instead of dropping out entirely.
ALL_TRADE_SYMBOLS = list(dict.fromkeys(
    RISE_FALL_SYMBOLS + MEAN_REVERSION_SYMBOLS + RANGE_BREAK_SYMBOLS
    + BOOM_CRASH_SYMBOLS + STEP_SYMBOLS + JUMP_BUILDUP_SYMBOLS
    + BEAR_BULL_SYMBOLS + DIGIT_SYMBOLS + DIGIT_PARITY_SYMBOLS
    + DRIFT_FADE_SYMBOLS + VOL_MULTIPLIER_SYMBOLS
))
ALL_SYMBOLS        = ALL_TRADE_SYMBOLS
VOLATILITY_SYMBOLS = ALL_TRADE_SYMBOLS  # alias for compatibility with bot_engine.py

# ── DYNAMIC, ATR-NORMALIZED STOP-LOSS (Fix H) ─────────────────
# See §1 of the brief: STOP_LOSS_MAP's static percentages were calibrated
# for Boom/Crash's ~x100 multiplier. Copied unchanged onto e.g. R_10's
# x400 floor, a 30% stop_loss_pct works out to ~0.075% price movement —
# inside normal tick noise for a 2-second-tick index, so positions would
# get stopped out by noise, not by the market being wrong. Fix: compute
# stop_loss_pct from live ATR instead, so the dollar stop always
# corresponds to a stable number of ATRs of real price movement no
# matter which multiplier a symbol is forced into. STOP_LOSS_MAP /
# DEFAULT_STOP_LOSS_PCT stay untouched and keep governing Boom/Crash
# exactly as before — this now applies to every symbol in
# config.MULTIPLIER_SYMBOLS (widened from VOL_MULTIPLIER_SYMBOLS only,
# Aug 2026, so Boom/Crash gets the same minimized stop as everything
# else — see RiskManager.compute_dynamic_stop_loss_pct() in
# risk_manager.py and its call site in bot_engine.py's _execute()).
DYNAMIC_STOP_LOSS_ENABLED   = True

# Safety margin applied outside the Kalman-filter noise floor (see
# risk_manager.compute_dynamic_stop_loss_pct()) — the smallest multiple
# of the filter's own residual noise band a stop can sit at before it's
# just measuring noise rather than a real move against the position.
# 1.2 means "20% wider than the noise band itself", the tightest margin
# considered still statistically defensible; the noise band itself
# (1.0x, no margin) is used as the hard floor beneath which the
# computed distance is never allowed to go.
STOP_KALMAN_SAFETY_MULT     = 1.2
STOP_KALMAN_LOOKBACK        = 20     # bars of residuals used for the noise read
# ── DYNAMIC STOP-LOSS FLOOR (user-directed, Aug 2026) — MINIMIZED ─────────
# Was 2.0 (target stop distance = 2x ATR) / 15.0 (never tighter than 15%
# of stake). The user reported the live stop-loss was landing bigger
# than the take-profit and asked for it to be as tight as it can
# mathematically be. STOP_ATR_MULT is left as a sanity ceiling only —
# risk_manager.compute_dynamic_stop_loss_pct() now takes the SMALLER of
# this ATR-based distance and a Kalman-filter noise-floor-based distance
# (see STOP_KALMAN_SAFETY_MULT below), and DYNAMIC_STOP_LOSS_PCT_MIN is
# now just a hard backstop for when neither read is available — the
# Kalman floor is what actually determines "the lowest it can possibly
# be" on a live trade, not this constant.
STOP_ATR_MULT               = 2.0    # stop distance ceiling, in ATRs of price
DYNAMIC_STOP_LOSS_PCT_MIN   = 3.0    # hard backstop only — see note above
DYNAMIC_STOP_LOSS_PCT_MAX   = 90.0   # ceiling — leave headroom under Deriv's
                                      # own 100%-of-stake max-loss cap

# ── VOL REGIME DETECTION (for VOL_BREAKOUT / VOL_REV_MULT, signal_engine.py) ──
# ENHANCEMENT (win-rate pass, Aug 2026): dashboard trade history showed
# VOL_BREAKOUT losing on the large majority of its trades while carrying
# a healthy win/loss $ ratio — i.e. the direction/exit logic is fine, the
# entries firing on noise are the problem. Root cause: a single-bar
# ratio>=0.6 read let a transient EMA wiggle flip the regime to TREND for
# one cycle, routing straight into a breakout evaluator with no real
# trend behind it. Added a persistence requirement (VOL_REGIME_CONFIRM_BARS
# below) rather than changing which evaluator handles which regime — same
# two strategies, stricter gate on which one fires.
# CORRECTION (same pass, second iteration): the ratio itself was first
# raised 0.6 -> 0.85 alongside the persistence requirement — stacking both
# knobs at once turned out to suppress TREND classification almost
# entirely (dashboard went quiet across all 11 VOL_MULTIPLIER_SYMBOLS,
# leaving only BOOM_CRASH visible). 0.6 was never really the problem —
# no persistence requirement was. Reverted the ratio to its original
# value and kept persistence as the only added lever.
VOL_REGIME_TREND_RATIO = 0.6   # |EMA_fast-EMA_slow| / ATR >= this -> TREND,
                                # else RANGE. Back to its original value —
                                # see CORRECTION note above.
VOL_REGIME_CONFIRM_BARS = 2    # the ratio must clear VOL_REGIME_TREND_RATIO
                                # on this many consecutive completed bars
                                # (not just the latest) before the regime
                                # is called TREND. Any NaN/insufficient
                                # history in the window defaults to RANGE
                                # (the more conservative evaluator).

# ── VOL_BREAKOUT ENTRY CONFIRMATION (win-rate pass, Aug 2026) ─────────────
# A close that merely touches the Donchian channel edge was being scored
# as a full breakout — on 2s/1m synthetic ticks that's frequently just
# noise. BREAKOUT_MARGIN_ATR requires the close to clear the channel by a
# real distance (in ATRs) before it counts. Same Donchian+EMA+MACD scoring
# model as before — this only tightens what counts as "broke the level".
# CORRECTION (same pass, second iteration): an earlier version of this
# fix also required the *prior* bar to already be sitting near the
# channel edge, on the theory that would filter single-tick spikes.
# In practice that blocks the sharp, decisive candle a real breakout
# often is — it only let through slow grinding moves that were already
# extended, which is backwards for an entry strategy. Removed; the ATR
# margin alone is the filter now.
# CORRECTION (win-rate pass, Aug 2026, third iteration): live Render logs
# showed VOL_BREAKOUT consistently landing at 4/7 (EMA+MACD agree, break
# condition alone fails) across many different VOL_MULTIPLIER_SYMBOLS,
# never reaching the 6/7 fire threshold — for 8+ days straight, zero
# VOL_BREAKOUT trades. A fresh 20-bar Donchian high on a near-random-walk
# instrument typically only clears the prior high by a small fraction of
# ATR, not 15% of it — 0.15 was still too strict even after already being
# implicated once in this same pass. Lowered to 0.05: still requires a
# real move past the level (not a bare 1-tick touch, the original bug),
# just not an unrealistically large one.
BREAKOUT_MARGIN_ATR = 0.05

# ── VOL_REV_MULT ENTRY CONFIRMATION (win-rate pass, Aug 2026) ────────────
# When True, evaluate_vol_reversion_mult() requires the latest close to
# have already ticked back toward the mean vs. the prior close (not just
# RSI/BB/ROC sitting at an extreme) before firing — cuts entries taken
# while price is still accelerating into the extreme ("catching a falling
# knife"). Does not change the RSI/Bollinger/ROC thresholds that define
# the setup itself.
MEAN_REV_REQUIRE_TURN = True

# Take profit = stop loss × this ratio.
# ENHANCEMENT (win-rate pass, Aug 2026): raised 2.0 -> 2.5. Dashboard
# history already shows winners running several multiples larger than
# losers ($ magnitude) — this gives the exit engine's trailing layer
# (below) more room to ride a genuine winner before the static outer
# boundary force-closes it, without touching the stop-loss side (and
# therefore without changing per-trade downside risk).
# For Multiplier contracts these remain the static outer boundary set at
# buy time — see ADAPTIVE EXIT ENGINE near the bottom of this file for
# the layer that trails stop_loss inside this boundary via
# contract_update, without changing this ratio itself.
TAKE_PROFIT_RATIO = 2.5

# ── SIGNAL DIRECTION: TAKE AS COMPUTED (user-directed, Sep 2026) ─────────
# Was "UNIVERSAL SIGNAL INVERSION" (Aug 2026 - Sep 2026): unconditionally
# flipped every computed direction (BUY placed as SELL, SELL as BUY)
# before every order. Reverted per explicit instruction: the bot now
# trades exactly what the indicator layer (see POPULAR INDICATOR STRATEGY
# below) computed — its LONG is placed as LONG, its SHORT as SHORT, its
# own stop-loss price level is sent as the stop-loss, its own take-profit
# price level is sent as the take-profit. No per-symbol table, no
# win-rate gating, no flip, no swap. See bot_engine.py's execution path
# for the single choke point that applies this (search "SIGNAL DIRECTION:
# TAKE-AS-COMPUTED").
INVERT_ALL_SIGNALS = False

# ── TP/SL SWAP FOR MULTIPLIER CONTRACTS (reverted, Sep 2026) ─────────────
# Was: for Multiplier contracts using the legacy stake-percentage SL/TP
# path (deriv_client.buy_multiplier(), only reached when the signal has
# no native SL/TP price levels — i.e. NOT the live popular-indicator
# path, which always supplies native levels), the computed stop-loss and
# take-profit dollar amounts were swapped before being sent. Reverted
# alongside INVERT_ALL_SIGNALS above, for the same reason: stop-loss goes
# where stop-loss was computed, take-profit goes where take-profit was
# computed. TAKE_PROFIT_RATIO (take-profit = stop-loss x this ratio) is
# unchanged — only the swap is removed.
TP_SL_SWAP_ENABLED = False

# ── DELAYED ENTRY — percent-trigger toward target, target the native stop
#    (user-directed, Sep 12 2026, replacing the stop-as-trigger design) ───
# A firing signal is NOT bought immediately. It's armed
# (bot_engine.py's self._pending_entries) and watched tick by tick against
# its own RAW, UNMODIFIED native levels:
#
#   TRIGGER: price moves DELAYED_ENTRY_TRIGGER_PCT of the way from
#   native_entry_price toward native_target_price (confirms the
#   indicator's move looked real).
#     -> enter there, live price
#     -> take-profit = the original native_stop_price, used as-is
#     -> direction MUST flip to the OPPOSITE of the indicator's original
#        call — this is forced, not a toggle: with entry now sitting
#        between the original entry and target, native_stop_price is on
#        the far side, opposite from where price just came from, and a
#        level on that side can only be a valid take-profit for a trade
#        running the other way. Confirmed with the user before
#        implementing.
#     -> stop-loss, computed AFTER the target: placed on the OPPOSITE side
#        of entry from the target, at 1/STOP_TRIGGERED_RISK_REWARD_RATIO of
#        the target distance — so the reward:risk ratio is always exactly
#        STOP_TRIGGERED_RISK_REWARD_RATIO (2:1 by default), by
#        construction, regardless of what the original indicator's own
#        entry-to-stop distance was.
#
# Worked example: indicator says LONG, entry=100, native_stop=90,
# native_target=130, DELAYED_ENTRY_TRIGGER_PCT=0.25. Trigger = 107.5 (25%
# toward 130). If reached, enter SHORT there (inverted): take-profit = 90
# (the original native_stop), stop-loss = 107.5 - ((90-107.5)/2) = 116.25
# (distances 17.5 vs 8.75 -> exactly 2:1). Mirrored the same way for an
# original SHORT: entry=100, native_stop=110, native_target=70 -> trigger
# = 92.5, enter LONG there (inverted): take-profit=110, stop-loss=83.75
# (same ratio).
#
# STOP_LOSS_MIDPOINT_PCT and the previous fade-back/two-trigger/
# stop-as-trigger designs are all dormant now. There is no "scrap"
# outcome. Only DELAYED_ENTRY_TIMEOUT_SECS still results in no trade at
# all, if the market never reaches the trigger.
#
# Only applies to signals that carry native price levels (the live
# popular-indicator path) with direction LONG/SHORT and contract_kind
# RISE_FALL — DIGIT signals (Jump buildup) are untouched. See
# bot_engine.py's _arm_pending_entry / _check_pending_entry /
# _execute_pending_entry for the implementation.
DELAYED_ENTRY_ENABLED      = True
DELAYED_ENTRY_TIMEOUT_SECS = 600   # 10 min — tune if setups expire too eagerly/slowly
DELAYED_ENTRY_TRIGGER_PCT  = 0.25   # REACTIVATED — was dormant at 0.75; before
                                      # that 0.50, 0.25, 0.15, 0.75, 0.33
# NOTE (handoff, Sep 15 2026): the above DELAYED_ENTRY_* design and this
# STOP_TRIGGERED_RISK_REWARD_RATIO constant remain exactly as they were —
# still live for every symbol EXCEPT the nine in STOP_AS_TRIGGER_SYMBOLS
# below, which now go through that parallel path instead (see
# bot_engine.py's dispatch: `stop_trigger_eligible` is checked, and takes
# priority, before `delayed_eligible`). Nothing here was retired globally.

STOP_TRIGGERED_RISK_REWARD_RATIO = 2.0  # target_distance / stop_distance, held exactly via construction

# ── STOP-AS-TRIGGER ENTRY (handoff, Sep 15 2026) ──────────────────────────
# A NEW, PARALLEL pending-order path — scoped only to
# config.STOP_AS_TRIGGER_SYMBOLS (the nine symbols carved out above to
# evaluate_pullback_trend / evaluate_fast_mean_reversion /
# evaluate_spike_catch_1000 / evaluate_spike_catch_500 /
# evaluate_step_grid) — built alongside, not in place of, the
# DELAYED_ENTRY_* design above. This is the "stop as the trigger, no
# direction flip" mechanic that bot_engine.py's PendingEntry docstring and
# this file's dormant STOP_TRIGGERED_TARGET_PCT (below) already describe,
# but that the live DELAYED_ENTRY_* design (percent-trigger toward
# target, forced flip) does not actually implement — genuinely built here
# for the first time, in bot_engine.py's StopTriggerPendingEntry /
# _arm_stop_trigger_entry / _check_stop_trigger_entry /
# _execute_stop_trigger_entry.
#
# Mechanism:
#   1. A firing signal from one of the five evaluators above is armed
#      (not bought), watched tick by tick against its own RAW native
#      levels — never re-run through DELAYED_ENTRY_* or the
#      fixed-percentage immediate-entry path.
#   2. TRIGGER = the evaluator's own native_stop_price. Reaching it is
#      also where we enter — direction is NEVER flipped, we take exactly
#      what the evaluator signalled.
#   3. Take-profit = the evaluator's original native_target_price, used
#      as-is (unchanged from what was armed).
#   4. Stop-loss is RE-DERIVED after the fill, from the live entry-to-
#      target distance, so that take_profit_distance / stop_loss_distance
#      is STRICTLY greater than STOP_AS_TRIGGER_MIN_RR_RATIO (enforced
#      with `>`, not `>=`): max_sl_distance = target_distance / ratio,
#      then the actual sl_distance sits STOP_AS_TRIGGER_SL_SAFETY_MARGIN
#      below that ceiling so it's never exactly on the boundary.
#   5. Cancel-before-fill: if price reaches the ORIGINAL
#      native_target_price before ever reaching the trigger, the pending
#      order is cancelled outright — no trade. A time-based expiry
#      (shared DELAYED_ENTRY_TIMEOUT_SECS) remains as a fallback safety
#      net.
#
# Worked example (LONG signal, mirrors the handoff's own): evaluator
# fires LONG on R_75, native_entry_price=100 (informational only),
# native_stop_price=95, native_target_price=115.
#   - Price reaching 115 first -> cancelled, no trade.
#   - Price falling to 95 first -> enter LONG at 95 (live price).
#     take-profit stays 115. target_distance = 115-95 = 20.
#     max_sl_distance = 20 / 2.0 = 10. sl_distance = 10 * (1-0.10) = 9.
#     new stop-loss = 95 - 9 = 86 (ratio 20:9 ≈ 2.22:1, strictly > 2.0).
# Mirrored symmetrically for SHORT (stop above entry, target below).
STOP_AS_TRIGGER_ENABLED          = True
STOP_AS_TRIGGER_SYMBOLS = list(dict.fromkeys(
    PULLBACK_TREND_SYMBOLS + FAST_MEAN_REV_SYMBOLS
    + SPIKE_CATCH_1000_SYMBOLS + SPIKE_CATCH_500_SYMBOLS + STEP_GRID_SYMBOLS
))
STOP_AS_TRIGGER_MIN_RR_RATIO     = 2.0   # take_profit_distance / stop_loss_distance must be STRICTLY greater than this
STOP_AS_TRIGGER_SL_SAFETY_MARGIN = 0.10  # actual stop sits this fraction inside the max-allowed distance (see worked example)

# ── RESTRICT TRADING TO THE 5 NEW EVALUATORS ONLY (user request, Sep 16 2026) ──
# When True, the bot scans and trades ONLY the nine STOP_AS_TRIGGER_SYMBOLS —
# every other strategy (evaluate_popular_indicator() on the remaining
# R_10/R_25/R_50/1HZ10V/1HZ25V/1HZ50V, DIGIT_SYMBOLS, JUMP_BUILDUP_SYMBOLS,
# BEAR_BULL_SYMBOLS, RANGE_BREAK_SYMBOLS, DRIFT_FADE_SYMBOLS) is effectively
# disabled — not by deleting or unrouting any of that code, but by shrinking
# ALL_TRADE_SYMBOLS (the single master scan list bot_engine.py._init_all_symbols()
# and symbol_manager.py's get_queue()/update_active() both read, confirmed the
# only consumer — TRADE_SYMBOLS isn't otherwise defined in this file) down to
# just the nine. A symbol that's never in the scan queue is never evaluated,
# so its strategy function never runs, full stop.
# Flip this back to False (or narrow STOP_AS_TRIGGER_SYMBOLS instead) to bring
# any of the other strategies back — nothing else needs to change.
RESTRICT_TRADING_TO_STOP_AS_TRIGGER_SYMBOLS = True
if RESTRICT_TRADING_TO_STOP_AS_TRIGGER_SYMBOLS:
    ALL_TRADE_SYMBOLS = list(STOP_AS_TRIGGER_SYMBOLS)
    ALL_SYMBOLS        = ALL_TRADE_SYMBOLS
    VOLATILITY_SYMBOLS = ALL_TRADE_SYMBOLS

# DORMANT (Sep 12 2026): was the target-placement percentage for the
# previous "stop as trigger" design (target = trigger + this % of the
# original entry-to-target distance). The current design targets
# native_stop_price directly instead — not read by the current design.
STOP_TRIGGERED_TARGET_PCT = 0.5

# DORMANT (Sep 12 2026): superseded by the "DELAYED ENTRY — native stop as
# the trigger" design above — with DELAYED_ENTRY_ENABLED=True, every
# native-level-carrying signal becomes delayed-eligible and never reaches
# this transform at all. Left in place in case immediate entry is wanted
# again later (set DELAYED_ENTRY_ENABLED=False to make this live again).
# ── FIXED-PERCENTAGE-OF-TARGET NATIVE LEVELS (user-directed, Sep 12 2026,
#    inversion twist same day) ────────────────────────────────────────────
# Entry is immediate, exactly as the indicator signals — no delay,
# native_entry_price used unmodified. Direction and levels behave
# differently depending on FIXED_ENTRY_INVERT_DIRECTION:
#
# FIXED_ENTRY_INVERT_DIRECTION = True (current default): direction flips
# (LONG<->SHORT: indicator's buy executes as sell, sell as buy) and the
# take-profit/stop-loss mirror to the sides that match the FLIPPED
# direction — both still measured off the SAME entry-to-target distance:
#   take_profit = native_entry_price - FIXED_ENTRY_TP_PCT * (native_target_price - native_entry_price)
#   stop_loss   = native_entry_price + FIXED_ENTRY_SL_PCT * (native_target_price - native_entry_price)
# Worked example: indicator says LONG (buy), entry=100, native_target=130.
# Executed as SHORT (sell) instead, take_profit=85 (50% of the
# entry-to-target distance, now BELOW entry — the flipped trade's profit
# side), stop_loss=107.5 (25% of that distance, now ABOVE entry — the
# flipped trade's loss side). Ratio 0.50/0.25 = exactly 2:1.
#
# FIXED_ENTRY_INVERT_DIRECTION = False: direction is left alone and the
# same two distances land on their ORIGINAL (non-mirrored) sides instead:
#   take_profit = native_entry_price + FIXED_ENTRY_TP_PCT * (native_target_price - native_entry_price)
#     -> on the TARGET side of entry, same side as the original native target.
#   stop_loss   = native_entry_price - FIXED_ENTRY_SL_PCT * (native_target_price - native_entry_price)
#     -> on the OPPOSITE side of entry (the native stop-loss's side).
#
# Either way, both formulas are direction-agnostic — (native_target_price -
# native_entry_price) already carries the right sign for LONG vs SHORT. See
# bot_engine.py's _apply_fixed_pct_native_levels() for the implementation,
# and _execute()'s `inverted` bookkeeping (keyed off
# SignalResult.execution_inverted) for how a flip gets recorded for
# meta-labeling / strategy_stats.
FIXED_ENTRY_LEVELS_ENABLED   = True
FIXED_ENTRY_INVERT_DIRECTION = True
FIXED_ENTRY_TP_PCT           = 0.50   # was 0.59
FIXED_ENTRY_SL_PCT           = 0.25   # was 0.29 — ratio held at exactly 2:1

# DORMANT as of the Sep 12 2026 fixed-percentage redesign above — that
# design uses native_target_price/native_stop_price directly as the exact
# stop-loss, not a midpoint. Left here in case a future design wants it
# back; not read by any current code path.
# Stop-loss placement (user-directed, Sep 12 2026): on whichever branch
# fires (confirm or reject), the stop is no longer pinned exactly to
# native_entry_price — it sits STOP_LOSS_MIDPOINT_PCT of the way between
# native_entry_price and the ACTUAL fill price (wherever the trigger
# executed). 0.5 = the midpoint. 0.0 would put it back at native_entry_price
# exactly; 1.0 would put it at the fill price itself (no room at all).
# See bot_engine.py's _execute_pending_entry / _check_pending_entry's
# planned_stop logging for the implementation.
STOP_LOSS_MIDPOINT_PCT = 0.5

# ── SCALED NATIVE SL/TP (user-directed, Sep 11 2026) ──────────────────────
# NOTE: no longer used by the delayed-entry path above (Sep 12 2026) — that
# path now builds its own stop/target directly from the two-trigger design.
# This transform only still runs for a signal that skips delayed entry
# entirely (missing native levels, DIGIT contracts, or
# DELAYED_ENTRY_ENABLED=False) — a no-op in practice for anything currently
# traded, since every live symbol carries native levels and goes through
# delayed entry instead. Left in place, dormant, in case it's wanted again.
# Decides WHAT gets executed — direction and stop/target — for a signal
# that skips delayed entry (no native levels, DIGIT contracts, or
# DELAYED_ENTRY_ENABLED=False), entry left unmodified. As of Sep 12 2026
# this is the ONLY place it still runs; see the dormancy note above.
#
# SCALED_SL_TP_INVERT_DIRECTION = True (current default): direction flips
# (LONG<->SHORT) and the two scaled distances land mirrored, on the sides
# that match the FLIPPED direction:
#   new_stop   = native_entry_price + SCALED_SL_TARGET_MULT *
#                (native_target_price - native_entry_price)
#   new_target = native_entry_price - SCALED_TP_STOP_MULT *
#                (native_entry_price - native_stop_price)
# Worked example: indicator says LONG, entry=100, native_stop=90,
# native_target=130. Executed as SHORT instead, with stop=118 (35% of the
# original entry-to-target distance, now above entry) and target=86.7
# (150% of the original entry-to-stop distance, now below entry).
#
# SCALED_SL_TP_INVERT_DIRECTION = False: direction is left alone and the
# same two distances scale onto their ORIGINAL sides instead of mirrored:
#   new_target = native_entry_price + SCALED_TP_STOP_MULT *
#                (native_entry_price - native_stop_price)
#   new_stop   = native_entry_price - SCALED_SL_TARGET_MULT *
#                (native_target_price - native_entry_price)
#
# Either way, both formulas are direction-agnostic (native_target_price -
# native_entry_price) and (native_entry_price - native_stop_price) already
# carry the right sign for LONG vs SHORT. See bot_engine.py's
# _apply_scaled_native_levels() for the implementation, and _execute()'s
# `inverted` bookkeeping (keyed off SignalResult.execution_inverted) for
# how a flip gets recorded for meta-labeling / strategy_stats.
# DISABLED (user-directed, Sep 12 2026): FIXED_ENTRY_LEVELS_ENABLED above
# takes over entirely for the currently-active immediate-execution path,
# and it never flips direction ("we are going to trade just how the
# indicators tell us to, no inversion"). Left in place, dormant.
SCALED_SL_TP_ENABLED          = False
SCALED_SL_TP_INVERT_DIRECTION = True
SCALED_TP_STOP_MULT           = 1.50   # was 1.33
SCALED_SL_TARGET_MULT         = 0.35   # was 0.60

# Per-(indicator, symbol) suspension window (spec point 8, Aug 2026): when
# strategy_stats.is_underperforming(strategy, symbol) first flips True for a
# given (indicator, symbol) pair, pair_suspension.maybe_suspend() starts a
# flat, non-renewing clock this many minutes long. Only that pair sits out;
# every other indicator keeps trading that symbol, and this indicator keeps
# trading every other symbol. See pair_suspension.py.
PAIR_SUSPEND_MINUTES = 60

# ── POPULAR INDICATOR CONFLUENCE STRATEGY (user-directed, Aug 2026) ──────
# Replaces the strategy used to trade every symbol in MULTIPLIER_SYMBOLS
# (Volatility/1Hz/Step family via VOL_MULTIPLIER_SYMBOLS, and the
# Boom/Crash family) with a single composite evaluator
# (signal_engine.evaluate_popular_confluence()) built only from the
# technical indicators and approaches most widely used by retail and
# algorithmic traders generally — chosen for popularity of use, not for
# any backtested edge on these specific instruments. Old strategy-specific
# evaluators (evaluate_vol_breakout, evaluate_vol_reversion_mult,
# evaluate_boom_crash) are left in signal_engine.py, unrouted, the same
# way this codebase already retires strategies (see MEAN_REVERSION_SYMBOLS
# above) rather than deleting the code.
#
# Each indicator below casts one vote for LONG or SHORT (or abstains).
# The composite direction is whichever side has the larger weighted vote
# total; POPULAR_CONFLUENCE_MIN_SCORE is the minimum fraction of total
# available weight that side must reach to fire at all.
POPULAR_CONFLUENCE_MIN_SCORE = 0.55

# Trend-following (moving averages, ADX/DMI, Parabolic SAR, Supertrend,
# Ichimoku) and momentum/oscillator (RSI, MACD, Stochastic, CCI,
# Williams %R, Bollinger Bands) indicator periods — standard textbook
# defaults, the same defaults most charting platforms ship with, since
# those defaults are themselves a large part of why these indicators are
# "the ones many people use".
POPULAR_SMA_FAST_PERIOD   = 10
POPULAR_SMA_SLOW_PERIOD   = 30
POPULAR_EMA_FAST_PERIOD   = 12
POPULAR_EMA_SLOW_PERIOD   = 26
POPULAR_RSI_PERIOD        = 14
POPULAR_RSI_OVERBOUGHT    = 70.0
POPULAR_RSI_OVERSOLD      = 30.0
POPULAR_MACD_FAST         = 12
POPULAR_MACD_SLOW         = 26
POPULAR_MACD_SIGNAL       = 9
POPULAR_BB_PERIOD         = 20
POPULAR_BB_STD            = 2.0
POPULAR_STOCH_K_PERIOD    = 14
POPULAR_STOCH_D_PERIOD    = 3
POPULAR_STOCH_OVERBOUGHT  = 80.0
POPULAR_STOCH_OVERSOLD    = 20.0
POPULAR_ADX_PERIOD        = 14
POPULAR_ADX_TREND_MIN     = 20.0   # ADX below this = indicator abstains
POPULAR_SAR_STEP          = 0.02
POPULAR_SAR_MAX_STEP      = 0.20
POPULAR_ICHIMOKU_TENKAN   = 9
POPULAR_ICHIMOKU_KIJUN    = 26
POPULAR_ICHIMOKU_SENKOU_B = 52
POPULAR_CCI_PERIOD        = 20
POPULAR_CCI_OVERBOUGHT    = 100.0
POPULAR_CCI_OVERSOLD      = -100.0
POPULAR_WILLIAMS_R_PERIOD = 14
POPULAR_SUPERTREND_PERIOD = 10
POPULAR_SUPERTREND_MULT   = 3.0

# "Cutting edge" computational additions layered on top of the classic
# indicator set above: a Kalman filter (adaptive trend/velocity estimate,
# reacts faster in high-volatility regimes and slower in calm ones without
# needing a fixed lookback window) and a Hurst exponent regime read
# (rescaled-range analysis — H > 0.5 trending/persistent, H < 0.5
# mean-reverting/anti-persistent) used to dynamically re-weight the
# trend-following indicators against the mean-reversion/oscillator ones
# rather than weighting every indicator equally regardless of regime.
# NOTE: the live signal_engine.py evaluator was subsequently simplified
# to a single-most-popular-indicator pick rather than this confluence
# vote (per a later, separate request) and no longer reads
# POPULAR_CONFLUENCE_MIN_SCORE / POPULAR_HURST_LOOKBACK /
# POPULAR_HURST_MIN_BARS — left in place here untouched, unused, exactly
# as they already are in the live file, since only the stop-loss-related
# settings below were touched this round.
POPULAR_KALMAN_PROCESS_VAR  = 1e-5
POPULAR_KALMAN_MEASURE_VAR  = 1e-2
POPULAR_HURST_LOOKBACK      = 100
POPULAR_HURST_MIN_BARS      = 40

# ── STAKE SETTINGS ───────────────────────────────────────────
# MANUAL STAKE MODE (user-directed, Sep 12 2026): the person wants direct
# control of stake size from here, full stop — everything else that used
# to have a say (BASE_STAKE_PCT/balance-scaling, the PLS win/loss-streak
# multiplier, the Kelly overlay, the stability dampener, the portfolio
# exposure ceiling) is bypassed entirely. risk_manager.calculate_stake()
# checks this flag first and — when True — returns MANUAL_STAKE_AMOUNT
# immediately, no other stake logic runs at all. Set False to restore all
# of the dynamic sizing below exactly as it was.
MANUAL_STAKE_MODE   = True
MANUAL_STAKE_AMOUNT = 0.5
# Only remaining size-relevant guard when MANUAL_STAKE_MODE is True:
# MAX_CONCURRENT_TRADES below caps position COUNT (not total $ exposure) —
# at 100.0 × that limit, worst-case simultaneous exposure is bounded, just
# no longer expressed as a % of balance.

BASE_STAKE_PCT       = 0.005   # 0.5% of current balance per trade — this
                                # IS the compounding: stake grows/shrinks
                                # automatically as balance grows/shrinks.
                                # INACTIVE while MANUAL_STAKE_MODE = True.
MIN_STAKE            = 0.5    # UPDATED — was 100, then 0.35. Now matches
                                # MANUAL_STAKE_AMOUNT ($0.5); the
                                # codebase's own built-in default
                                # (risk_manager.py's RiskManager falls back
                                # to 0.35 here if config.MIN_STAKE is ever
                                # missing) is now stale relative to this
                                # value — the fallback only matters if this
                                # setting itself goes missing, which it
                                # isn't. Doubles as the can_trade()
                                # balance floor (current_balance > MIN_STAKE
                                # required to trade at all) — at 100 this
                                # would have silently blocked all trading
                                # on any real balance under $100.
                                # SUPERSEDED Sep 12 2026 by MANUAL_STAKE_MODE
                                # above — this value (and the dynamics this
                                # comment describes) only matter again if
                                # that flag is turned back off. This is also
                                # the exact bug that led to MANUAL_STAKE_MODE:
                                # the stability dampener (drawdown/loss-streak,
                                # further below) ran AFTER this $100 floor
                                # clamp, so any dampening at all pushed the
                                # stake below $100, and calculate_stake()
                                # zeroed it outright rather than shrinking it
                                # — a hard stop after as few as 2 consecutive
                                # losses, not a graceful size-down.
MAX_STAKE            = 1000.0  # safety backstop only, not the everyday driver.
                                # INACTIVE while MANUAL_STAKE_MODE = True.
DAILY_LOSS_LIMIT_PCT = float("inf")  # REMOVED PER REQUEST — was 0.06 (6%).
                                # bot_engine._check_confirmed_loss_limit()
                                # compares confirmed_daily_loss/day_start_balance
                                # against this value to set _confirmed_paused;
                                # inf makes that comparison impossible to trip,
                                # so trading is never auto-paused on daily loss.
                                # DAILY_LOSS_PAUSE_MINS below is now unused —
                                # nothing left to trigger a pause to time out.
DAILY_LOSS_PAUSE_MINS = 30

# FIX (profitability audit, round 2): global, account-wide circuit breaker —
# pause ALL new entries (any symbol/strategy) after this many consecutive
# losses, independent of the %-based DAILY_LOSS_LIMIT_PCT above. Added
# because a bad run (e.g. 5 losses in a 7-trade session) previously had
# nothing account-wide stopping it short of that much coarser daily-%
# threshold. See BotEngine._global_consecutive_losses.
GLOBAL_CONSECUTIVE_LOSS_LIMIT = 4
GLOBAL_CONSECUTIVE_LOSS_PAUSE_MINS = 0  # REMOVED PER REQUEST — was 45.
                                # This is the 2nd of 3 pause mechanisms
                                # (daily %, this one, buy-failure breaker
                                # below) — 0min makes a trip a no-op.

# ── EQUITY CURVE STABILIZATION (win-rate/drawdown pass, Aug 2026) ─────────
# The circuit breaker above is binary: trading stops entirely for
# GLOBAL_CONSECUTIVE_LOSS_PAUSE_MINS once it trips, then resumes at full
# size. Between "nothing" and "fully paused" there was no way for stake
# to ease down smoothly during a rough patch and ease back up as it
# recovers — every red dot on the balance curve landed at the same $
# size as every green one, which is what makes the curve zig-zag.
# RiskManager._stability_dampener_mult() (risk_manager.py) applies a
# continuous multiplier on top of PLS/Kelly, driven by two signals, and
# takes whichever is more conservative rather than multiplying them
# (they're correlated — a loss streak IS a drawdown — so multiplying
# would double-punish the same event):
#   1. Distance below the balance high-water mark (drawdown %)
#   2. Consecutive losses since the last win
# Both recover automatically as balance/streak improve — no separate
# "unpause" event needed, unlike the hard breaker above.
DRAWDOWN_DAMPENER_ENABLED  = True
DRAWDOWN_DAMPENER_START_PCT = 0.015  # below this drawdown from peak balance,
                                       # no throttling at all (1.0x)
DRAWDOWN_DAMPENER_FULL_PCT  = 0.06   # drawdown at which the floor multiplier
                                       # is reached — matches DAILY_LOSS_LIMIT_PCT
                                       # so the dampener has fully engaged by the
                                       # time the hard daily-loss breaker would trip
DRAWDOWN_DAMPENER_FLOOR     = 0.40   # stake never shrinks below 40% of normal
                                       # from this signal alone

LOSS_STREAK_DAMPENER_ENABLED = True
# (consecutive losses since last win) -> stake multiplier. First entry is
# the implicit 0-1 loss baseline (no throttle); each tuple after that is
# (streak_count, multiplier), checked in order, last match wins.
LOSS_STREAK_DAMPENER_TABLE = [
    (2, 0.85),
    (3, 0.70),
    (4, 0.55),  # GLOBAL_CONSECUTIVE_LOSS_LIMIT=4 hard-pauses right after
                # this tier — the floor here is deliberately close to
                # LOSS_STREAK_DAMPENER's own floor rather than needing a
                # tier of its own beyond this point.
]

# ── AGGRESSIVE COMPOUNDING ───────────────────────────────────
# Disabled per user request — stake no longer scales up on win streaks.
# Multipliers all set to 1.0 so PLS tier lookups (wherever risk_manager.py
# applies them) are a no-op; stake stays flat regardless of streak length.
PLS_WIN_THRESHOLDS  = [3,   5,   8,   12,  15  ]
PLS_WIN_MULTIPLIERS = [1.0, 1.0, 1.0, 1.0, 1.0]
PLS_WIN_EXTRA_SLOTS = [0,   0,   0,   0,   0   ]

# ── CONCURRENT TRADES ────────────────────────────────────────
# FIX (profitability audit): was 30. With every trade effectively forced to
# $100 (see MIN_STAKE fix above) that allowed up to $3,000 of simultaneous
# exposure — a large fraction of account equity open at once, much of it in
# highly-correlated symbols (e.g. R_10 and 1HZ10V both track the same
# volatility parameter). Lowered to reduce simultaneous drawdown risk;
# raise gradually only once live win-rate/profit-factor justify it.
MAX_CONCURRENT_TRADES = 6

# Correlated-symbol grouping — synthetic indices sharing the same underlying
# volatility parameter (just different tick generation) move together far
# more than unrelated symbols do, so treating them as independent slots
# understates real concurrent risk. Caps how many concurrently-open
# positions may share a family, on top of MAX_CONCURRENT_TRADES overall.
# See bot_engine.py's execution loop for the enforcement point.
SYMBOL_FAMILY_MAP = {
    "R_10": "VOL10", "1HZ10V": "VOL10",
    "R_25": "VOL25", "1HZ25V": "VOL25",
    "R_50": "VOL50", "1HZ50V": "VOL50",
    "R_75": "VOL75", "1HZ75V": "VOL75",
    "R_100": "VOL100", "1HZ100V": "VOL100",
    "BOOM500": "BOOMCRASH500", "CRASH500": "BOOMCRASH500",
    "BOOM1000": "BOOMCRASH1000", "CRASH1000": "BOOMCRASH1000",
}
MAX_CONCURRENT_PER_FAMILY = 2

# ── TIMEFRAMES ───────────────────────────────────────────────
HTF_GRANULARITY   = 3600   # 1H
MTF_GRANULARITY   = 300    # 5M
LTF_GRANULARITY   = 60     # 1M
HTF_BARS          = 100
MTF_BARS          = 50
LTF_BARS          = 30

# ── SIGNAL SETTINGS ──────────────────────────────────────────
MIN_SIGNAL_SCORE       = 0.68
MIN_STRATEGY_AGREEMENT = 4

# SMC parameters
OB_LOOKBACK            = 50
FVG_MIN_ATR            = 0.5
SWEEP_LOOKBACK         = 20
SWING_LOOKBACK         = 5
FIB_LEVELS             = [0.382, 0.5, 0.618, 0.786]
FIB_TOLERANCE          = 0.1
EMA_FAST              = 8
EMA_SLOW              = 21
EMA_TREND             = 50
RSI_PERIOD            = 14
RSI_OVERBOUGHT        = 70
RSI_OVERSOLD          = 30
MOMENTUM_LOOKBACK     = 10
ATR_PERIOD            = 14
BREAKOUT_ATR_MULT     = 1.5

# ── CONTRACT SETTINGS ────────────────────────────────────────
# Real, actively-used constants — bot_engine.py reads these two names
# directly (previously it read CONTRACT_MAX_AGE_SECS/CONTRACT_FORCE_CLOSE_SECS
# which didn't exist here at all, silently falling back to unsafe 120s/300s
# hardcoded defaults against a real 14-minute/840s contract duration — see
# Implementation Brief v2, Fix B). Derived from TRADE_DURATION (14m = 840s)
# with a generous margin, per Deriv's documented multi-minute settlement lag.
CONTRACT_MAX_AGE_SECS     = 900     # trigger a non-destructive poll
CONTRACT_FORCE_CLOSE_SECS = 1350    # trigger active reconciliation (never a guess)

# Per-symbol Rise/Fall duration overrides (seconds omitted — same unit as
# TRADE_DURATION_UNIT). Populate here if a contracts_for audit finds a
# symbol that rejects the default TRADE_DURATION (14m). Empty = every
# symbol uses TRADE_DURATION/TRADE_DURATION_UNIT unchanged.
TRADE_DURATION_OVERRIDES = {}

# ── RECONCILIATION (never-fabricate-a-result path, Fix C) ────
# After CONTRACT_FORCE_CLOSE_SECS, a Rise/Fall contract that still hasn't
# settled moves to "reconcile_pending" instead of being marked a loss.
# It keeps polling on this cadence until it resolves for real, or until
# RECONCILE_MAX_SECS is hit, at which point it's escalated/logged loudly
# but STILL never assigned a guessed win/loss.
RECONCILE_POLL_INTERVAL_SECS = 30
RECONCILE_MAX_SECS           = 1800   # 30 min — far longer than any real
                                       # settlement should ever take

# ── MULTIPLIER CONTRACTS — explicit max-hold policy (Fix E) ──
# Multiplier contracts have no fixed expiry. If held this long, the bot
# actively calls sell_contract() to realize the real price (never a
# guess) and logs it as a deliberate time-based close — this replaces
# the old dead MAX_TRADE_OPEN_MINS/CHECK_TRADE_MINS constants, which were
# never actually read by anything.
# This remains the outer horizontal barrier either way — see the
# ADAPTIVE EXIT ENGINE section near the bottom of this file for the
# active management layer that now operates *inside* this bound
# (and inside STOP_LOSS_MAP / TAKE_PROFIT_RATIO below), rather than
# replacing it.
MULTIPLIER_MAX_HOLD_MINS = 30

# ── SYMBOL SUSPENSION (minutes) ──────────────────────────────
SYMBOL_WIN_SUSPEND_MINS   = 30     # was 20 — increased by 10min per user request
SYMBOL_MIN_GAP_MINS       = 1

# Escalating per-symbol loss suspension ladder (Implementation Brief v2,
# Requirement 2 / Fix F). Indexed by min(loss_count, len(ladder)) - 1, so
# 1st consecutive session loss on a symbol -> 60min, 2nd -> 120min,
# 3rd -> 180min, 4th and every further loss that session -> 240min.
# This counter/ladder is reset ONLY by a redeploy (a real process
# restart) — never by a UTC-midnight or other calendar boundary.
# Replaces the old flat SYMBOL_LOSS_SUSPEND_MINS / SYMBOL_SESSION_BAN_LOSSES
# (59,940-minute "session ban") scheme entirely.
SESSION_LOSS_SUSPEND_LADDER_MINS = [70, 130, 190, 250]  # was [60,120,180,240] — +10min per tier per user request

# ── RAW TICK BUFFER (feeds tick-based evaluators via evaluate(ticks=...)) ──
TICK_BUFFER_MAXLEN = 200

# ── DEGRADED-SYMBOL TICK-SUBSCRIPTION RETRY ──────────────────
TICK_RESUBSCRIBE_RETRY_SECS = 30

# ── BUY-FAILURE CIRCUIT BREAKER ──────────────────────────────
BUY_FAILURE_CIRCUIT_BREAKER_THRESHOLD    = 5
BUY_FAILURE_CIRCUIT_BREAKER_SUSPEND_MINS = 0  # REMOVED PER REQUEST — was
                                # 15. 3rd pause mechanism: per-symbol
                                # suspend after repeated buy failures.
                                # 0min = no-op. NOTE: if a symbol's buy
                                # keeps failing (e.g. it doesn't actually
                                # support DIGITOVER/DIGITUNDER), the bot
                                # will now retry it forever every cycle
                                # instead of backing off — watch the logs
                                # for repeated PLACEMENT FAILED on one
                                # symbol.

# ── SCANNING ────────────────────────────────────────────────
SCAN_CYCLE_SLEEP       = 1
INIT_BATCH_SIZE        = 8
INIT_BATCH_DELAY       = 0.3
PRIORITY_SYMBOLS = [
    "R_75","R_100","1HZ75V","1HZ100V",
    "R_50","R_25",
]

# ── RATE LIMITING ────────────────────────────────────────────
BUY_REQUEST_DELAY_SECS = 3.0
MAX_BUY_PER_SECOND     = 3

# ── RENDER ──────────────────────────────────────────────────
RENDER_DEPLOY_HOOK_URL = os.environ.get(
    "RENDER_DEPLOY_HOOK_URL","")
# REDEPLOY_EVERY_N_CYCLES previously = 8. With SETTLE_WAIT_SECS defaulting to
# 15s (bot_engine._settle_loop), that was an 8*15=120s cycle-based redeploy —
# fighting restart_scheduler.py's timer. Set high enough that it never fires
# on its own; restart_scheduler.py's daily Kenya-midnight timer (see
# REDEPLOY_TIMEZONE below) is the only authoritative redeploy trigger. Lower
# this back down only if you deliberately want a SECOND, settle-count-based
# redeploy path in addition to the daily timer.
REDEPLOY_EVERY_N_CYCLES = 999999
SETTLE_WAIT_SECS = 15

# restart_scheduler.py fires every REDEPLOY_INTERVAL_HOURS, anchored to
# 00:00 in this zone (Africa/Nairobi = EAT = UTC+3 year-round, no DST) —
# so with the default of 6 that's 00:00 / 06:00 / 12:00 / 18:00 EAT (4
# redeploys/day). Was a once-daily fixed 00:00 timer per Implementation
# Brief v2, Fix G; widened to 4x/day on request — see restart_scheduler.py's
# _next_scheduled_fire().
REDEPLOY_TIMEZONE = "Africa/Nairobi"
REDEPLOY_INTERVAL_HOURS = 5 / 60   # 5 minutes, expressed as hours since
                                      # that's the unit restart_scheduler.py
                                      # expects (interval_secs = hours*3600).
                                      # Was 13.7 min, before that 1h, 3h.

# How long bot_engine.py's _settle_loop will wait, actively trying to
# confirm-close every remaining open contract, once a redeploy has been
# scheduled, before delaying the redeploy rather than wiping contract
# bookkeeping (Fix G). Kept generous even at 4x/day — 30min of drain
# headroom out of every 6h window is still cheap, and a redeploy that's
# delayed a few minutes because a contract is still confirming its close
# is far better than one that guesses.
DRAIN_MAX_SECS = 1800

# ── ALIASES (required by bot_engine.py / risk_manager.py) ────
RISK_PER_TRADE_PCT = BASE_STAKE_PCT          # alias
MAX_CONCURRENT     = 20               # alias — NOTE: mismatched with MAX_CONCURRENT_TRADES=30, see flags
DAILY_LOSS_LIMIT   = DAILY_LOSS_LIMIT_PCT    # alias

# ── ADDITIONAL SIGNAL/RISK SETTINGS ──────────────────────────
MIN_MODULES_FOR_SIGNAL     = 3
MIN_INDICATOR_VOTES        = 3
OB_EXPIRY_BARS             = 100
NEWS_BLOCK_MINUTES         = 60
FOREX_LTF_GRANULARITY      = 900
OTHER_LTF_GRANULARITY      = 60
MIN_SIGNAL_PROBABILITY     = 1.8
MIN_STRENGTH_REPEAT_SYMBOL = 3

# ── DERIV WEBSOCKET ───────────────────────────────────────────
DERIV_WS_URL : str = os.environ.get(
    "DERIV_WS_URL",
    f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
)

# ── SIGNAL GENERATION GATES (additional) ─────────────────────
MIN_SCORE                  = 2.0
MIN_CONFLUENCE             = 2
MIN_MODULE_STRENGTH        = 2
MIN_MODULE_STRENGTH_NORMAL = 2
MIN_CONFIDENCE_NORMAL      = 5
MIN_CONFIDENCE_FOR_PARTIAL = 5

# ── SESSION TIMING — disabled for 24/7 synthetics ────────────
DEAD_ZONE_START_UTC  = 0
DEAD_ZONE_END_UTC    = 5
BOOM500_PRIME_START  = 7
BOOM500_PRIME_END    = 12
TRADE_DURATION = 6
TRADE_DURATION_UNIT = "m"

# ══════════════════════════════════════════════════════════════
# NEW STRATEGY CONFIG (added in this pass — none of these are
# wired into ALL_SYMBOLS / signal_engine.py routing yet; they are
# config surfaces for strategies you're building incrementally)
# ══════════════════════════════════════════════════════════════

# ── DIGIT STRATEGY (Over/Under) ───────────────────────────────
# When True, a Digit Over/Under signal must be confirmed by BOTH an
# indicator-based read AND a statistical digit-frequency read before
# it fires. DIGIT_SYMBOLS is still empty above, so this is inert
# until you populate that list.
DIGIT_HYBRID_MODE = False

# ── ACCUMULATOR SETTINGS ──────────────────────────────────────
ACCU_GROWTH_RATE_MIN = 1.0   # percent, per-tick growth rate floor
ACCU_GROWTH_RATE_MAX = 5.0   # percent, per-tick growth rate ceiling
# Fraction of a symbol's historical average in-range tick survival at
# which to take profit early instead of holding to knockout.
# e.g. 0.7 = exit once you've captured 70% of the typical survival length.
ACCU_EXIT_FRACTION = 0.7

# ── STRATEGY PERFORMANCE MONITORING ───────────────────────────
# Once a (strategy, symbol) pair has this many logged trades, flag it as
# underperforming if its win rate falls below the floor. FIX (profitability
# audit): was 100 — is_underperforming() sat completely unused by any
# execution path (dead code, confirmed by grep across the codebase), so
# raising this wasn't even the bottleneck; the real fix is wiring it into
# signal_engine.SignalEngine.evaluate() (done) with a threshold low enough
# to matter before large losses accumulate. 30 aligns with KELLY_MIN_TRADES's
# order of magnitude below.
STRATEGY_WIN_RATE_FLOOR = 0.55
STRATEGY_WIN_RATE_MIN_TRADES = 30

# ── META-LABELING (future ML filter) ──────────────────────────
META_LABEL_MIN_TRADES      = 200   # trades required before the filter is trusted
META_LABEL_RETRAIN_EVERY_N = 100   # retrain cadence, in newly logged trades
# FIX (profitability audit): required buffer above breakeven for the EV
# gate in meta_labeling.predict_take_trade() — was an implicit 0.0, taking
# any trade with a nominally-positive point estimate regardless of how
# noisy that estimate was.
META_LABEL_EV_MARGIN        = 0.03

# ── SIGNAL DIRECTION INVERSION (win-rate/drawdown pass, Aug 2026) ────────
# meta_labeling.predict_take_trade() can return "INVERT" in addition to
# "TAKE"/"SKIP": once a (strategy, symbol) pair's per-pair EV model has
# enough history (META_LABEL_EV_MIN_FEATURE_ROWS rows, config below) to
# produce a real estimate of p(win | features) for the signal's ORIGINAL
# direction, and that estimate is low enough that the OPPOSITE direction
# has the better expected value, bot_engine._execute() flips sig.direction
# before placing the order — same entry price, same ATR-based stop/target
# sizing (already symmetric for LONG vs SHORT), just the side is flipped.
# This cannot fire for a pair until it has real evidence — below
# META_LABEL_EV_MIN_FEATURE_ROWS this always returns TAKE, never a guess.
# Simplifying assumption: the inverted direction's win probability is
# approximated as (1 - p_hat_original) and its payout ratio as the same
# avg_ratio already measured for the original direction. Real
# execution/spread asymmetries mean this is an approximation, not exact —
# worth revisiting once enough inverted trades exist to measure their own
# realized payout ratio directly instead of borrowing the original's.
META_LABEL_INVERT_ENABLED = True
INVERT_MIN_CONFIDENCE     = 0.65  # only invert when the OPPOSITE direction's
                                    # estimated win probability clears this —
                                    # deliberately higher than the plain
                                    # META_LABEL_EV_MARGIN skip bar, since
                                    # inverting is a stronger claim than
                                    # simply not trusting the original signal

# NOTE: an earlier iteration of this design had a separate
# META_LABEL_NO_SKIP_STRATEGIES set (BOOM_CRASH only) alongside a
# strategy-level default-action map. Superseded by the per-symbol default
# map below — no symbol is ever skipped outright now (the gate always
# picks between a symbol's default and its opposite), so a standalone
# no-skip list is redundant with that design and has been removed.

# ── PER-SYMBOL DEFAULT DIRECTION (win-rate/drawdown pass, Aug 2026) ──────
# User-directed design, refined from a strategy-level default to a
# symbol-level one: the "H..." volatility symbols (1HZ10V/25V/50V/75V/100V)
# default to TAKE (their own coded strategy, un-inverted); BOOM/CRASH
# symbols default to INVERT; the remaining VOL_MULTIPLIER_SYMBOLS — the
# plain R_xx symbols and stpRNG — also default to INVERT. AI/ML only
# steers a symbol off its default once the per-pair EV model has
# accumulated enough evidence (META_LABEL_EV_MIN_FEATURE_ROWS rows) that
# doing so is actually the better bet.
#
# IMPORTANT HONESTY NOTE, read before changing these defaults: below the
# per-pair data threshold there is no real evidence either way for that
# SPECIFIC symbol — every one of these starting postures is a directional
# choice the user made deliberately, not something any model calculated.
# Once a symbol crosses the data threshold, its action becomes genuinely
# evidence-based and can move off its default.
#
# Built from the symbol lists above rather than hand-typed, so this can
# never silently drift out of sync with VOL_MULTIPLIER_SYMBOLS/BOOM_CRASH
# if either list changes later.
#
# REFINEMENT (win-rate/drawdown pass, Aug 2026): R_10/R_100 moved back to
# TAKE and 1HZ10V/1HZ100V moved to INVERT — a deliberate per-user swap on
# top of the general "H... symbols TAKE, others INVERT" rule below, not a
# correction of it. Built as an explicit override dict applied after the
# general rule so the exception is easy to find and won't get silently
# regenerated away if the base rule above is ever rebuilt from the symbol
# lists again.
META_LABEL_DEFAULT_ACTION_BY_SYMBOL: dict = {
    **{s: "TAKE" for s in VOL_MULTIPLIER_SYMBOLS if s.startswith("1HZ")},
    # REVERTED (win-rate pass, Aug 2026): BOOM_CRASH was set to INVERT by
    # explicit user request earlier this session. Fresh trade data since
    # then (uploaded dashboard screenshots) shows BOOM_CRASH at ~28.6%
    # win rate (2W/5L) under INVERT — a stark reversal from the ~73.3%
    # win rate (11W/4L) it had under TAKE earlier in this same session,
    # before any of these changes. That's exactly the signature you'd
    # expect if the original BOOM_CRASH signal has genuine positive edge:
    # inverting a strategy that's actually skilled turns a good edge into
    # a bad one. Reverted back to TAKE on that evidence. (Small sample
    # either way — ~8-15 trades per period — so keep an eye on this as
    # more data accumulates, but the direction and size of the swing is a
    # strong enough signal to act on now rather than wait.)
    **{s: "TAKE" for s in BOOM_CRASH},
    **{s: "INVERT" for s in VOL_MULTIPLIER_SYMBOLS if not s.startswith("1HZ")},
    # Explicit per-symbol overrides — applied last so they win regardless
    # of the general rules above. This is the current, final state (not a
    # diff/history of prior rounds):
    # UPDATED (Aug 2026, explicit user request): R_10 -> TAKE, 1HZ25V ->
    # TAKE, R_100 -> INVERT. This flips R_10/R_100 back from their prior
    # swap earlier this session and moves 1HZ25V off the general "1HZ ->
    # TAKE" rule above.
    "R_10":     "TAKE",
    "R_25":     "TAKE",
    "1HZ10V":   "TAKE",
    "1HZ25V":   "TAKE",
    "1HZ75V":   "INVERT",
    "1HZ100V":  "TAKE",
    "R_100":    "INVERT",
    # ALT METHOD REMOVED (win-rate pass, Aug 2026, per explicit user
    # request: "strictly either take or invert, no ALT"). R_75 and
    # 1HZ50V now get plain defaults like every other symbol, same as the
    # general VOL_MULTIPLIER_SYMBOLS rule above would already give them
    # (R_75 -> INVERT via the non-"1HZ" branch, 1HZ50V -> TAKE via the
    # "1HZ" branch) — listed explicitly here anyway so the fact that they
    # WERE special-cased is visible in the diff rather than silently
    # disappearing. The Bayesian bandit below (not the old ALT method)
    # is what now gets a chance to move either of these off its default,
    # same mechanism as every other symbol, once it has its own evidence.
    "R_75":     "INVERT",
    "1HZ50V":   "TAKE",
}
# Fallback for any symbol not explicitly listed above (e.g. JD10-JD100,
# RDBEAR/RDBULL — strategies where INVERT isn't even applicable, see
# bot_engine._execute()'s contract_kind guard) — TAKE, i.e. behave as
# though this whole feature didn't exist for them.
META_LABEL_DEFAULT_ACTION_FALLBACK = "TAKE"

# ── BAYESIAN TAKE/INVERT BANDIT (win-rate pass, Aug 2026) ─────────────────
# User-directed replacement for the old enriched-feature EV-gate AND for
# the ALT method (both removed): meta_labeling.py's predict_take_trade()
# now compares each symbol's own realized TAKE vs INVERT win/loss counts
# (strategy_stats.get_take_invert_stats() — no other data, no borrowed
# priors) via a Beta-Bernoulli posterior comparison, and only switches a
# symbol off its default once there's real, appropriately-humble
# statistical confidence the other mode is actually better. See
# meta_labeling.py's _prob_a_beats_b() for the exact math.
BAYESIAN_MIN_SAMPLES_FOR_OVERRIDE = 8     # the OVERRIDE mode (not the
                                            # default) needs at least this
                                            # many of its own trades before
                                            # its posterior is trusted at all
BAYESIAN_OVERRIDE_CONFIDENCE      = 0.80  # P(override mode's true win rate
                                            # > default mode's) must clear
                                            # this before switching — a real
                                            # posterior probability, not a
                                            # raw point-estimate threshold,
                                            # so it's automatically stricter
                                            # with fewer trades and looser
                                            # with more, no separate
                                            # sample-size rule needed


# ── POSITION SIZING (Kelly) ───────────────────────────────────
# Conservative multiplier applied to full Kelly-optimal sizing.
# 0.25 = quarter-Kelly.
KELLY_FRACTION_MULTIPLIER = 0.25

# ── ENSEMBLE MODE ──────────────────────────────────────────────
# When True, requires 2+ independent strategies to agree within the
# agreement window before a signal fires.
ENSEMBLE_MODE = False
ENSEMBLE_AGREEMENT_WINDOW_SECS = 60
ENSEMBLE_MIN_STRATEGIES_AGREEING = 2

# ── SESSION / DAY-OF-WEEK SCORE WEIGHTING ─────────────────────
# Multiplier applied to signal score based on symbol category, UTC
# hour, and day of week. Range is 0.8-1.2 by convention (0.8 = dampen,
# 1.2 = boost, 1.0 = neutral/no adjustment).
#
# `days` uses Python's datetime.weekday() convention:
#   Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6
# `days: None` means "applies every day". `hours_utc` is an inclusive
# (start, end) 24h UTC range; `hours_utc: None` means "applies all hours".
#
# NOTE: Boom/Crash symbols are currently NOT traded by this bot
# (BOOM_CRASH = [] above, blocked on buy_multiplier() being built). Jump
# indices (JD10-JD100) ARE now traded via Rise/Fall as of the 2026-07-31
# audit, but get_symbol_class() still returns a generic category for
# them (check symbols.py — there's no "jump" branch in get_symbol_class()
# yet), so these table entries have no effect until that's added. These
# remain config-only placeholders for now.
SESSION_DOW_WEIGHT_TABLE = {
    "BOOM600_CRASH900": {
        "hours_utc": (14, 20),   # boosted 14:00-20:00 UTC
        "days": None,             # every day
        "multiplier": 1.2,
    },
    "BOOM300N_CRASH300N": {
        "hours_utc": None,
        "days": [6],               # Sunday
        "multiplier": 1.15,
    },
    "JUMP50": {
        "hours_utc": (11, 13),   # around 12:00 UTC
        "days": [5],               # Saturday
        "multiplier": 1.15,
    },
}
SESSION_DOW_WEIGHT_DEFAULT = 1.0  # applied when no table entry matches

# ══════════════════════════════════════════════════════════════
# ADAPTIVE EXIT ENGINE (Multiplier / non-time-bound contracts only)
# ══════════════════════════════════════════════════════════════
# Rise/Fall contracts are untouched by this — they keep using
# TRADE_DURATION/TRADE_DURATION_UNIT exactly as before (see SESSION /
# DAY-OF-WEEK section above; do not touch those two constants or
# TRADE_DURATION_OVERRIDES for this feature).
#
# Multipliers (MULTUP/MULTDOWN — MULTIPLIER_SYMBOLS above) have no fixed
# expiry; today they close only via the static STOP_LOSS_MAP /
# TAKE_PROFIT_RATIO set at buy time, or the blunt
# MULTIPLIER_MAX_HOLD_MINS forced close (see MULTIPLIER CONTRACTS
# section above). This engine actively manages the open contract
# between those two existing boundaries — trailing the stop-loss up as
# profit grows, and closing early if profit decays — instead of just
# waiting for one of the two static limits to fire. It never replaces
# STOP_LOSS_MAP, TAKE_PROFIT_RATIO, or MULTIPLIER_MAX_HOLD_MINS; those
# stay in force as the outer vertical/horizontal barriers this engine
# operates inside of. Lives in exit_engine.py (new file, built
# separately); revises stop_loss/take_profit on an already-open
# Multiplier contract via Deriv's contract_update request, wired up in
# deriv_client.py (also built separately). This section only adds the
# config surface it needs.
EXIT_ENGINE_ENABLED         = True
EXIT_ENGINE_SYMBOLS         = list(MULTIPLIER_SYMBOLS)  # only Multiplier contracts

# Rule-based trailing layer (always active — the ML layer below only ever
# adds an *earlier* close on top of this, never removes this safety net):
# ENHANCEMENT (win-rate pass, Aug 2026): retuned all three fractions.
# 30%-to-arm / 60%-lock / 25%-decay was letting a lot of paper profit
# round-trip back into a loss (or a much smaller win) on a low-win-rate
# strategy before the trailing layer ever engaged. Arming earlier and
# locking a bigger share of peak profit banks more of every winner —
# raises effective win/loss $ skew without touching entry logic.
EXIT_ARM_PROFIT_FRACTION    = 0.15   # start trailing once profit >= 15% of the
                                      # contract's static take_profit_amount
                                      # (was 0.30 — armed too late)
EXIT_TRAIL_LOCK_FRACTION    = 0.75   # once armed, ratchet stop_loss to lock in
                                      # 75% of peak profit seen so far
                                      # (was 0.60 — gave back too much)
EXIT_DECAY_CLOSE_FRACTION   = 0.20   # once armed, close immediately if profit
                                      # falls back below 20% of peak (rather than
                                      # waiting for the original static stop_loss)
                                      # (was 0.25)
EXIT_POLL_INTERVAL_SECS     = 15     # how often the exit engine re-checks each
                                      # open Multiplier contract (independent of
                                      # the general 30s orphan-sweep cadence)

# EARLY LOSS CAP (Aug 2026, user-directed: "take low losses, minimal
# magnitude"). Before this, a LOSING contract had zero active management
# from this engine — the arm/trail/decay logic above only ever engages
# once profit is positive (>= EXIT_ARM_PROFIT_FRACTION * static_tp_amount).
# An unarmed, losing trade rode all the way out to the FULL static
# stop-loss (STOP_LOSS_MAP — 30-70% of stake) before anything closed it.
# This adds a second, independent rule-layer check that runs regardless
# of arm state: once past a short grace window (to avoid closing on
# ordinary entry noise/spread), close immediately if the loss reaches
# EXIT_LOSS_CAP_FRACTION of the contract's own static stop-loss budget —
# i.e. realize a small fraction of the worst-case loss instead of the
# whole thing. This can only make losses SMALLER than STOP_LOSS_MAP
# already caps them at; it never overrides or loosens that static bound.
EXIT_LOSS_CAP_ENABLED       = True
EXIT_LOSS_CAP_FRACTION      = 0.25   # close once loss >= 25% of this
                                      # contract's static_sl_amount, e.g.
                                      # on a symbol with a 50%-of-stake
                                      # static SL, the realized loss caps
                                      # out around 12.5% of stake instead
EXIT_LOSS_CAP_GRACE_SECS    = 20     # ignore the loss cap for this many
                                      # seconds after open (~1 poll cycle)
                                      # so normal entry spread/noise
                                      # doesn't trigger an instant exit

# Lightweight ML layer (meta-labeling-inspired; reuses the existing
# META_LABEL_MIN_TRADES / META_LABEL_RETRAIN_EVERY_N constants defined
# above under META-LABELING — do not duplicate them here). Below
# META_LABEL_MIN_TRADES logged Multiplier-contract snapshots this is a
# strict no-op; only the rule-based layer above runs.
EXIT_ML_ENABLED             = True
EXIT_ML_MODEL_PATH          = "exit_model.joblib"  # ephemeral on Render free
                                                     # tier — resets on redeploy;
                                                     # acceptable for this
                                                     # research phase, retrains
                                                     # from fresh logs each time
EXIT_ML_FEATURE_WINDOW      = 5      # number of past polls used to compute
                                      # profit "velocity" as a feature
EXIT_ML_MIN_CONFIDENCE      = 0.60   # ML must be at least this confident a
                                      # reversal is coming to override the rule
                                      # layer's HOLD decision

# ═══════════════════════════════════════════════════════════════════════════
# DONKEY STRATEGY — inverted digit-frequency + inverted trend-filter
# ═══════════════════════════════════════════════════════════════════════════
# Signal 1 (frequency): bets the HOT digit (last DONKEY_FREQ_WINDOW ticks)
#   continues, structured so the COLD digit falls on the losing side.
# Signal 2 (trend filter): DONKEY_TREND_SMA_PERIOD-tick SMA; fires
#   DIGITUNDER at DONKEY_TREND_BARRIER only when the current tick is BELOW
#   the SMA. See signal_engine.evaluate_donkey_strategy() for the exact
#   logic, and deriv_client.DerivClient._digit_contract_type() for the
#   DIGITOVER/DIGITUNDER support this relies on.
#
# DONKEY_STRATEGY_ENABLED doubles as the GLOBAL EXECUTION GATE — see
# SignalEngine.evaluate() in signal_engine.py, checked FIRST, before any
# other evaluator in that file. While True (the default), Donkey Strategy
# is the ONLY strategy in the entire bot that can produce a tradeable
# signal; every other evaluate_*() function stays in the file untouched
# but unreachable. Setting this False does not hand control back to the
# legacy multi-strategy routing — it just stops the bot from trading at
# all. There is deliberately no separate toggle for that.
DONKEY_STRATEGY_ENABLED = True

# Symbols Donkey Strategy is allowed to trade — defaults to the full
# 26-symbol universe already wired into ALL_TRADE_SYMBOLS. NOTE: this
# file's own STRATEGY ROUTING section above documents that DIGITOVER/
# DIGITUNDER (digit-contract) support has only been explicitly
# audit-confirmed for JD10-JD100 and RDBEAR/RDBULL on this account — the
# rest are included per spec ("all 26 symbols") but are UNVERIFIED for
# digit contracts specifically. buy_digit_contract() already fails safe
# (logs the proposal error, returns None — no crash) if Deriv rejects a
# symbol for DIGITOVER/DIGITUNDER, so nothing breaks either way, but don't
# be surprised if some symbols simply never place a trade. Narrow this
# list to a confirmed subset once you've checked contracts_for per symbol.
DONKEY_STRATEGY_SYMBOLS = list(ALL_TRADE_SYMBOLS)

# "INDEPENDENT": Signal 1 and Signal 2 each fire on their own whenever
#   they individually qualify (Signal 1 checked first; falls through to
#   Signal 2 only if Signal 1 has no read on that tick).
# "COMBINED": only fires when both signals agree — both must have a read
#   AND both land on DIGITUNDER (Signal 2 is DIGITUNDER-only by spec, so a
#   Signal-1 OVER pick can never combine — independent mode still fires
#   it, combined mode just won't on those ticks), taking the more
#   restrictive of the two barriers (min) so a combined win is guaranteed
#   to satisfy both signals' individual criteria at once.
DONKEY_STRATEGY_MODE = "INDEPENDENT"   # "INDEPENDENT" | "COMBINED"

# ── Donkey variant lock (Sep 2026, chat-requested) ─────────────────────
# The strategy-cycle balance-trend switch and the RAW variant it could
# switch onto are BOTH disabled now — see signal_engine.
# _donkey_active_variant(), which no longer reads strategy_cycle.py at
# all and unconditionally returns "ORIGINAL". strategy_cycle.py itself
# is no longer imported anywhere (dead file, kept only for reference).
# ORIGINAL = has inversion (bets hot digit continues, DIGITUNDER trend
#   filter) — the only variant that can trade now.
DONKEY_CYCLE_START = "ORIGINAL"        # kept only as _donkey_active_
                                        # variant()'s belt-and-suspenders
                                        # fallback value; not otherwise
                                        # read anymore.

# ── Fixed trading cycle (Sep 2026, chat-requested; restructured same
#    day from an earlier two-leg version — leg 2 removed, cooldown
#    length randomized. Supersedes and disables the old balance-trend
#    Donkey switch above AND the profit-target cycle that used to live
#    in this section; strategy_cycle.py and profit_cycle.py are no
#    longer imported anywhere) ────────────────────────────────────────
# Trading now runs on a simple, fixed, never-ending single-leg pattern
# instead of anything balance-, profit-, drawdown-, or time-of-day-
# driven — see fixed_cycle.py for the full state machine:
#   1. Trade for FIXED_CYCLE_LEG_MINUTES ("leg 1") — ONE deploy, no
#      auto-redeploy while it's running. REDEPLOY_INTERVAL_HOURS above
#      is already set to this same 5 minutes, so the existing rolling-
#      redeploy mechanism (restart_scheduler.py) IS leg 1's timer — no
#      separate clock needed for it, and its firing always means leg 1
#      just ended (there's no leg 2 to redeploy into anymore).
#   2. Drain open contracts, then disconnect from Deriv entirely for a
#      cooldown drawn fresh, uniformly at random, between
#      FIXED_CYCLE_COOLDOWN_MIN_MINUTES and FIXED_CYCLE_COOLDOWN_MAX_
#      MINUTES (health checks only during this window), then redeploy
#      back into a fresh leg 1.
#   3. Repeat forever. Nothing — not daily drawdown, not profit, not
#      time of day — can skip, shorten, lengthen, or pause any of this.
FIXED_CYCLE_LEG_MINUTES          = 5     # length of leg 1 — kept equal
                                          # to REDEPLOY_INTERVAL_HOURS
                                          # (5 min) above; change both
                                          # together if you ever want a
                                          # different leg length
FIXED_CYCLE_COOLDOWN_MIN_MINUTES = 1     # was 75 — lower bound of the
                                          # randomized cooldown after
                                          # every leg 1
FIXED_CYCLE_COOLDOWN_MAX_MINUTES = 3     # was 150 — upper bound; a fresh
                                          # value is drawn uniformly at
                                          # random between these two
                                          # every time leg 1 ends (see
                                          # fixed_cycle.enter_cooldown_
                                          # now()), then persisted as an
                                          # absolute deadline so it
                                          # can't be re-rolled by a
                                          # redeploy landing mid-cooldown

# Render API credentials used ONLY to persist fixed_cycle.py's state
# (trading/cooldown phase + the cooldown deadline) into this service's
# own env vars so it survives a redeploy — Render's disk here is
# ephemeral, so without these the bot would forget an in-progress
# cooldown (and its randomly chosen length) on any redeploy that lands
# mid-cooldown for an unrelated reason.
RENDER_API_KEY    = os.environ.get("RENDER_API_KEY", "")
RENDER_SERVICE_ID = os.environ.get("RENDER_SERVICE_ID", "")

# Signal 1 — frequency window (ticks) and minimum sample before it's
# willing to call a hot/cold digit at all.
DONKEY_FREQ_WINDOW      = 100
DONKEY_FREQ_MIN_SAMPLE  = 100

# Signal 2 — SMA period. DONKEY_TREND_BARRIER default raised 3 -> 8 (see
# below) so this signal has a chance of clearing DONKEY_MIN_WIN_PROB
# instead of being dead weight (UNDER 3 is a fixed 30%-of-digits contract
# — see the P(win) note below).
DONKEY_TREND_SMA_PERIOD = 8
DONKEY_TREND_BARRIER    = 9    # UNDER 9 -> P=0.90, matches the max-win-
                                # probability ceiling below so signal 2
                                # can still qualify instead of always
                                # losing to the 0.90 floor.

# ═══════════════════════════════════════════════════════════════════════
# DONKEY WIN-RATE TUNING (Sep 28 2026, chat-requested — win rate was ~10%)
# ═══════════════════════════════════════════════════════════════════════
# Root cause of the ~10% win rate: signal_engine._donkey_signal_1_original
# picked the barrier CLOSEST to the hot digit (OVER hot-1 / UNDER hot+1),
# which is the NARROWEST winning zone of every barrier choice that still
# keeps hot inside and cold outside — e.g. hot=9 -> OVER 8, a contract
# that only wins on a single digit (10% of ticks). Signal 2 additionally
# used a FIXED 30%-win DIGITUNDER 3, taken uncritically whenever it fired
# and signal 1 hadn't. Neither signal's "score" (hot/cold frequency gap,
# distance below the SMA) is actually a probability of winning — score
# was being used as if it were one.
#
# For a Deriv digit Over/Under contract, the win probability is a fact of
# arithmetic, not a read on the market: DIGITOVER(b) wins on digits > b,
# i.e. P = (9-b)/10; DIGITUNDER(b) wins on digits < b, i.e. P = b/10 —
# true regardless of any hot/cold/trend read, as long as the underlying
# last digit is close to uniformly distributed (true by design on these
# synthetic indices). Two changes now apply:
#   1. DONKEY_WIDEST_ZONE: signal 1's barrier is chosen to be the WIDEST
#      zone that still keeps hot inside / cold outside (barrier = cold,
#      not hot±1) — this alone raises the ceiling from ~10-50% to 50-90%
#      depending on where the cold digit lands.
#   2. DONKEY_MIN_WIN_PROB: a hard floor — evaluate_donkey_strategy() now
#      computes both signals' actual P(win) and REJECTS the trade
#      entirely (no entry that tick) unless the best one clears this bar,
#      taking the higher-P(win) signal when both qualify instead of
#      always preferring signal 1.
#
# ── IMPORTANT — this does NOT create a bigger edge, it reshapes the bet ──
# Deriv prices Over/Under payouts off this same win probability (minus
# its house edge), so a 90%-win contract pays out only slightly more than
# stake per win, while the ~10-20%-win contracts the bot was taking
# before pay out several times stake per win. Raising DONKEY_MIN_WIN_PROB
# trades "loses often, wins big" for "wins often, wins small" — it does
# NOT change the long-run expected value of a single bet, which stays
# governed by Deriv's built-in edge either way. If most of the account's
# actual profit has been coming from the rare big wins on those
# low-probability barriers, pushing win rate this high can shrink realised
# profit even though the strategy now "wins" far more often. Back-test /
# paper-trade this before pointing it at a funded account, and treat
# DONKEY_MIN_WIN_PROB as a dial, not a one-way switch — lower it (e.g.
# 0.60-0.70) for a middle ground between win frequency and payout size.
DONKEY_WIDEST_ZONE   = True    # False restores the old hot±1 barrier.
                                # Only used when DONKEY_MIN_PAYOUT_RATIO
                                # below is unset/0.
# ── MAX-WIN-PROBABILITY MODE (Sep 28 2026, chat-requested — trade very
# rarely, but only at the highest win probability the bot can reach) ────
# 0.90 is not a chosen target, it's a HARD CEILING: on a single-tick Deriv
# digit contract (Over/Under/Match/Differ) with a uniform last digit, the
# best any barrier can ever score is 9/10 — e.g. DIGITOVER(0) loses only
# if the digit is exactly 0, DIGITUNDER(9) loses only if it's exactly 9.
# No barrier choice, scoring tweak, or combination of signals gets a
# single such contract above 90% — there is no "closer to 1" available
# from this contract family. Setting the floor here AT that ceiling means
# only the rare tick where the widest-zone barrier lands on 0.90 (cold
# digit reads exactly 0 or 9) clears it — simulated: ~10.3% of ticks
# (vs. ~19% at a 0.80 floor), win rate 0.900 exactly, on 4M synthetic
# ticks. Lower this (e.g. 0.80-0.85) for more trades at a still-high but
# less extreme win rate.
DONKEY_MIN_WIN_PROB  = 0.90    # 0.0-1.0, capped at 0.90 by contract math.
                                # Only used when DONKEY_MIN_PAYOUT_RATIO
                                # below is unset/0.
DONKEY_MIN_SCORE     = 0.10    # signal-1 hot/cold gap floor (noise guard)
DONKEY_TREND_MIN_SCORE = 0.20  # signal-2 below-SMA distance floor

# ── SIGNIFICANCE GATE (Sep 28 2026, chat-requested) ──────────────────────
# Extra filter on top of whichever mode above is active (payout-ratio or
# max-win-probability) — NOT a way to exceed either one's guarantees. Only
# lets signal 1 fire when a chi-square test says the observed hot/cold gap
# is unlikely (p < DONKEY_SIGNIFICANCE_ALPHA) to be pure chance, using the
# same exact test _digit_hybrid_check() elsewhere in this file already
# uses. This is a noise filter, not an edge detector: on a genuinely fair
# digit stream (which these synthetic indices are built to be) it can
# ONLY reduce trade frequency, never raise the realised win rate above
# what DONKEY_MIN_WIN_PROB / DONKEY_MIN_PAYOUT_RATIO already guarantee by
# construction. Simulated: alpha=0.01 cuts trade frequency ~63% and
# leaves win rate exactly where it was without this gate (0.2817 vs
# 0.2817, 4M synthetic ticks) — confirming there's no hidden edge for it
# to surface, only weaker reads for it to filter out.
DONKEY_SIGNIFICANCE_ENABLED = True
DONKEY_SIGNIFICANCE_ALPHA   = 0.01   # require p < this to trade at all;
                                      # lower (e.g. 0.001) = rarer, more
                                      # demanding; higher (e.g. 0.05) =
                                      # more frequent, less demanding

# ── DONKEY PAYOUT-RATIO MODE (Sep 28 2026, chat-requested) ──────────────
# Supersedes DONKEY_WIDEST_ZONE / DONKEY_MIN_WIN_PROB above whenever set
# (>0) — this is now the primary knob. Request was: win rate as low as
# ~30% is fine, but every trade must pay out AT LEAST this many times the
# stake, AND win rate should be the highest possible subject to that.
#
# Deriv fair-prices Over/Under payout as payout_ratio = (1-P)/P, so a
# payout floor is exactly a win-probability CEILING: P_max = 1/(1+ratio).
# For ratio=2.0 that's P_max = 1/3 -> the nearest barrier at-or-under that
# ceiling is P=0.30 (OVER 6 / UNDER 3, payout ~2.33x stake) — the highest
# win rate obtainable without breaking the 2x payout promise. Both Donkey
# signals now solve for that same barrier (see signal_engine.py's
# _donkey_payout_target_barrier() / _donkey_trend_barrier()); a tick whose
# hot/cold read can't reach a barrier satisfying BOTH the payout floor and
# the contrarian constraint is skipped rather than taking a trade that
# breaks the payout guarantee — so this also throttles trade frequency
# versus the old always-fires behavior (simulated: ~27% of ticks, 28.4%
# empirical win rate, ~2.5x realised payout ratio, on 4M synthetic ticks).
#
# Set to 0 or None to go back to the win-rate-maximizing mode above
# (DONKEY_WIDEST_ZONE / DONKEY_MIN_WIN_PROB) with no payout floor at all.
DONKEY_MIN_PAYOUT_RATIO = 2.0   # ON again (Sep 28 2026, chat-requested:
                                # highest win rate achievable while payout
                                # stays > 2x stake). Overrides
                                # DONKEY_MIN_WIN_PROB/DONKEY_WIDEST_ZONE
                                # above entirely while set > 0 — see the
                                # PAYOUT-RATIO MODE block below for the
                                # math. Max win rate achievable under a 2x
                                # payout floor is 1/3 (33%); can't be
                                # pushed higher without lowering this.

# ── Donkey guard (donkey_guard.py) — LIMITS losses, does not create edge ──
# Stops are in units of your fixed stake, so they scale with MANUAL_STAKE_AMOUNT.
# State is saved at the end of every leg (fixed_cycle.enter_cooldown_now) so
# it survives the redeploy between legs.
DONKEY_GUARD_ENABLED                     = True
DONKEY_GUARD_SESSION_STOP_LOSS_STAKES    = 15   # halt after net -15 stakes
DONKEY_GUARD_SESSION_TAKE_PROFIT_STAKES  = 10   # halt after net +10 stakes
DONKEY_GUARD_HALT_MINS                   = 120  # halt length, then a fresh session
DONKEY_GUARD_CONSEC_LOSS_LIMIT           = 8    # losses in a row before pausing
DONKEY_GUARD_CONSEC_LOSS_PAUSE_MINS      = 30
DONKEY_GUARD_MAX_TRADES_PER_HOUR         = 60   # 0 disables the cap
