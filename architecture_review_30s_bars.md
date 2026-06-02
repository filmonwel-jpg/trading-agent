# Architectural Review: 30s Bars vs Event-Driven Tick Models

## Validity of the 30s Bar Paradigm

Using strict 30-second bars for evaluating entries and exits in a high-frequency trading context (like TSLA options or stock ping-ponging) exposes the system to several structural blind spots:

1. **Information Loss (Smearing):** Within 30 seconds, critical microstructure events such as an aggressive institutional sweep (L1 book imbalance skewing suddenly) or a sweep of the tape will occur within milliseconds. A 30s bar averages these out or captures them only after the move has happened.
2. **Lag in Reaction:** If a breakdown starts at second 2 of a 30s bar, the model waits until second 30 to close and trigger an evaluation. By then, the critical exit or entry price is gone. 
3. **Volatility Distortions:** The frequency of the market varies. In high volatility (e.g., at the Open), hundreds of trades might happen in 5 seconds. In low volatility (midday), passing a 30s bar might hold no new information. 

**Recommendation: Shift to Tick-Driven or Volume-Driven Data**
Instead of `train_30s_models.py` strictly processing chronological seconds, switch to **Volume Bars** or **Tick Bars** (e.g., every 5,000 shares traded, or every 100 consolidated tape prints). Alternatively, use **Event-Driven Appraisals** where standard continuous bars are kept for context, but sudden micro-structure signals (L1 imbalance, large bid/ask spread collapse) force an out-of-band evaluation.

## Correlating Exits with Entry Theses

Right now, the exit labels (`Label_Long_Exit`, `Label_Short_Exit`) are trained as standalone top/bottom detectors. They have their own fixed percentage targets (`EXIT_DROP_PCT = 0.0020`). 

**The Flaw:**
If a Long Entry was taken because the model identified an "exhaustion pullback" against a key SMA, the exit condition should be evaluated relative to whether that SMA bounces successfully. If the bot exits randomly because a standalone exit model flagged a tiny 0.2% drop, it might miss the actual target.

**How to adjust (The Solution):**
1. **Thesis-Tracking in Production (`PingPongStrategy.java`):**
   When triggering a long entry, the strategy should record the primary features that caused the entry. If the order was a dip-buy near the VWAP, the exit should be coupled to the VWAP distance.
2. **Conditional Exit Models in Training (`train_30s_models.py`):**
   Instead of generating raw exit paths, exit models should be trained on the *state of an active position*.
   In machine learning terms, an exit model should answer: *"Given we entered at X, and PnL is currently Y%, will the price hit -0.2% before it hits +0.5%?"*
   We would add a feature to the dataset: `f_current_position_pnl`, and generate labels that predict the next optimal action (HOLD or CLOSE) rather than raw uncoupled tops/bottoms.

## Immediate Changes Applied
In the training file `train_30s_models.py`:
- We adjusted entry requirements to demand a stronger reward-to-risk ratio (1.4R, `+0.35%` profit vs `-0.25%` risk).
- We stretched the `FUTURE_WINDOW_BARS` to 20 (equivalent to 10 minutes) to give trades more time to play out their entry thesis, avoiding premature aborts from the AI generating a flat label just because the 30s timeframe wasn't enough.
- We modified exit thresholds to act as earlier risk alerts (`-0.10%`) to trap failures before they hit the hard stop loss.
