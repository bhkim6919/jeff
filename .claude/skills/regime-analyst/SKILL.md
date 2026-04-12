---
name: regime-analyst
description: Analyze Q-TRON regime prediction accuracy, compare predicted vs actual regimes, validate EMA smoothing and axis score distributions across KR and US markets.
user_invocable: true
command: regime-analyst
---

# Q-TRON Regime Analyst

Analyze and validate the regime prediction system across KR (domestic) and US markets. Checks prediction accuracy, EMA smoothing, score distributions, and regime transition behavior.

---

## Invocation

```
/regime-analyst                   Full regime analysis (all checks)
/regime-analyst accuracy          Predicted vs actual regime comparison
/regime-analyst distribution      Axis score distribution analysis
/regime-analyst smoothing         EMA smoothing effectiveness check
/regime-analyst transitions       Regime transition frequency and stability
/regime-analyst kr                KR market regime only
/regime-analyst us                US market regime only
```

---

## Key File Paths

### KR Market (Gen04-REST)
- **Predictor**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/predictor.py`
- **Scorer**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/scorer.py`
- **Actual regime**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/actual.py`
- **Models**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/models.py`
- **Feature builder**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/feature_builder.py`
- **Storage**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/storage.py`
- **Calendar**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/calendar.py`
- **Domestic collector**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/collector_domestic.py`
- **Global collector**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/collector_global.py`
- **Theme regime**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/theme_regime.py`
- **Regime detector (strategy)**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/strategy/regime_detector.py`
- **Regime backtester**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/backtest/backtester_regime.py`
- **Regime backtester v3**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/backtest/backtester_regime_v3.py`
- **API endpoint**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/api.py`

### US Market (Gen04-US)
- **Predictor**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/regime/predictor.py`
- **Actual regime**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/regime/actual.py`
- **Models**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/regime/models.py`
- **Collector**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/regime/collector.py`

### Config
- **KR config**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/config.py` (regime parameters)
- **US config**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/config.py` (regime parameters)

---

## Analysis Procedure

### Phase 1: Regime Model Architecture Review

1. **Read `models.py`** (both KR and US) to understand:
   - Regime enum definitions (BULL, BEAR, SIDEWAYS, etc.)
   - Axis score structure (what axes exist, their ranges)
   - Threshold definitions for regime classification

2. **Read `predictor.py`** to understand:
   - Input features used for prediction
   - EMA smoothing parameters (span, decay)
   - Prediction logic (rule-based vs ML)
   - Confidence scoring mechanism

3. **Read `scorer.py`** (KR) to understand:
   - How raw signals are converted to axis scores
   - Score normalization method
   - Weight assignment across axes

4. **Document the model**:
   ```
   REGIME MODEL ARCHITECTURE
   Market: [KR/US]
   Axes:   [list of axis names and ranges]
   Regimes: [BULL/BEAR/SIDEWAYS/...]
   Thresholds: [axis -> regime mapping rules]
   EMA span: [value]
   Features: [list of input features]
   ```

### Phase 2: Prediction Accuracy Analysis

1. **Read `actual.py`** to understand how "actual" regime is determined:
   - What defines ground-truth regime (realized returns, volatility, etc.)
   - Lookback window for actual regime classification
   - Whether actual regime is forward-looking (which would be invalid for live use)

2. **Compare predicted vs actual**:
   - If historical prediction logs exist, compute confusion matrix
   - Calculate accuracy, precision, recall per regime class
   - Identify systematic biases (e.g., always predicts BULL)
   - Check if predictor leads or lags actual regime transitions

3. **Transition analysis**:
   - Count regime transitions per month (too many = noisy, too few = lagging)
   - Ideal: 1-3 transitions per quarter
   - Check for oscillation (BULL->BEAR->BULL within days = unstable)

4. **Report format**:
   ```
   REGIME PREDICTION ACCURACY
   Period: YYYY-MM-DD to YYYY-MM-DD
   
   Confusion Matrix:
                Actual BULL  Actual BEAR  Actual SIDE
   Pred BULL       NN           NN           NN
   Pred BEAR       NN           NN           NN
   Pred SIDE       NN           NN           NN
   
   Accuracy:  XX.X%
   BULL precision/recall: XX.X% / XX.X%
   BEAR precision/recall: XX.X% / XX.X%
   
   Transition frequency: X.X per month
   Oscillation events: NN (regime flip within 5 days)
   ```

### Phase 3: EMA Smoothing Effectiveness

1. **Extract EMA parameters** from predictor.py:
   - Span/half-life for each axis
   - Whether smoothing is applied before or after scoring

2. **Evaluate smoothing quality**:
   - Compare raw scores vs smoothed scores (if both available)
   - Check if smoothing eliminates noise without excessive lag
   - Identify if smoothing causes regime calls to be consistently late

3. **Smoothing indicators**:
   - Score autocorrelation (should be moderate: 0.3-0.7)
   - Score volatility ratio (smoothed/raw < 0.5 = over-smoothed, > 0.9 = under-smoothed)
   - Lag estimation: how many days after a real regime change does the smoothed score cross threshold?

4. **Report format**:
   ```
   EMA SMOOTHING ANALYSIS
   Axis          EMA Span    Raw StdDev    Smoothed StdDev    Ratio    Verdict
   [axis1]       NN          X.XX          X.XX               X.XX     [OK/OVER/UNDER]
   [axis2]       NN          X.XX          X.XX               X.XX     [OK/OVER/UNDER]
   
   Estimated regime detection lag: X.X days
   ```

### Phase 4: Axis Score Distribution Analysis

1. **Examine score distributions**:
   - Read feature_builder.py and scorer.py to understand score construction
   - Check for score clustering (all scores near threshold = fragile regime calls)
   - Verify scores use full range (not bunched in one area)
   - Check for outliers or extreme values

2. **Distribution health checks**:
   - Skewness: should be near 0 (symmetric)
   - Kurtosis: moderate (not too peaked, not too flat)
   - Threshold proximity: what % of scores fall within 0.1 of regime boundary?
   - Score correlation across axes (high correlation = redundant axes)

3. **Report format**:
   ```
   AXIS SCORE DISTRIBUTIONS
   Axis       Mean    StdDev    Skew    Kurt    Near-Threshold%    Verdict
   [axis1]    X.XX    X.XX      X.XX    X.XX    XX.X%              [HEALTHY/FRAGILE/BIASED]
   [axis2]    X.XX    X.XX      X.XX    X.XX    XX.X%              [HEALTHY/FRAGILE/BIASED]
   
   Cross-axis correlation:
   [axis1] x [axis2]: X.XX [INDEPENDENT/REDUNDANT]
   ```

### Phase 5: Regime Impact on Strategy

1. **Check regime integration**:
   - Read `regime_detector.py` (KR) for how regime affects trading
   - Gen04 confirmed decision: regime NOT used for position sizing (BEAR still +23.3%)
   - Verify regime is informational only, not blocking trades incorrectly
   - Check if Strategy Lab uses regime for strategy selection

2. **Backtest by regime**:
   - If regime backtester results exist, summarize performance per regime
   - Confirm strategy is robust across all regimes (not regime-dependent)

---

## Summary Report Format

```
REGIME ANALYSIS SUMMARY
============================================================
Check                         KR Status    US Status    Notes
------------------------------------------------------------
Model architecture reviewed   [OK/ISSUE]   [OK/ISSUE]   ...
Prediction accuracy           XX.X%        XX.X%        ...
EMA smoothing quality         [OK/OVER/UNDER]  ...      ...
Score distributions           [HEALTHY/FRAGILE] ...     ...
Transition stability          [STABLE/NOISY]   ...      ...
Strategy impact               [INFO-ONLY/BLOCKING]      ...
============================================================
```

---

## Safety Rules

- **Read-only analysis** -- never modify regime model parameters
- Regime is currently informational for Gen04 core strategy (do not change this)
- If regime data collectors fail, note it but do not retry API calls
- Historical regime data may be stored in DB (PostgreSQL) -- use read-only queries only
