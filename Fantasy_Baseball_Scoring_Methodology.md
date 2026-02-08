# Fantasy Baseball Team Scoring Methodology

## Overview

This document explains the comprehensive scoring system used to evaluate fantasy baseball teams beyond traditional win-loss records. The methodology creates normalized scores (0-100) for each statistical category and combines them into an overall team performance score.

## Data Sources

### 1. Current Season Statistics
- **Batting Statistics**: All offensive categories including hits, runs, home runs, RBIs, stolen bases, batting average, OPS, etc.
- **Pitching Statistics**: All pitching categories including wins, saves, strikeouts, ERA, WHIP, innings pitched, etc.
- **Source**: Live data scraped from Yahoo Fantasy Baseball league pages

### 2. Head-to-Head Records
- Win-Loss-Draw records for each statistical category
- Overall league standings and rankings
- Games behind leader and recent moves

## Statistical Categories

### Batting Categories (37 total)
**High-Value Categories** (higher numbers = better performance):
- GP, GS_BAT, AB, R, H, 1B, 2B, 3B, HR, RBI, SH, SF, SB, BB, IBB, HBP, PO, A, FIELD, BA, OBP, SLG, OPS, EXBH, NSB, SB_PER, CYCLE, PA, GSHR, OA, DPT, CI

**Low-Value Categories** (lower numbers = better performance):
- CS (Caught Stealing), ERR (Errors), K_BAT (Strikeouts), GIDP (Grounded Into Double Play)

### Pitching Categories (51 total)
**High-Value Categories** (higher numbers = better performance):
- PITCH_APP, GS_PITCH, IP, W, CG, SO, SV, O, TBF, K_PITCH, SBA, GIDPF, SV_CHANCE, HLD, KBB, K9, PC, RW, POFF, RAPP, WIN_PER, NH, PG, SV_PER, QS, NSV, SVH, NSVH, NW

**Low-Value Categories** (lower numbers = better performance):
- L (Losses), H_PITCH (Hits Allowed), R_PITCH (Runs Allowed), ER (Earned Runs), HRA (Home Runs Allowed), BBA (Walks), IBBA (Intentional Walks), HB (Hit Batters), WP (Wild Pitches), BALK (Balks), TBA (Total Bases Allowed), ERA (Earned Run Average), WHIP (Walks + Hits per Inning), 1BA, 2BA, 3BA (Singles/Doubles/Triples Allowed), RL (Relief Losses), OBPA (On-Base Percentage Against), H9 (Hits per 9 Innings), B9 (Walks per 9 Innings), IR_SCORE (Inherited Runners Scored), BS (Blown Saves)

## Scoring Methodology

### Step 1: Data Collection
1. **Current Statistics**: Scrape current season totals for all statistical categories
2. **Rankings**: Calculate team rankings within each category
3. **Records**: Parse head-to-head win-loss records for each category

### Step 2: Normalization Process

#### For High-Value Categories (Higher = Better)
```
Score = ((Team_Value - League_Minimum) / (League_Maximum - League_Minimum)) × 100
```
- **Range**: 0-100 points
- **Best Team**: Gets 100 points
- **Worst Team**: Gets 0 points
- **Example**: If team has 150 HRs, league min is 100, league max is 200, then Score = ((150-100)/(200-100)) × 100 = 50.00

#### For Low-Value Categories (Lower = Better)
```
Score = 100 - ((Team_Value - League_Minimum) / (League_Maximum - League_Minimum)) × 100
```
- **Range**: 0-100 points (inverted)
- **Best Team** (lowest value): Gets 100 points
- **Worst Team** (highest value): Gets 0 points
- **Example**: If team has 3.50 ERA, league min is 3.00, league max is 5.00, then Score = 100 - ((3.50-3.00)/(5.00-3.00)) × 100 = 75.00

### Step 3: Total Score Calculation
```
Total_Score_Sum = Sum of all individual category scores
Total_Score_Rank = Ranking based on Total_Score_Sum (1 = highest total score)
```

### Step 4: Variation Analysis
```
Score_Variation = Total_Score_Rank - Current_League_Rank
```
- **Positive Value**: Team is underperforming relative to their statistical production
- **Negative Value**: Team is overperforming relative to their statistical production
- **Zero**: Team's rank matches their statistical performance

## Key Metrics Explained

### Individual Category Scores
- **Range**: 0.00 - 100.00 points
- **Precision**: Rounded to 2 decimal places
- **Interpretation**: Higher scores indicate better performance in that category

### Total Score Sum
- **Range**: Typically 2,000 - 6,000+ points (sum of ~88 categories)
- **Calculation**: Simple addition of all individual category scores
- **Purpose**: Overall measure of statistical dominance across all categories

### Power Rankings
- **Stats_Power_Rank**: Ranking based on statistical performance only
- **Variation**: Difference between statistical rank and actual league standing
- **Batting/Pitching Ranks**: Separate rankings for offensive and defensive performance

## Advantages of This Methodology

### 1. **Comprehensive Evaluation**
- Considers all statistical categories equally
- Accounts for both offensive and defensive performance
- Provides granular insight into team strengths/weaknesses

### 2. **Normalization Benefits**
- Eliminates scale differences between categories (HRs vs. ERA)
- Creates comparable scores across all statistics
- Handles both "higher is better" and "lower is better" categories

### 3. **Objective Analysis**
- Removes bias from traditional win-loss records
- Identifies teams that may be over/underperforming
- Provides data-driven insights for trades and roster decisions

### 4. **Actionable Insights**
- **High Total Score + Low Rank**: Team is unlucky, likely to improve
- **Low Total Score + High Rank**: Team is overperforming, may decline
- **Category-Specific Scores**: Identify specific areas for improvement

## Limitations and Considerations

### 1. **Equal Weighting**
- All categories weighted equally (may not reflect real-world importance)
- Some categories may be more valuable in specific league formats

### 2. **Context Independence**
- Doesn't account for league-specific scoring systems
- May not reflect head-to-head matchup dynamics

### 3. **Timing Factors**
- Based on cumulative season statistics
- Doesn't account for recent trends or momentum

## Example Interpretation

**Team A Results:**
- Total_Score_Sum: 4,250.75
- Total_Score_Rank: 3
- Current League Rank: 7
- Score_Variation: -4.00

**Interpretation:**
Team A has the 3rd-best statistical performance but is ranked 7th in the league. The negative variation (-4.00) suggests they are significantly underperforming relative to their statistical production, possibly due to bad luck in head-to-head matchups or poor timing of performances.

## Technical Implementation

### Tools Used
- **Python pandas**: Data manipulation and analysis
- **scikit-learn MinMaxScaler**: Normalization calculations
- **BeautifulSoup**: Web scraping for live data
- **AWS DynamoDB**: Data storage and historical tracking

### Output Format
- **CSV Export**: Comprehensive spreadsheet with all metrics
- **Precision**: All scores rounded to 2 decimal places
- **Sorting**: Teams ranked by Total_Score_Sum (highest to lowest)

---

*This methodology provides a comprehensive, objective evaluation of fantasy baseball team performance that goes beyond traditional win-loss records to identify statistical trends and potential opportunities.*