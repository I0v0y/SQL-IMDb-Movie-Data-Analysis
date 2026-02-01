# 🎬 IMDb Movie Data Analytics: End-to-End BI Project

## 📖 Executive Summary
This project explores the profitability and trends of the top 1,000 rated movies. By building an ETL pipeline with **Python** and executing advanced queries in **MySQL**, I uncovered key insights regarding genre ROI and market segmentation.

**Key Tech Stack:**
* **Python (Pandas)**: Data cleaning & ETL automation.
* **MySQL**: Relational database storage.
* **SQL Techniques**: `Window Functions`, `CTEs`, `Complex CASE WHEN`, `Indexing`.

---

## 📊 Business Insights & Visualizations

### 1. Genre ROI Analysis (High-Yield Content)
**Business Question:** *Which genres provide the best balance of volume and revenue?*

Using **Window Functions** (`DENSE_RANK`), I ranked genres by their average box office performance.
* **Insight**: Adventure and Action movies consistently top the revenue charts, acting as "Cash Cows" despite high competition.

> **SQL Snippet:**
```sql
SELECT 
    TRIM(SUBSTRING_INDEX(Genre, ',', 1)) AS Main_Genre,
    CONCAT('$', ROUND(AVG(Gross)/1000000, 2), 'M') as Avg_Box_Office,
    DENSE_RANK() OVER (ORDER BY AVG(Gross) DESC) as Revenue_Rank
FROM imdb_top_1000_movies
GROUP BY Main_Genre
LIMIT 5;
```
### 👇 Dashboard Result:
| Main_Genre | Movie_Count | Avg_Box_Office | Avg_Rating | Revenue_Rank |
| :--- | :--- | :--- | :--- | :--- |
| Family | 2 | $219.56M | 7.8 | 1 |
| Action | 172 | $128.64M | 7.95 | 2 |
| Animation | 82 | $117.00M | 7.93 | 3 |
| Adventure | 72 | $83.64M | 7.94 | 4 |
| Horror | 11 | $73.08M | 7.91 | 5 |
| Fantasy | 2 | $68.03M | 8 | 6 |
| Biography | 88 | $60.94M | 7.94 | 7 |
| Drama | 289 | $44.26M | 7.96 | 8 |
| Mystery | 12 | $39.84M | 7.97 | 9 |
| Comedy | 155 | $38.72M | 7.9 | 10 |
<br>

### 2. Market Segmentation (Box Office Tiers)
**Business Question:** *How is the movie market stratified?*

I applied complex `CASE WHEN` logic to categorize movies into strategic tiers: **Global Blockbusters** (>300M) vs. **Niche Hits** (<50M).
* **Insight**: The market follows a "Winner-Takes-All" distribution. Blockbusters account for the majority of revenue, while "Niche/Indie" films rely on critical acclaim (High Ratings) rather than Box Office.

> **SQL Logic:**
```sql
SELECT 
    Series_Title, 
    Released_Year, 
    CONCAT('$', ROUND(Gross/1000000, 2), 'M') as Gross_Revenue,
    CASE 
        WHEN Gross >= 300000000 THEN '🦄 Global Blockbuster (>300M)' 
        WHEN Gross BETWEEN 100000000 AND 300000000 THEN '🔥 Mainstream Hit (100M-300M)'
        ELSE '🎬 Niche/Indie (<50M)'
    END as Market_Tier
FROM imdb_top_1000_movies
ORDER BY Gross DESC
LIMIT 5;
```
### 👇 Dashboard Result:
| Series_Title | Released_Year | Gross_Revenue | Market_Tier |
| :--- | :--- | :--- | :--- |
| Star Wars: Episode VII - The Force Awakens | 2015 | $936.66M | 🦄 Global Blockbuster (>30) |
| Avengers: Endgame | 2019 | $858.37M | 🦄 Global Blockbuster (>30) |
| Avatar | 2009 | $760.51M | 🦄 Global Blockbuster (>30) |
| Avengers: Infinity War | 2018 | $678.82M | 🦄 Global Blockbuster (>30) |
| Titanic | 1997 | $659.33M | 🦄 Global Blockbuster (>30) |
| The Avengers | 2012 | $623.28M | 🦄 Global Blockbuster (>30) |
<br>

### 3. Historical Trend Analysis
**Business Question:** *Are modern movies sacrificing quality for revenue?*

* **Observation**:
    * **Revenue**: The "Digital Era" (Post-2000) has the highest average revenue due to inflation and global market expansion.
    * **Quality**: The "Classic Era" (Pre-1980) maintains the highest average IMDB Ratings, indicating stronger long-term audience retention and "Cult Classic" potential.

> **SQL Logic:**
```sql
SELECT 
    CASE 
        WHEN Released_Year < 1980 THEN 'Classic Era (Pre-1980)'
        WHEN Released_Year BETWEEN 1980 AND 2000 THEN 'VHS/DVD Era (1980-2000)'
        ELSE 'Digital Era (Post-2000)'
    END as Cinema_Era,
    COUNT(*) as Total_Movies,
    ROUND(AVG(IMDB_Rating), 2) as Avg_Rating,
    CONCAT('$', ROUND(AVG(Gross)/1000000, 2), 'M') as Avg_Revenue
FROM imdb_top_1000_movies
WHERE Released_Year REGEXP '^[0-9]+$'
GROUP BY Cinema_Era
ORDER BY Avg_Rating DESC;
```
### 👇 Dashboard Result:
| Era | Count | Score | Revenue |
| :--- | :--- | :--- | :--- |
| Classic Era (Pre-1980) | 275 | 8 | $43.61M |
| VHS/DVD Era (1980-2000) | 258 | 7.96 | $61.66M |
| Digital Era (Post-2000) | 466 | 7.91 | $85.75M |
