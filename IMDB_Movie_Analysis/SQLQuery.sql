-- 1. Standardize 'Runtime' and convert to INT for calculation
UPDATE imdb_top_1000_movies
SET Runtime = REPLACE(Runtime, ' min', '');

ALTER TABLE imdb_top_1000_movies
MODIFY COLUMN Runtime INT;

-- 2. Handle Missing Financial Data (Impute with Market Average)
UPDATE imdb_top_1000_movies AS t1
JOIN (SELECT AVG(Gross) AS avg_gross FROM imdb_top_1000_movies) AS t2
SET t1.Gross = t2.avg_gross
WHERE t1.Gross IS NULL;

# Insight 1: Genre Profitability (ROI Analysis)
SELECT 
    TRIM(SUBSTRING_INDEX(Genre, ',', 1)) AS Main_Genre,
    COUNT(*) as Movie_Count,
    -- Format huge numbers for readability (Millions)
    CONCAT('$', ROUND(AVG(Gross)/1000000, 2), 'M') as Avg_Box_Office,
    ROUND(AVG(IMDB_Rating), 2) as Avg_Rating,
    -- Rank genres by commercial success
    DENSE_RANK() OVER (ORDER BY AVG(Gross) DESC) as Revenue_Rank
FROM imdb_top_1000_movies
GROUP BY Main_Genre
ORDER BY Revenue_Rank ASC
LIMIT 10;

# Insight 2: Market Segmentation (Box Office Tiers)
SELECT 
    Series_Title,
    Released_Year,
    CONCAT('$', ROUND(Gross/1000000, 2), 'M') as Gross_Revenue,
    -- Segmentation Logic
    CASE 
        WHEN Gross >= 300000000 THEN '🦄 Global Blockbuster (>300M)' 
        WHEN Gross BETWEEN 100000000 AND 300000000 THEN '🔥 Mainstream Hit (100M-300M)'
        WHEN Gross BETWEEN 50000000 AND 100000000 THEN '✅ Mid-Tier Success (50M-100M)'
        ELSE '🎬 Niche/Indie (<50M)'
    END as Market_Tier
FROM imdb_top_1000_movies
ORDER BY Gross DESC
LIMIT 15;

#Insight 3: Era Evolution (Trend Analysis)
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
WHERE Released_Year REGEXP '^[0-9]+$' -- Filter out non-numeric years
GROUP BY Cinema_Era
ORDER BY Avg_Rating DESC;