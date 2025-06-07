
#------> Data Cleaning in Sql

SELECT *
FROM layoffs;

#1. Remove duplicate data
#2. Standarize the data
#3. null values and blank values
#4.remove unnecessary columns


CREATE TABLE layoffs_stagging
LIKE layoffs;

SELECT * FROM layoffs_stagging;

INSERT INTO layoffs_stagging
SELECT * FROM layoffs;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date'
) AS row_num
FROM layoffs_stagging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging
) 
DELETE
FROM duplicate_cte
WHERE row_num>1;


SELECT * FROM layoffs_stagging
WHERE company='casper';

CREATE TABLE layoffs_stagging2(
company text, 
location text, 
industry text ,
total_laid_off int ,
percentage_laid_off text,
date text ,
stage text ,
country text, 
funds_raised_millions int,
row_num INT
);

INSERT INTO layoffs_stagging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;


DELETE FROM layoffs_stagging2
WHERE row_num>1;

SET SQL_SAFE_UPDATES =1;

SELECT * FROM layoffs_stagging2
WHERE row_num>1;


SELECT COUNT(*) FROM layoffs_stagging2;

#---- standarize the data

SELECT company, TRIM(company)
FROM layoffs_stagging2;

SET SQL_SAFE_UPDATES=0;

UPDATE layoffs_stagging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_stagging2
ORDER BY 1;

SELECT * 
FROM layoffs_stagging
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location 
FROM layoffs_stagging
ORDER BY 1;

SELECT *
FROM layoffs_stagging2
WHERE country LIKE 'United States'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_stagging2
ORDER BY 1;

UPDATE layoffs_stagging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States';


SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');


ALTER TABLE layoffs_stagging2
MODIFY COLUMN date DATE;


SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULl
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL
OR industry ='';

SELECT *
FROM layoffs_stagging2
WHERE company ='Airbnb';


SELECT t1.industry,t2.industry
FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
ON t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;


UPDATE layoffs_stagging2
SET industry ='	NULL'
WHERE industry ='';

SET SQL_SAFE_UPDATES=0;


UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off is NULL;

DELETE
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off is NULL;

ALTER TABLE layoffs_stagging2
DROP COLUMN row_num;

SELECT * FROM layoffs_stagging2;

##---Exploratory Data Analysis

SELECT MAX(total_laid_off)
FROM layoffs_stagging2;

SELECT location, percentage_laid_off
FROM layoffs_stagging2
WHERE percentage_laid_off= 1;

SELECT company, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(date), MAX(date)
FROM layoffs_stagging2;

SELECT industry, SUM(total_laid_off) AS total
FROM layoffs_stagging2
GROUP BY industry
ORDER BY total DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY country
ORDER BY 2 DESC
LIMIT 5;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY YEAR(date)
ORDER BY 2 DESC; 

SELECT stage, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY stage
ORDER BY 2 DESC; 

SELECT company, AVG(percentage_laid_off)
FROM layoffs_stagging
GROUP BY company
ORDER BY 2 DESC;

SELECT DAYNAME(date), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY DAYNAME(date)
ORDER BY 2 DESC;

SELECT MONTH(date), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY MONTH(date);

SELECT YEAR(date) AS year, MONTH(date) AS month, SUM(total_laid_off) AS total_off
FROM layoffs_stagging2
WHERE date IS NOT NULL
GROUP BY year, month
ORDER BY 1 ASC;


WITH rolling_data AS(
SELECT YEAR(date) AS year, MONTH(date) AS month, SUM(total_laid_off) AS total_off
FROM layoffs_stagging2
WHERE date IS NOT NULL
GROUP BY year, month
ORDER BY 1 ASC
) 
SELECT year, month, total_off, SUM(total_off) OVER(ORDER BY year, month)
FROM rolling_data;

-- complete exploratory data analysis and also complete work 
-- Thank You 




