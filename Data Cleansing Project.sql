-- Data Cleaning Use SQL

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022



SELECT * 
FROM world_layoffs.layoffs;


-- the Step that was used for this project is below :
-- 1. Remove Duplicates
-- 2. Standarize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns


-- first off all is create a staging table. we want to keep original raw data to avoid any problems 
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

--  1. Remove Duplicate

-- we want to check any dulpicate number by adding row number
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- if there are any row numbers greater than 1, it is means that there are is a duplicate table


-- to delete duplicate number, we cannot delete from layoffs_staging table. so we need to create one table that has row table value  


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INTEGER
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;


-- now we can delete row number greater than 1
DELETE  
FROM layoffs_staging2
WHERE row_num > 1;

-- check again to make sure all the duplicate table was deleted
SELECT *
FROM layoffs_staging2;

-- 2. Standarizing data

-- start by checking company name, if there are any space in first name we must delete it.
SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


-- in industry column there are multiple variation of Crypto. let's make standarize by making all to Crypto
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- in country column has multiple variations of United States. so we standarize by making all United States
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- in the date column in default by raw data showed the date's format is text, we want to change it to date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- double check that all the standardization was implemented well
SELECT*
FROM layoffs_staging2;


-- 3. NULL VALUS AND BLANK VALUES

-- we check the table if for any null and blank values 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT  *
FROM layoffs_staging2
WHERE industry IS NULL
OR  industry = '';

-- we update the industry column cause in same company there are null and blank values for their industry
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

-- make sure again
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


-- delete row for total laid off and percentage laid off that has  NULL values for both
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;


-- 4. Remove rows that we don't need


-- Delete row
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
