/*
Data Cleaning project using SQL. 

We have a data set of layoff data of the companies and need to clean the data using SQL before using it to the exploratory data analysis. 

We are using PostgreSQL to work in this project. The steps we are going to follow are:
1. Removing duplicates rows 
2. Standardizing data (removing the typing errors)
3. Handling Null values 	
4. Removing unnecessary columns

*/

select * from layoffs;

SET datestyle = mdy;

COPY layoffs 
FROM 'C:\Users\abkhairn2201\Documents\New Folder\The Analyst\Scalar\SQL\Layoffs\layoffs1.csv' 
DELIMITER ',' 
CSV HEADER;

select * from layoffs;



-- creating a copy table from existing table 
create table layoffs_staging as table layoffs;

select * from layoffs_staging;

-- 1. Removing duplicates

select *, row_number()over(partition by company, 'location', industry, total_laid_off, percentage_laid_off, 'date') as row_num 
from layoffs_staging;

-- method 1 for identifying duplicates
with duplicate_cte as(
select *, row_number()over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num 
from layoffs_staging
)

Select * 
from duplicate_cte
where row_num > 1
order by company;


-- method 2 
select *, count(*) as cnt
from layoffs_staging
group by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
having count(*)>1
order by company;

-- deleting the rows using delete function does not happen as SQL does not allow us to update/delete table using a cte function. 
with duplicate_cte as(
select *, row_number()over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num 
from layoffs_staging
)

delete 
from duplicate_cte
where row_num > 1;

-- need to create a new table for removing duplicates

create table layoffs_staging2 as 
	select *, row_number()over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num 
from layoffs_staging;

select * from layoffs_staging2;

delete from layoffs_staging2 
where row_num >1;

select * from layoffs_staging2;

-- no duplicate rows present 
select *, count(*) as cnt
from layoffs_staging2
group by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num
having count(*)>1
order by company


--  2. Standardization of data

-- triming the blank spaces in columns 

select company, trim(company) from layoffs_staging2;

-- here we can see the row 2 has a blank space before the comapny name, need to trim and update it. 

update layoffs_staging2 set company = trim(company);

-- here we have updated the trimmed company names. 

--  checking for similar categories, let's check industry column

select distinct industry from layoffs_staging2 order by 1;

 -- here we can see the industries - Crypto, Crypto Currency and CryptoCurrency are same and we need to merge into one category
 
-- method 1 using IN operator 
 update layoffs_staging2 
 set industry = 'Crypto'
 where industry in ('Crypto', 'Crypto Currency', 'CryptoCurrency');
 
-- Using wildcards (I avoid wildcards as its not the best practice)
update layoffs_staging2 
 set industry = 'Crypto'
 where industry like 'Crypto%';
 
-- lets' check other columns for such issues

select distinct location from layoffs_staging2 order by 1;

-- looks like "Düsseldorf" & "Dusseldorf" / "Malmö"& "Malmo" are same locations

update layoffs_staging2 
 set location = 'Dusseldorf'
 where location like 'Düsseldorf';
 
 update layoffs_staging2 
 set location = 'Malmo'
 where location like 'Malm%';
 
 select distinct location from layoffs_staging2 order by 1;


-- check for country (United States & United States. are same)

select distinct country from layoffs_staging2 order by 1;

 update layoffs_staging2 
 set country = 'United States'
 where country like 'United States%';
 
--  handling null values

Select * from layoffs_staging2 where total_laid_off is null;

Select * from layoffs_staging2 where percentage_laid_off is null;


-- let's start with industry blank/ null values -- as no location and country rows are null
Select * from layoffs_staging2
where company = 'Airbnb'

Select st1.company, st1.industry, st2.industry from layoffs_staging2 st1
	join layoffs_staging2 st2 on st1.company = st2.company
where st1.industry is null; 

Select st1.company, st1.industry, st2.industry from layoffs_staging2 st1
	join layoffs_staging2 st2 on st1.company = st2.company
where st1.industry is null and st2.industry is not null;

-- updating the null industry using other value of the same company

Update layoffs_staging2 as st1
Set industry = st2.industry
from layoffs_staging2 as st2 
Where st1.company = st2.company and (st1.industry is null and st2.industry is not null); 

Select st1.company, st1.industry, st2.industry from layoffs_staging2 st1
	join layoffs_staging2 st2 on st1.company = st2.company
where st1.industry is null;


-- there are lot of rows where both percentage_laid_off & total_laid_off columns has nulls. 
-- for laid off analysis we, can not use these rows. Need to delete the rows. (deleting data is tricky, need to be 100% sure)

Select * from layoffs_staging2 where percentage_laid_off is null and total_laid_off is null;

delete from layoffs_staging2 where percentage_laid_off is null and total_laid_off is null; -- 361 rows deleted

Select * from layoffs_staging2;

-- now we need to drop the row_num column as we don't actually need that

alter table layoffs_staging2 
drop column row_num;

Select * from layoffs_staging2;

-- Now this is how we can use this data. Data is cleaned and ready to use for EDA. 






 
