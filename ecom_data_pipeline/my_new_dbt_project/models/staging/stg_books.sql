-- models/staging/stg_books.sql

-- قم بإنشاء هذا النموذج كـ 'view'
{{ config(materialized='view') }}

SELECT
    -- إنشاء معرف فريد لكل كتاب
    ROW_NUMBER() OVER(ORDER BY title, price, rating) AS book_id,
    
    -- عنوان الكتاب
    CAST(title AS STRING) AS title,
    
    -- سعر الكتاب
    CAST(price AS NUMERIC(10, 2)) AS price,
    
    -- تقييم الكتاب
    CAST(rating AS INTEGER) AS rating,

    -- تاريخ الاستخراج
    CAST(extraction_date AS TIMESTAMP) AS extraction_date

FROM {{ source('books_db_source', 'BOOKS_RAW') }}
