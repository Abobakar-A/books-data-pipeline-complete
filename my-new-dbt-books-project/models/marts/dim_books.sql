-- models/marts/dim_books.sql

SELECT
    book_id,
    title,
    author,
    rating,
    price,
    is_available,
    num_reviews,
    description,
    image_url,
    book_url,
    -- إضافة عمود جديد لتصنيف السعر
    CASE
        WHEN price < 15.00 THEN 'Low'
        WHEN price >= 15.00 AND price < 30.00 THEN 'Medium'
        WHEN price >= 30.00 THEN 'High'
        ELSE 'Unknown'
    END AS price_segment
FROM {{ ref('stg_books') }}