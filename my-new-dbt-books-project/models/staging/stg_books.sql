-- models/staging/stg_books.sql

SELECT
    -- معرف فريد لكل كتاب
    CAST(book_id AS STRING) AS book_id,
    -- عنوان الكتاب
    CAST(title AS STRING) AS title,
    -- اسم المؤلف
    CAST(author AS STRING) AS author,
    -- تقييم الكتاب (نجمات)
    CAST(rating AS INTEGER) AS rating,
    -- سعر الكتاب (مع إزالة رمز العملة وتحويله إلى رقم عشري)
    CAST(REPLACE(price, '£', '') AS NUMERIC(10, 2)) AS price,
    -- توفر الكتاب (مع تحويل النص إلى قيمة منطقية)
    CASE
        WHEN availability = 'In stock' THEN TRUE
        ELSE FALSE
    END AS is_available,
    -- عدد المراجعات
    CAST(num_reviews AS INTEGER) AS num_reviews,
    -- وصف الكتاب
    CAST(description AS STRING) AS description,
    -- رابط صورة الغلاف
    CAST(image_url AS STRING) AS image_url,
    -- رابط صفحة الكتاب
    CAST(book_url AS STRING) AS book_url
FROM {{ source('books_db_source', 'BOOKS_RAW') }}