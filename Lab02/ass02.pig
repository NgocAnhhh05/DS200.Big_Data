processed_data = LOAD 'lab02/input/ass01.txt' USING PigStorage(',') AS (id:chararray ,word:chararray, category: chararray, aspect: chararray, sentiment: chararray);

--- 1: Thống kê tần số từ
grouped_words = GROUP processed_data BY word;
word_counts = FOREACH grouped_words GENERATE group AS word, COUNT(processed_data) AS frequency;
count_larger_500 = FILTER word_counts BY frequency > 500;
count_word_ordered = ORDER count_larger_500 BY frequency DESC;
DUMP count_word_ordered
STORE count_word_ordered INTO 'lab02/output/word_counts' USING PigStorage(',');

-- 2: Thống kê số BÌNH LUẬN theo category
grouped_category = GROUP processed_data BY category;
count_category = FOREACH grouped_category {
    distinct_ids = DISTINCT processed_data.id;
    GENERATE group AS category, COUNT(distinct_ids) AS total_comments_1;
};
count_category_ordered = ORDER count_category BY total_comments_1 DESC;
DUMP count_category_ordered;
STORE count_category_ordered INTO 'lab02/output/category_count' USING PigStorage(',');

-- 3: Thống kê số BÌNH LUẬN theo aspect

grouped_aspect = GROUP processed_data BY aspect;
count_aspect = FOREACH grouped_aspect {
    distinct_ids = DISTINCT processed_data.id;
    GENERATE group as aspect, COUNT(distinct_ids) AS total_comments_2;
};
count_aspect_ordered = ORDER count_aspect BY total_comments_2 DESC;
DUMP count_aspect_ordered;
STORE count_aspect_ordered INTO 'lab02/output/aspect_count' USING PigStorage(',');
