
-- 1. Load dataset
raw_data = LOAD 'lab02/input/hotel-review.csv' USING PigStorage(';') AS (id: chararray, review: chararray, category: chararray, object: chararray, sentiment: chararray);
stop_words = LOAD 'lab02/input/stopwords.txt' AS(word: chararray);

valid_data =  FILTER raw_data BY review IS NOT NULL AND review != '' AND review != ' ';

-- 2.Clean text
clean_text = FOREACH valid_data GENERATE
    id,
    TRIM(REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    LOWER(review), '[-^]', ' '
                ), '\\p{Punct}', ' '
            ), '[0-9]', ' '
        ), '\\s+', ' '
    )) AS review, category, object, sentiment;

-- 3.Tokenize
processed_data = FOREACH clean_text GENERATE
    id,
    TOKENIZE(review) AS words_bag, category, object, sentiment;

-- 4. Flatten
flattened_word = FOREACH processed_data GENERATE id, FLATTEN(words_bag) AS word, category, object, sentiment;

-- 5. Eliminate stop_words by LEFT OUTER JOIN
joined_data = JOIN flattened_word by word LEFT OUTER, stop_words by word;

filtered_data = FOREACH joined_data GENERATE
    flattened_word::id AS id,
    flattened_word::word AS word,
    flattened_word::category AS category,
    flattened_word::object AS object,
    flattened_word::sentiment AS sentiment,
    stop_words::word AS s_word;

final_result = FILTER filtered_data BY s_word IS NULL;

final_output = FOREACH final_result GENERATE id, word, category, object, sentiment;


--6. Save the final_output
STORE final_output INTO 'lab02/output' USING PigStorage(',');

--7. For screenshot
top10 = LIMIT final_output 10;
DUMP top10;
