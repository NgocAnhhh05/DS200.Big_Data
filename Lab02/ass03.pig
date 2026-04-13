-- Xác định khía cạnh nào nhận nhiều đánh giá tiêu cực (negative) nhất, và khía cạnh nào nhận nhiều đánh giá tích cực nhất (positive) nhất.


data = LOAD 'lab02/input/hotel-review.csv' USING PigStorage(';') AS (id: chararray, review: chararray, category: chararray, aspect: chararray, sentiment: chararray);


valid_data = FILTER data BY review IS NOT NULL AND TRIM(review) != '';
used_data = FOREACH valid_data GENERATE
    LOWER(aspect) AS aspect,
    LOWER(sentiment) AS sentiment;

pos_data = FILTER used_data BY sentiment == 'positive';
neg_data = FILTER used_data BY sentiment == 'negative';

grouped_pos_aspect = GROUP pos_data BY aspect;
grouped_neg_aspect = GROUP neg_data BY aspect;

pos_counts = FOREACH grouped_pos_aspect GENERATE
    group AS aspect,
    COUNT(pos_data) AS count_pos;

pos_counts_ordered = ORDER pos_counts BY count_pos DESC;
top1_pos = LIMIT pos_counts_ordered 1;
STORE top1_pos INTO 'lab02/output/pos'  USING PigStorage(',');

neg_counts = FOREACH grouped_neg_aspect GENERATE
    group AS aspect,
    COUNT(neg_data) AS count_neg;
neg_counts_ordered = ORDER neg_counts BY count_neg DESC;
top1_neg = LIMIT neg_counts_ordered 1;
-- DUMP top1_neg;
STORE top1_neg INTO 'lab02/output/neg' USING PigStorage(',');

