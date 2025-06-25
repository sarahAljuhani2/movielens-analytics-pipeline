SELECT
     movie_id,
     tag_id,
     relevance_score
FROM {{ ref('fct_genome_scores')}}
where relevance_score <=0
