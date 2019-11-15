import pandas as pd

import logging
logging.info('aggre Import success')

def agg_rating_movie(df_movie_ratings):
    logging.info('aggri_ rating called')
    df = df_movie_ratings[['movieId', 'title', 'rating']]
    df_agg_rating = df.groupby(['movieId','title']).count() \
        .sort_values('rating', ascending=False)
    logging.info('aggri1_rating returned')    
    return df_agg_rating
#[:50]

def agg_rating_tag(df_movie_tags):
    logging.info('aggri_ ganre called')
    df = df_movie_tags[['tag', 'genres_x', 'rating']]
    df_agg_rating_tags = df.groupby(['genres_x']).mean() \
        .sort_values(['rating'], ascending=False)
    logging.info('aggri1_ genere returned')    
    return df_agg_rating_tags

#[:50]