import pandas as pd

FILEPATH_MOVIE = r'/home/airflow/gcs/data/movie.csv'
FILEPATH_RATING = r"/home/airflow/gcs/data/rating.csv"
FILEPATH_TAGS = r"/home/airflow/gcs/data/tag.csv"

import logging
logging.info('Retraval Import success')

#df_movie = pd.read_csv(FILEPATH_MOVIE, nrows=1)
#logging.info('after calling the read function')
#, nrows=10000
def get_data():
   logging.info('Retraval called on CSV')
   df_movie = pd.read_csv(FILEPATH_MOVIE, nrows=20000)
   df_ratings = pd.read_csv(FILEPATH_RATING, nrows=20000)
   df_tags = pd.read_csv(FILEPATH_TAGS, nrows=20000)
   logging.info('Retraval success on CSV')
   return df_movie, df_ratings, df_tags


def merge_data(df_movie, df_ratings, df_tags):
   logging.info('merge called on DFS1')
   df_movie_ratings =df_movie.merge(df_ratings, how="inner", on="movieId")
   logging.info('merge called on DFS2')
   df_movie_tags = df_movie.merge(df_tags, how="inner", on="movieId", )
   logging.info('merge called on DFS3')
   df_movie_tags_ratings = df_movie_ratings.merge(df_movie_tags, how="inner", on="movieId")
   logging.info('merge complete on DFS4')
   return df_movie_ratings, df_movie_tags, df_movie_tags_ratings


logging.info('Retraval Import success2')