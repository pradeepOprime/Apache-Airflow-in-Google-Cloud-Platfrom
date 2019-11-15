from mypipe.data_retrieval import merge_data, get_data
from mypipe.aggregation import agg_rating_tag, agg_rating_movie
import pandas_gbq
import pandas as pd
import logging
logging.info('aggre Import success')

Stage_Movie_Rating = r'/home/airflow/gcs/data/Movie_Rating_stg.csv'
Stage_Tags_Rating = r"/home/airflow/gcs/data/Tags_Rating_stg.csv"

Agg_Movie_Rating = r'/home/airflow/gcs/data/Movie_Rating_agg.csv'
Agg_Ganre_Rating = r"/home/airflow/gcs/data/Ganre_Rating_agg.csv"


def core_get_data(**kwargs):
    task_instance = kwargs['ti']
    logging.info('core_getData callaed')
    df_movie, df_ratings, df_tags = get_data()
    logging.info('core_getData returned, calling merge next')
    df_movie_ratings, df_movie_tags, df_movie_tags_ratings = merge_data(df_movie, df_ratings, df_tags)
    logging.info('core_merge assigend to dfs')
    #task_instance.xcom_push(key='df_movie_ratings', value=df_movie_ratings)
    #task_instance.xcom_push(key='df_movie_tags_ratings', value=df_movie_tags_ratings)
    df_movie_ratings.to_csv(Stage_Movie_Rating)
    df_movie_tags_ratings.to_csv(Stage_Tags_Rating)
    logging.info('core_ xcom pushed to ag stage')


def core_aggregation(**kwargs):
    task_instance = kwargs['ti']
    logging.info('core_aggregation callaed')
    #df_movie_ratings = task_instance.xcom_pull(key='df_movie_ratings', task_ids='get_data')
    #df_movie_tags_ratings = task_instance.xcom_pull(key='df_movie_tags_ratings', task_ids='get_data')
    df_movie_ratings=pd.read_csv(Stage_Movie_Rating)
    df_movie_tags_ratings=pd.read_csv(Stage_Tags_Rating)
    logging.info('xcom merge pulled')
    df_agg_rating = agg_rating_movie(df_movie_ratings)
    df_agg_rating_genre = agg_rating_tag(df_movie_tags_ratings)
    logging.info('exe aggrigate called')
    
    #task_instance.xcom_push(key='df_agg_rating', value=df_agg_rating)
    #task_instance.xcom_push(key='df_agg_rating_genre', value=df_agg_rating_genre)
    df_agg_rating.to_csv(Agg_Movie_Rating)
    df_agg_rating_genre.to_csv(Agg_Ganre_Rating)
    logging.info('xcom aggri pushed')


def core_db_insert_to_db(**kwargs):
    task_instance = kwargs['ti']
    logging.info('core_db_insert_to_db callaed')
    #df_agg_rating = task_instance.xcom_pull(key='df_agg_rating', task_ids='aggregation')
    #df_agg_rating_genre = task_instance.xcom_pull(key='df_agg_rating_genre', task_ids='aggregation')
    df_agg_rating=pd.read_csv(Agg_Movie_Rating)
    df_agg_rating_genre=pd.read_csv(Agg_Ganre_Rating)
    logging.info('xcom agge pulled')
    project_id = "oprime-data-001"
    table_id_m = 'Test.most_popular_movies'
    table_id_g = 'Test.most_popular_generes'
    logging.info('data write to db started')
    pandas_gbq.to_gbq(df_agg_rating, table_id_m, project_id=project_id,if_exists='replace')
    pandas_gbq.to_gbq(df_agg_rating, table_id_g, project_id=project_id,if_exists='replace')
    logging.info('data write complte')
    #insert_to_db(engine, df_agg_rating, 'agg_movie_ratings')
    #insert_to_db(engine, df_agg_rating_genre, 'agg_rating_genre')
