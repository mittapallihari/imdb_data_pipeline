import pandas as pd
from sqlalchemy import create_engine


def transform_data(input_path, output_path):
    """ To create a parquet files with required columns after joining the input files

    :param input_path: folder path where the input files are located
    :param output_path: output file path where the output parquet file will be saved
    :return:
    """

    # Load data from TSV files
    titles = pd.read_csv(input_path + 'title.basics.sample.tsv', sep='\t', header=0)
    ratings = pd.read_csv(input_path + 'title.ratings.sample.tsv', sep='\t', header=0)
    principals = pd.read_csv(input_path + 'title.principals.sample.tsv', sep='\t', header=0)
    names = pd.read_csv(input_path + 'name.basics.sample.tsv', sep='\t', header=0)

    # Join titles and ratings on 'tconst'
    title_rating = pd.merge(titles, ratings, on='tconst', how='left')

    # Select relevant columns
    title_rating = title_rating[['tconst', 'originalTitle', 'runtimeMinutes', 'averageRating', 'numVotes', 'genres']]

    # Filter principals for category 'director' and select relevant columns
    directors = principals[principals['category'] == 'director'][['tconst', 'nconst']]

    # Join directors with names on 'nconst'
    director_names = pd.merge(directors, names[['nconst', 'primaryName']], on='nconst', how='inner')

    # Join title_rating with director_names on 'tconst'
    title_rating_dir_names = pd.merge(title_rating, director_names, on='tconst', how='left')

    # Write the output to a parquet file
    title_rating_dir_names.to_parquet(output_path, index=False)


def load_data(db_conn, transformed_file_path):
    """ Load the transformed data to SQL DB
    :param db_conn: sqlalchemy db connection details
    :param transformed_file_path: the transformed parquet file path
    :return:
    """

    engine = create_engine(db_conn)
    transformed_data = pd.read_parquet(transformed_file_path)
    transformed_data.to_sql('imdb_data', engine, if_exists='replace', index=False)
