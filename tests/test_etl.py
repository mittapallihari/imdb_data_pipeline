import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from dags.common.etl import transform_data, load_data
import os


@pytest.fixture
def setup_test_data(tmp_path):
    input_path = tmp_path / "input"
    input_path.mkdir()
    output_path = tmp_path / "output.parquet"

    # Create sample data files
    titles = pd.DataFrame({
        'tconst': ['tt0000001', 'tt0000002'],
        'originalTitle': ['Carmencita', 'The Clown and His Dogs'],
        'runtimeMinutes': [1, 5],
        'genres': ['Documentary,Short', 'Short']
    })
    ratings = pd.DataFrame({
        'tconst': ['tt0000001', 'tt0000002'],
        'averageRating': [5.7, 6.1],
        'numVotes': [1552, 197]
    })
    principals = pd.DataFrame({
        'tconst': ['tt0000001', 'tt0000002'],
        'nconst': ['nm0005690', 'nm0721526'],
        'category': ['director', 'director']
    })
    names = pd.DataFrame({
        'nconst': ['nm0005690', 'nm0721526'],
        'primaryName': ['William K.L. Dickson', 'William K.L. Dickson']
    })

    titles.to_csv(input_path / 'title.basics.sample.tsv', sep='\t', index=False)
    ratings.to_csv(input_path / 'title.ratings.sample.tsv', sep='\t', index=False)
    principals.to_csv(input_path / 'title.principals.sample.tsv', sep='\t', index=False)
    names.to_csv(input_path / 'name.basics.sample.tsv', sep='\t', index=False)

    return str(input_path), str(output_path)


def test_transform_data(setup_test_data):
    input_path, output_path = setup_test_data

    # Run the transformation function
    transform_data(input_path, output_path)

    # Read the output file
    output_df = pd.read_parquet(output_path)

    # Create the expected output DataFrame
    expected_data = {
        'tconst': ['tt0000001', 'tt0000002'],
        'originalTitle': ['Carmencita', 'The Clown and His Dogs'],
        'runtimeMinutes': [1, 5],
        'averageRating': [5.7, 6.1],
        'numVotes': [1552, 197],
        'genres': ['Documentary,Short', 'Short'],
        'nconst': ['nm0005690', 'nm0721526'],
        'primaryName': ['William K.L. Dickson', 'William K.L. Dickson']
    }
    expected_df = pd.DataFrame(expected_data)

    # Assert the output matches the expected output
    assert_frame_equal(output_df, expected_df)


def test_load_data(setup_test_data, tmp_path):
    input_path, output_path = setup_test_data

    # Create a small DataFrame to act as the transformed data
    test_data = {
        'tconst': ['tt0000001', 'tt0000002'],
        'originalTitle': ['Carmencita', 'The Clown and His Dogs'],
        'runtimeMinutes': [1, 5],
        'averageRating': [5.7, 6.1],
        'numVotes': [1552, 197],
        'genres': ['Documentary,Short', 'Short'],
        'nconst': ['nm0005690', 'nm0721526'],
        'primaryName': ['William K.L. Dickson', 'William K.L. Dickson']
    }
    transformed_df = pd.DataFrame(test_data)
    transformed_df.to_parquet(output_path, index=False)

    # Define the database connection string
    db_path = tmp_path / "test_db.sqlite"
    db_conn = f'sqlite:///{db_path}'

    # Run the load_data function
    load_data(db_conn, output_path)

    # Query the database to check if the data was loaded correctly
    engine = create_engine(db_conn)
    with engine.connect() as conn:
        result_df = pd.read_sql_table('imdb_data', conn)

    # Assert the data in the database matches the transformed data
    assert_frame_equal(result_df, transformed_df)