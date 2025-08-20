import pytest
from unittest.mock import patch
import pandas as pd

from src.search import search_result


@patch('src.search.vector_search')
@patch('src.search.table_exists')
def test_search_result_success_when_table_exists(mock_table_exists, mock_vector_search):
    """
    Tests the happy path where the table exists and a search is performed.
    """
    # Arrange: Configure mocks
    mock_table_exists.return_value = True
    expected_df = pd.DataFrame({'target_plotid':['5','5'],'base_plotid': [101, 102], 'distance': [0.1, 0.2]})
    mock_vector_search.return_value = expected_df

    project = "test-project"
    dataset = "test-dataset"
    table = "test-table"
    uniqueid = "5"
    matches = 2

    # Act: Call the function under test
    result_df = search_result(uniqueid, matches, project, dataset, table)

    # Assert: Verify correct calls and return value
    mock_table_exists.assert_called_once_with(project, dataset, table)
    mock_vector_search.assert_called_once_with(
        plotid=uniqueid,
        n_matches=matches,
        project=project,
        dataset=dataset,
        table=table
    )
    pd.testing.assert_frame_equal(result_df, expected_df)


@patch('src.search.vector_search')
@patch('src.search.table_exists')
def test_search_result_raises_error_when_table_not_found(mock_table_exists, mock_vector_search):
    """
    Tests that a FileNotFoundError is raised if the table does not exist.
    """
    # Arrange: Configure mocks
    mock_table_exists.return_value = False

    project = "test-project"
    dataset = "test-dataset"
    table = "non-existent-table"

    # Act & Assert: Use pytest.raises to check for the expected exception
    with pytest.raises(FileNotFoundError) as excinfo:
        search_result(uniqueid="5", matches=2, project=project, dataset=dataset, table=table)

    # Assert that the exception message is correct and no search was attempted
    assert f"The table {project}.{dataset}.{table} does not exist." in str(excinfo.value)
    mock_table_exists.assert_called_once_with(project, dataset, table)
    mock_vector_search.assert_not_called()