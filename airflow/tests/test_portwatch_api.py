import pytest
import requests

from dags.common.portwatch_api import query


def test_query_success(mocker):

    mock_response = mocker.Mock() #Mock the requests.get call

    mock_response.json.return_value = {"count": 100}
    mock_response.raise_for_status.return_value = None
    mocker.patch("requests.get", return_value=mock_response)

    result = query({"where": "1=1"}, "http://fake-url.com")
    
    assert result == {"count": 100}

    requests.get.assert_called_once()

def test_query_fatal_error_does_not_retry(mocker):

    mock_response = mocker.Mock()
    mock_response.json.return_value = {"error": {"code": 400, "message": "Bad Request"}} #Simulate a 400 error from ArcGIS

    mocker.patch("requests.get", return_value=mock_response)

    with pytest.raises(ValueError, match="ArcGIS rejected the query"):
        query({"where": "bad query"}, "http://fake-url.com")
    
    assert requests.get.call_count == 1 #Ensure it only called the API once (no retries for 400 errors)