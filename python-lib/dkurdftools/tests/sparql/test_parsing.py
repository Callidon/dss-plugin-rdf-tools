import pytest

from ...sparql.parsing import parse_query, is_query_select_type, is_query_construct_type, get_select_variables


@pytest.mark.parametrize("query, expected_result", [
    ("select ?s where {?s ?p ?o}", True),
    ("construct {?s ?p ?o} where {?s ?p ?o}", False),
    ("ask where {?s ?p ?o}", False),
])
def test_is_query_select_type(query, expected_result):
    assert is_query_select_type(parse_query(query)) == expected_result

@pytest.mark.parametrize("query, expected_result", [
    ("select ?s where {?s ?p ?o}", False),
    ("construct {?s ?p ?o} where {?s ?p ?o}", True),
    ("ask where {?s ?p ?o}", False),
])
def test_is_query_construct_type(query, expected_result):
    assert is_query_construct_type(parse_query(query)) == expected_result


@pytest.mark.parametrize("query, expected_result", [
    ("select ?s where {?s ?p ?o}", ["s"]),
    ("select ?s ?p ?o2 where {?s ?p ?o. ?s ?p2 ?o2}", ["s", "p", "o2"]),
])
def test_get_select_variables(query, expected_result):
    assert sorted(get_select_variables(parse_query(query))) == sorted(expected_result)