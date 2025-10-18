import pytest

from ...sparql.parsing import parse_query
from ...sparql.connector import (
    UnsupportedSparqlQueryType,
    generate_rows,
    get_and_check_sparql_query_type,
    get_read_schema,
)


@pytest.mark.parametrize(
    "query, expected_type",
    [
        ("select ?s where {?s ?p ?o}", "select"),
        ("construct {?s ?p ?o} where {?s ?p ?o}", "construct"),
        ("ask where {?s ?p ?o}", None),
    ],
)
def test_get_and_check_sparql_query_type(query, expected_type):
    parsed_query = parse_query(query)
    if expected_type is not None:
        assert get_and_check_sparql_query_type(parsed_query) == expected_type
    else:
        with pytest.raises(UnsupportedSparqlQueryType):
            get_and_check_sparql_query_type(parsed_query)


@pytest.mark.parametrize(
    "query, expected_columns",
    [
        (
            "select ?s where {?s ?p ?o}",
            [
                {"name": "s", "type": "STRING"},
            ],
        ),
        (
            "select ?s ?p where {?s ?p ?o}",
            [
                {"name": "s", "type": "STRING"},
                {"name": "p", "type": "STRING"},
            ],
        ),
        (
            "construct {?s ?p ?o} where {?s ?p ?o}",
            [
                {"name": "subject", "type": "STRING"},
                {"name": "predicate", "type": "STRING"},
                {"name": "object", "type": "STRING"},
            ],
        ),
        # any construct query always yields the same schema
        (
            "construct {?s ?p ?o. ?s ?p2 ?o2} where {?s ?p ?o. ?s ?p2 ?o2}",
            [
                {"name": "subject", "type": "STRING"},
                {"name": "predicate", "type": "STRING"},
                {"name": "object", "type": "STRING"},
            ],
        ),
    ],
)
def test_get_read_schema(query, expected_columns):
    parsed_query = parse_query(query)

    def sort_key(r):
        return r["name"]

    schema = get_read_schema(parsed_query)
    assert schema.keys() == {"columns"}

    assert sorted(schema["columns"], key=sort_key) == sorted(
        expected_columns, key=sort_key
    )


def test_generate_rows_select_query_json_format(sparql_select_query):
    url, parsed_query, json_resp = sparql_select_query

    rows = list(generate_rows(url, parsed_query, select_results_type="json"))
    assert len(rows) == len(json_resp["results"]["bindings"])

    def sort_key(r):
        return r["book"]["value"]

    assert sorted(rows, key=sort_key) == sorted(
        json_resp["results"]["bindings"], key=sort_key
    )


def test_generate_rows_select_query_n3_format(sparql_select_query):
    url, parsed_query, json_resp = sparql_select_query

    rows = list(generate_rows(url, parsed_query, select_results_type="n3"))
    assert len(rows) == len(json_resp["results"]["bindings"])

    def sort_key(r):
        return r["book"]

    expected_results = [
        {
            "book": "<http://example.org/book/book6>",
            "title": '"The Hitchhiker\'s Guide to the Galaxy"',
        },
        {
            "book": "<http://example.org/book/book7>",
            "title": '"The Restaurant at the End of the Universe"',
        },
        {
            "book": "<http://example.org/book/book5>",
            "title": '"Life, the Universe and Everything"',
        },
    ]

    assert sorted(rows, key=sort_key) == sorted(expected_results, key=sort_key)


def test_generate_rows_construct_query(sparql_construct_query):
    url, parsed_query = sparql_construct_query

    rows = list(generate_rows(url, parsed_query))
    assert len(rows) == 3

    def sort_key(r):
        return r["subject"]

    expected_results = [
        {
            "subject": "<http://example.org/book/book6>",
            "predicate": "<http://www.w3.org/2000/01/rdf-schema#label>",
            "object": '"The Hitchhiker\'s Guide to the Galaxy"',
        },
        {
            "subject": "<http://example.org/book/book7>",
            "predicate": "<http://www.w3.org/2000/01/rdf-schema#label>",
            "object": '"The Restaurant at the End of the Universe"',
        },
        {
            "subject": "<http://example.org/book/book5>",
            "predicate": "<http://www.w3.org/2000/01/rdf-schema#label>",
            "object": '"Life, the Universe and Everything"',
        },
    ]

    assert sorted(rows, key=sort_key) == sorted(expected_results, key=sort_key)


def test_generate_rows_records_limit(requests_mock):
    pass
