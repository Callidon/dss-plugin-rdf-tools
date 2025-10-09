from copy import deepcopy
from typing import Iterator, Literal
from rdflib import Graph
from rdflib.plugins.sparql.sparql import Query
from rdflib.plugins.sparql.results.jsonresults import parseJsonTerm
import requests

from .parsing import (
    unparse_query,
    is_query_select_type,
    is_query_construct_type,
    add_limit_to_query,
    get_select_variables,
)


class UnsupportedSparqlQueryType(Exception):
    """Raised when a SPARQL query type isn't supported"""


def get_and_check_sparql_query_type(
    parsed_query: Query,
) -> Literal["select", "construct"]:
    """Get and check the SPARQL query type.
    Only Select and Construct queries are supported.

    :param parsed_query: Parsed SPARQL query
    :raises UnsupportedSparqlQueryType: Raised if the SPARQL query type isn't supported
    :return: Query type
    """
    if is_query_select_type(parsed_query):
        return "select"
    if is_query_construct_type(parsed_query):
        return "construct"
    raise UnsupportedSparqlQueryType("Only SELECT and CONSTRUCT query are supported")


def get_read_schema(parsed_query: Query) -> dict:
    """Get the DSS dataset read schema from a SPARQL query.
    Only Select and Construct queries are supported.

    :param parsed_query: Parsed SPARQL query
    :raises UnsupportedSparqlQueryType: Raised if the SPARQL query type isn't supported
    :return: DSS dataset schema
    """
    query_type = get_and_check_sparql_query_type(parsed_query)
    if query_type == "select":
        return {
            "columns": [
                {"name": select_var, "type": "STRING"}
                for select_var in get_select_variables(parsed_query)
            ]
        }
    # else, the query is a construct query
    return {
        "columns": [
            {"name": "subject", "type": "STRING"},
            {"name": "predicate", "type": "STRING"},
            {"name": "object", "type": "STRING"},
        ]
    }


def generate_rows(
    url: str,
    parsed_query: Query,
    records_limit: int = -1,
    select_results_type: Literal["json", "n3"] = "json",
) -> Iterator[dict]:
    """Generates rows for a DSS dataset from a SPARQL endpoint

    :param url: SPARQL endpoint URL
    :param parsed_query: SPARQL query
    :param records_limit: Maximum number of records to output, defaults to -1 (no limit)
    :param select_results_type: Results format for SELECT queries
    :raises UnsupportedSparqlQueryType: Raised if the SPARQL query type isn't supported
    :yield: Dataset record
    """
    query_type = get_and_check_sparql_query_type(parsed_query)
    sparql_query = deepcopy(parsed_query)
    if records_limit > -1:
        sparql_query = add_limit_to_query(sparql_query, records_limit)

    # Add header to ensure the endpoint returns the same data format per query
    # it's logically the default format, per the standard, but we cannot be sure as some implemntation
    # uses a custom output format by default
    if query_type == "construct":
        content_type = "application/xml;charset=utf-8"
    else:
        content_type = "application/sparql-results+json;charset=utf-8"
    headers = {
        "Content-type": content_type,
        "User-agent": "dataiku/rdf-tools-plugin",
    }
    res = requests.get(
        url, params={"query": unparse_query(sparql_query)}, headers=headers
    )
    res.raise_for_status()

    # format the output depending on the query type
    if query_type == "construct":
        # construct queries output raw RDF data
        graph = Graph()
        graph.parse(data=res.text, format="xml")
        for s, p, o in graph:
            yield {"subject": str(s), "predicate": str(p), "object": str(o)}
    else:
        # sparql queries output rows of bindings
        sparql_results = res.json()
        for result in sparql_results.get("results", {}).get("bindings", []):
            yield {
                key: value["value"]
                if select_results_type == "json"
                else parseJsonTerm(value["value"])
                for key, value in result.items()
            }
