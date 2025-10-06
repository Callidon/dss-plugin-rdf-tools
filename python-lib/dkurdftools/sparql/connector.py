from copy import deepcopy
from typing import Iterator
from rdflib import Graph
from rdflib.plugins.sparql.sparql import Query
import requests

from .parsing import (
    unparse_query,
    is_query_select_type,
    is_query_construct_type,
    add_limit_to_query,
    get_select_variables,
)


def get_read_schema(parsed_query: Query) -> dict:
    if is_query_select_type(parsed_query):
        return {
            "columns": [
                {"name": select_var, "type": "STRING"}
                # TODO make sure that there's no ? prefix on each variable name
                for select_var in get_select_variables(parsed_query)
            ]
        }

    if is_query_construct_type(parsed_query):
        return {
            "columns": [
                {"name": "subject", "type": "STRING"},
                {"name": "predicate", "type": "STRING"},
                {"name": "object", "type": "STRING"},
            ]
        }
    # TODO use a better exception class?
    raise NotImplementedError(
        "Unsupported query type, only SELECT and CONSTRUCT query are supported"
    )


def generate_rows(
    url: str,
    parsed_query: Query,
    records_limit: int = -1,
) -> Iterator[dict]:
    """
    The main reading method.

    Returns a generator over the rows of the dataset (or partition)
    Each yielded row must be a dictionary, indexed by column name.

    The dataset schema and partitioning are given for information purpose.
    """
    sparql_query = deepcopy(parsed_query)
    if records_limit > -1:
        sparql_query = add_limit_to_query(sparql_query, records_limit)

    # Add header to ensure the endpoint returns the same data format per query
    # it's logically the default format, per the standard, but we cannot be sure as some implemntation
    # uses a custom output format by default
    if is_query_construct_type(sparql_query):
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

    # analyse the output depending on the query type
    if is_query_construct_type(sparql_query):
        graph = Graph()
        graph.parse(data=res.text, format="xml")
        for s, p, o in graph:
            yield {"subject": str(s), "predicate": str(p), "object": str(o)}
    else:
        sparql_results = res.json()
        for result in sparql_results.get("results", {}).get("bindings", []):
            # format is {key: {"value", "type", "lang"}}
            # TODO use an rdflib function here to handle the response format
            # TODO add a parameter so users can choose if they get the raw sparql json or the RDF value
            yield {key: value["value"] for key, value in result.items()}
