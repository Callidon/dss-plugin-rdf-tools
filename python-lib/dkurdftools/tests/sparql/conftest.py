import pytest
import re

from rdflib import Graph, Literal, URIRef

from ...sparql.parsing import parse_query


@pytest.fixture()
def sparql_select_query(requests_mock):
    """
    Fixture that yields an URL, a parsed SPARQL select query, its JSON response,
    and configure an HTTP mock for the execution of that query.
    """

    url = "https://wikidata.com/sparql"
    parsed_query = parse_query("select ?book ?title where {?book rdfs:label ?title}")

    # mock the HTTP response
    json_resp = {
        "head": {"vars": ["book", "title"]},
        "results": {
            "bindings": [
                {
                    "book": {"type": "uri", "value": "http://example.org/book/book6"},
                    "title": {
                        "type": "literal",
                        "value": "The Hitchhiker's Guide to the Galaxy",
                    },
                },
                {
                    "book": {"type": "uri", "value": "http://example.org/book/book7"},
                    "title": {
                        "type": "literal",
                        "value": "The Restaurant at the End of the Universe",
                    },
                },
                {
                    "book": {"type": "uri", "value": "http://example.org/book/book5"},
                    "title": {
                        "type": "literal",
                        "value": "Life, the Universe and Everything",
                    },
                },
            ]
        },
    }
    requests_mock.get(re.compile(f"{url}*"), json=json_resp)

    yield url, parsed_query, json_resp


@pytest.fixture()
def sparql_construct_query(requests_mock):
    """
    Fixture that yields an URL, a parsed SPARQL construct query,
    and configure an HTTP mock for the execution of that query.
    """

    url = "https://wikidata.com/sparql"
    parsed_query = parse_query("construct {?s ?p ?o} where {?s ?p ?o}")

    # mock the HTTP response
    g = Graph()
    g.add(
        (
            URIRef("http://example.org/book/book6"),
            URIRef("http://www.w3.org/2000/01/rdf-schema#label"),
            Literal("The Hitchhiker's Guide to the Galaxy"),
        )
    )
    g.add(
        (
            URIRef("http://example.org/book/book7"),
            URIRef("http://www.w3.org/2000/01/rdf-schema#label"),
            Literal("The Restaurant at the End of the Universe"),
        )
    )
    g.add(
        (
            URIRef("http://example.org/book/book5"),
            URIRef("http://www.w3.org/2000/01/rdf-schema#label"),
            Literal("Life, the Universe and Everything"),
        )
    )
    xml_resp = g.serialize(format="xml")
    requests_mock.get(
        re.compile(f"{url}*"),
        text=xml_resp,
        headers={"Content-type": "application/xml"},
    )

    yield url, parsed_query
