from rdflib.term import BNode, Literal, URIRef, Variable
from rdflib.namespace import XSD
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery

def parse_query(query: str) -> dict:
    return translateQuery(parseQuery(query)).algebra


def unparse_query(parsed_query: str) -> dict:
    return {} # TODO


def is_query_select_type(parsed_query: str) -> bool:
    return parsed_query.name == "Select"

def is_query_construct_type(parsed_query: str) -> bool:
    return parsed_query.name == "Construct"


def add_limit_to_query(parsed_query: dict, limit: int) -> dict:
    # TODO update AST with a LIMIT clause
    return parsed_query

def get_select_variables(parsed_query: dict):
    # TODO
    return []