from typing import List

from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateAlgebra
from rdflib.plugins.sparql.sparql import Query


def parse_query(query: str) -> Query:
    """Parse a string SPARQL query into a logical query plan

    :param query: String SPARQL query
    :return SPARQL logical query plan
    """
    return translateQuery(parseQuery(query))


def unparse_query(parsed_query: Query) -> str:
    """Turn a logical SPARQL query plan into a string SPARQL query

    :param parsed_query: SPARQL logical query plan
    :return String SPARQL query
    """
    return translateAlgebra(parsed_query)


def is_query_select_type(parsed_query: Query) -> bool:
    """Test if a SPARQL query plan is a Select query

    :param parsed_query: SPARQL logical query plan
    :return True if the query is a Select query, False otherwise
    """
    return parsed_query.algebra.name.lower() == "selectquery"


def is_query_construct_type(parsed_query: Query) -> bool:
    """Test if a SPARQL query plan is a Construct query

    :param parsed_query: SPARQL logical query plan
    :return True if the query is a Construct query, False otherwise
    """
    return parsed_query.algebra.name.lower() == "constructquery"


def add_limit_to_query(parsed_query: Query, limit: int) -> Query:
    # TODO update AST with a LIMIT clause
    return parsed_query


def get_select_variables(parsed_query: Query) -> List[str]:
    return [str(var) for var in parsed_query.algebra.PV]
