import pathlib

import pytest
from rdflib import Graph

from ..formats.utils import parse_rdf_stream_as_graph


current_filepath = pathlib.Path(__file__).parent.resolve()

@pytest.mark.parametrize("file_name, rdf_format, expected_nb_triples", [
    ("dblp.nt", "nt", 17),
    ("dave_beckett.ttl", "ttl", 4),
])
def test_parse_rdf_stream_as_graph(file_name, rdf_format, expected_nb_triples):
    file_path = f"{current_filepath}/data/{file_name}"
    with open(file_path) as stream:
        graph = parse_rdf_stream_as_graph(stream, rdf_format)
    
    assert len(graph) == expected_nb_triples

    ref_graph = Graph()
    ref_graph.parse(file_path)

    assert graph.isomorphic(ref_graph) is True
