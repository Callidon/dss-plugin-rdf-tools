from typing import IO, Literal

from rdflib import Graph


def parse_rdf_stream_as_graph(
    stream: IO, file_format: Literal["xml", "n3", "nt", "trix"]
) -> Graph:
    """Parse a stream of RDF data as an rdflib Graph

    :param stream: Stream of RDF data
    :param file_format: File format
    :return: Graph loaded with the file content
    """
    graph = Graph()    
    file_content = "\n".join([line.decode("utf-8") if isinstance(line, bytes) else line for line in stream.readlines()])
    graph.parse(data=file_content, format=file_format)
    return graph
