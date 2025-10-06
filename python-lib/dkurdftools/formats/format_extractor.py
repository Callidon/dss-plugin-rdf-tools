from dataiku.customformat import FormatExtractor

from .utils import parse_rdf_stream_as_graph

class RDFFormatExtractor(FormatExtractor):
    """
    Extract an RDF file into a stream of rows
    """
    def __init__(self, file_format, stream, schema):
        """
        Initialize the extractor
        :param rdf_format: RDF file format ("text/turtle", "n3", "xml", etc)
        :param stream: the stream to read the formatted data from
        """
        FormatExtractor.__init__(self, stream)
        self.columns = ["subject", "predicate", "object"]
        # load file content
        self.graph = parse_rdf_stream_as_graph(stream)        
        # create an iterator over the graph content
        self.iterator = iter(self.graph)
        
    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        return [
            {"name": "subject", "type": "STRING"},
            {"name": "predicate", "type": "STRING"},
            {"name": "object", "type": "STRING"}
        ]
    
    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        try:
            s, p, o = next(self.iterator)
            return {"subject": s, "predicate": p, "object": o}
        except StopIteration:
            pass
        return None
        