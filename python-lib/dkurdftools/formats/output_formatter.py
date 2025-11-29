from dataiku.customformat import OutputFormatter

from rdflib import Graph
from rdflib.util import from_n3


class RDFOutputFormatter(OutputFormatter):
    """
    Writes a stream of rows to a stream in a RDF format. The calls will be:

    * write_header()
    * write_row(row_1)
      ...
    * write_row(row_N)
    * write_footer()

    """

    def __init__(
        self,
        stream,
        schema,
        format: str,
        subject_column_name: str = "subject",
        predicate_column_name: str = "predicate",
        object_column_name: str = "object",
        **kwargs,
    ):
        """
        Initialize the formatter
        :param stream: the stream to write the formatted data to
        """
        OutputFormatter.__init__(self, stream)
        self.schema = schema
        self.format = format
        self.subject_column_name = subject_column_name
        self.predicate_column_name = predicate_column_name
        self.object_column_name = object_column_name
        self.graph = Graph()

    def write_header(self):
        pass

    def write_row(self, row):
        """
        Write a row in the format.
        It will store the triple in the buffer graph instead of writing it to stream,
        as some RDF format needs to have the whole dataset to be serialized.

        :param row: array of strings, with one value per column in the schema
        """
        subj = from_n3(row[self.subject_column_name])
        pred = from_n3(row[self.predicate_column_name])
        obj = from_n3(row[self.object_column_name])
        self.graph.add((subj, pred, obj))

    def write_footer(self):
        """
        Write the footer of the format (if any).
        it will flush all the graph data into the output stream.
        """
        self.graph.serialize(self.stream, format=self.format)
