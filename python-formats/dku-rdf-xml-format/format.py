# This file is the actual code for the custom Python format tttest_tf

# import the base class for the custom format
from dataiku.customformat import Formatter, OutputFormatter, FormatExtractor

from dkurdftools.format_extractor import RDFFormatExtractor 

from rdflib import Graph

"""
A custom Python format is a subclass of Formatter, with the logic split into
OutputFormatter for outputting to a format, and FormatExtractor for reading
from a format

The parameters it expects are specified in the format.json file.

Note: the name of the class itself is not relevant.
"""
class MyFormatter(Formatter):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user for the formatter instance
        are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Formatter.__init__(self, config, plugin_config)  # pass the parameters to the base class

    def get_output_formatter(self, stream, schema):
        """
        Return a OutputFormatter for this format
        :param stream: the stream to write the formatted data to
        :param schema: the schema of the rows that will be formatted (never None)
        """
        return MyOutputFormatter(stream, schema)
        
    def get_format_extractor(self, stream, schema=None):
        """
        Return a FormatExtractor for this format
        :param stream: the stream to read the formatted data from
        :param schema: the schema of the rows that will be extracted. None when the extractor is used to detect the format.
        """
        return RDFFormatExtractor("xml", stream, schema)


class MyOutputFormatter(OutputFormatter):
    """
    Writes a stream of rows to a stream in a format. The calls will be:
    
    * write_header()
    * write_row(row_1)  
      ...
    * write_row(row_N)  
    * write_footer()  
    
    """
    def __init__(self, stream, schema):
        """
        Initialize the formatter
        :param stream: the stream to write the formatted data to
        """
        OutputFormatter.__init__(self, stream)
        self.schema = schema
        
    def write_header(self):
        """
        Write the header of the format (if any)
        """
        pass

    def write_row(self, row):
        """
        Write a row in the format
        :param row: array of strings, with one value per column in the schema
        """
        pass
    
    def write_footer(self):
        """
        Write the footer of the format (if any)
        """
        pass
        
        