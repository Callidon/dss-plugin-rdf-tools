from dataiku.customformat import Formatter

from dkurdftools.formats.format_extractor import RDFFormatExtractor, RDFOutputFormatter


class MyFormatter(Formatter):
    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user for the formatter instance
        are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Formatter.__init__(
            self, config, plugin_config
        )
        self.config = config

    def get_output_formatter(self, stream, schema):
        """
        Return a OutputFormatter for this format
        :param stream: the stream to write the formatted data to
        :param schema: the schema of the rows that will be formatted (never None)
        """
        return RDFOutputFormatter(stream, schema, "json-ld", **self.config)

    def get_format_extractor(self, stream, schema=None):
        """
        Return a FormatExtractor for this format
        :param stream: the stream to read the formatted data from
        :param schema: the schema of the rows that will be extracted. None when the extractor is used to detect the format.
        """
        return RDFFormatExtractor("nt", stream, schema)
