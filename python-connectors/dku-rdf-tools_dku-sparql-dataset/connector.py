from copy import deepcopy

from dataiku.connector import Connector

import requests
from rdflib import Graph

from dkurdftools.sparql_parsing import parse_query, is_query_select_type, is_query_construct_type, add_limit_to_query, get_select_variables


class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        self.url = self.config.get("url")
        sparql_query = self.config.get("custom_query", "SELECT ?s ?p ?o FROM {?s ?p ?o}")
        self.parsed_query = parse_query(self.sparql_query)
        

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        If you do provide a schema here, all columns defined in the schema
        will always be present in the output (with None value),
        even if you don't provide a value in generate_rows

        The schema must be a dict, with a single key: "columns", containing an array of
        {'name':name, 'type' : type}.

        Example:
            return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

        Supported types are: string, int, bigint, float, double, date, boolean
        """

        if is_query_select_type(self.parsed_query):
            return {
                "columns": [
                    {"name": select_var, "type": "STRING"}
                    # TODO make sure that there's no ? prefix on each variable name
                    for select_var in get_select_variables(self.parsed_query)
                ]
            }
        
        if is_query_construct_type(self.parsed_query):
            return {
                "columns": [
                    {"name": "subject", "type": "STRING"},
                    {"name": "predicate", "type": "STRING"},
                    {"name": "object", "type": "STRING"}
                ]
            }
        # TODO use a better exception class?
        raise NotImplemented("Unsupported query type, only SELECT and CONSTRUCT query are supported")

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        sparql_query = deepcopy(self.sparql_query)
        if records_limit > -1:
            sparql_query = add_limit_to_query(sparql_query, records_limit)

        # Add header to ensure the endpoint returns the same data format per query
        # it's logically the default format, per the standard, but we cannot be sure as some implemntation
        # uses a custom output format by default
        if is_query_construct_type(self.parsed_query):
            content_type = "application/xml;charset=utf-8"
        else:
            content_type = "application/sparql-results+json;charset=utf-8"
        headers = {"Content-type": content_type, "User-agent": "dataiku/rdf-tools-plugin"}
        res = requests.get(self.url, params={"query": unparse_query(self.parsed_query)}, headers=headers)
        res.raise_for_status()
        
        # analyse the output depending on the query type
        if is_query_construct_type(self.parsed_query):
            graph = Graph()
            graph.parse(data=res.text, format="xml")
            for s, p, o in graph:
                yield { "subject" : str(s), "predicate" : str(p), "object": str(o) }
        else:
            sparql_results = res.json()
            for result in sparql_results.get("results", {}).get("bindings", []):
                # format is {key: {"value", "type", "lang"}}
                # TODO use an rdflib function here to handle the response format
                # TODO add a paramater so use can choose if they get the sparql json as value or a real RDF value?
                yield {key: value["value"] for key, value in result.items()}


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None, write_mode="OVERWRITE"):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        write_mode can either be OVERWRITE or APPEND.
        It will not be APPEND unless the plugin explicitly supports append mode. See flag supportAppend in connector.json.
        If applicable, the write_mode should be handled in the plugin code.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        raise NotImplementedError


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise NotImplementedError


    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []


    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise NotImplementedError


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise NotImplementedError


class CustomDatasetWriter(object):
    def __init__(self):
        pass

    def write_row(self, row):
        """
        Row is a tuple with N + 1 elements matching the schema passed to get_writer.
        The last element is a dict of columns not found in the schema
        """
        raise NotImplementedError

    def close(self):
        pass
