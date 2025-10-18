# Code for custom code recipe id-dku-rdf-files-extractor
from typing import Literal
import dataiku
import pandas as pd
from rdflib import Graph

from dkurdftools.formats.utils import parse_rdf_stream_as_graph

# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config


# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
input_managed_folders_names = get_input_names_for_role("input_managed_folders")
input_managed_folders = [dataiku.Folder(name) for name in input_managed_folders_names]
# There's only one unary output in this recipe
output_dataset_names = get_output_names_for_role("output_dataset")
output_dataset = dataiku.Dataset(output_dataset_names[0])

# Read parameters (see recipe.json for details). the first two are mandatory
output_schema: Literal["sql_table", "single_column"] = get_recipe_config().get(
    "output_schema"
)
single_column_output = get_recipe_config().get("single_column_output")
single_column_output_format = get_recipe_config().get(
    "single_column_output_format", "turtle"
)
# file_source_output_column is optional and can be None
file_source_output_column = get_recipe_config().get("file_source_output_column")

# Compute a Pandas dataframe to write into the output dataset
if output_schema == "sql_table":
    columns = ["subject", "predicate", "object"]
else:
    columns = [single_column_output]
if file_source_output_column is not None:
    columns.append(file_source_output_column)
output_df = pd.DataFrame(columns=columns)


for input_managed_folder in input_managed_folders:
    for file_path in input_managed_folder.list_paths_in_partition():
        # load grpah form the file
        with input_managed_folder.get_download_stream(file_path) as stream:
            graph: Graph = parse_rdf_stream_as_graph(stream, file_format=None)

        # extract the graph into the output dataset
        if output_schema == "sql_table":
            for s, p, o in graph:
                data = [s.n3(), p.n3(), o.n3()]
                if file_source_output_column is not None:
                    data.append(file_path)
                output_df.loc[len(output_df)] = data
        else:  # else, serialize the whole graph into a single column
            data = [graph.serialize(format=single_column_output_format)]
            if file_source_output_column is not None:
                data.append(file_path)
            output_df.loc[len(output_df)] = data

# Write recipe outputs
output_dataset.write_with_schema(output_df)
