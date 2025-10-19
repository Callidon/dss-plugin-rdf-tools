# Code for custom code recipe id-dku-rdf-files-extractor
import dataiku
from rdflib import Graph

from dkurdftools.storage.dss_store import DataikuDatasetStore

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

# Read parameters (see recipe.json for details)
subject_output_column = get_recipe_config().get("subject_output_column", "subject")
predicate_output_column = get_recipe_config().get(
    "predicate_output_column", "predicate"
)
object_output_column = get_recipe_config().get("object_output_column", "object")

# use the dedicated dataiku dataset store for the graph
# which will take care of writing the output into the dataset
store = DataikuDatasetStore(
    output_dataset,
    subject_column_name=subject_output_column,
    predicate_column_name=predicate_output_column,
    object_column_name=object_output_column,
)
# init the dataset schema
store.write_schema()
graph = Graph(store=store)

# load each file into the graph
for input_managed_folder in input_managed_folders:
    for file_path in input_managed_folder.list_paths_in_partition():
        with input_managed_folder.get_download_stream(file_path) as stream:
            graph.parse(data=stream.read())

# commit any remaining data
graph.commit()
