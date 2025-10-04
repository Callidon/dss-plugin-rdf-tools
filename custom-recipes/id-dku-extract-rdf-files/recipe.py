# Code for custom code recipe id-dku-rdf-files-extractor (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_managed_folders_names = get_input_names_for_role('input_managed_folders')
# The dataset objects themselves can then be created like this:
# input_A_datasets = [dataiku.Dataset(name) for name in input_managed_folders]

# For outputs, the process is the same:
output_dataset_names = get_output_names_for_role('output_dataset')
output_datasets = [dataiku.Dataset(name) for name in output_dataset_names]


# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -*- coding: utf-8 -*-
import dataiku
from os.path import join
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from rdflib import Graph


# Read recipe inputs
RDF_sources = dataiku.Folder("Hdp2qZyJ")
RDF_sources_info = RDF_sources.get_info()


# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

# Compute a Pandas dataframe to write into the output dataset
output_df = pd.DataFrame(columns=["subject", "predicate", "object"])

g = Graph()

for file_path in RDF_sources.list_paths_in_partition():
    with RDF_sources.get_download_stream(file_path) as f:
        g.parse(f.read())

for s, p, o in g:
    output_df.loc[len(output_df)]= [s, p, o]


# Write recipe outputs
semantic_data = dataiku.Dataset("semantic_data")
semantic_data.write_with_schema(output_df)
