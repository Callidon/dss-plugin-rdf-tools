from typing import Iterator
from dataiku import Dataset
from rdflib.store import Store, TripleAddedEvent
from rdflib.graph import _TripleType
from rdflib.util import from_n3
import pandas as pd

# Match any node in a triple pattern
ANY: None = None


class DataikuDatasetStore(Store):
    """An rdflib gaph store that uses a DSS Dataset for storage.
    It follows a triplestore approach, which three columns "subject", "predicate" and "object".
    """

    def __init__(
        self,
        dss_dataset: Dataset,
        subject_column_name: str = "subject",
        predicate_column_name: str = "predicate",
        object_column_name: str = "object",
        autocommit_add_threshold: int = 5000,
        configuration=None,
        identifier=None,
    ):
        super().__init__(configuration, identifier)
        self.dss_dataset = dss_dataset
        self.subject_column_name = subject_column_name
        self.predicate_column_name = predicate_column_name
        self.object_column_name = object_column_name
        self.autocommit_add_threshold = autocommit_add_threshold

        # prepare the inner dataframe used to hold data waiting to be commited
        self.staging_df = pd.DataFrame(columns=self.dataframe_columns)

    def __len__(self, context=None):
        # TODO
        return 0

    @property
    def dataframe_columns(self):
        return [
            self.subject_column_name,
            self.predicate_column_name,
            self.object_column_name,
        ]

    def write_schema(self):
        self.dss_dataset.write_schema(
            [{"name": name, "type": "string"} for name in self.dataframe_columns]
        )

    def triples(self, triple_pattern, context) -> Iterator[tuple[_TripleType, None]]:
        """Search for a triple pattern in a DSS dataset.
        Triple matching is done by reading the dataset to Pandas dataframes using the iter_dataframes() method
        (in case the dataset is too large), and then using the DataFrame.query() method on each chunk to do
        the actual triple matching.

        Args:
          - triple_pattern: The triple pattern (s, p, o) to search.
          - context: The query execution context.

        Returns: An iterator that produces RDF triples matching the input triple pattern.
        """
        subject, predicate, obj = triple_pattern
        for chunk_df in self.dss_dataset.iter_dataframes(
            columns=self.dataframe_columns
        ):
            # use a dataframe query to do the triple pattern matching
            queries = []
            if subject != ANY:
                queries.append(f"{self.subject_column_name} == '{subject.n3()}'")
            if predicate != ANY:
                queries.append(f"{self.predicate_column_name} == '{predicate.n3()}'")
            if obj != ANY:
                queries.append(f"{self.object_column_name} == '{obj.n3()}'")

            for _, row in chunk_df.query(" and ".join(queries)).iterrows():
                row_subject = from_n3(row[self.subject_column_name])
                row_predicate = from_n3(row[self.predicate_column_name])
                row_object = from_n3(row[self.object_column_name])

                yield (row_subject, row_predicate, row_object), None
        return

    def create(self, configuration):
        pass  # no effect, as the DSS dataset is already created

    def add(self, triple, context=None, quoted=False):
        self.staging_df[len(self.staging_df)] = [
            triple[0].n3(),
            triple[1].n3(),
            triple[2].n3(),
        ]
        self.dispatcher.dispatch(TripleAddedEvent(triple=triple, context=context))
        if self.staging_df.shape[0] >= self.autocommit_add_threshold:
            self.commit()

    def commit(self):
        # write the staging dataframe to the output dataset, then clear it
        self.dss_dataset.write_dataframe(self.staging_df)
        self.staging_df = self.staging_df[:0]

    def remove(self, _, context):
        raise TypeError("The store is append only!")

    def destroy(self, configuration):
        raise TypeError("The store is append only!")
