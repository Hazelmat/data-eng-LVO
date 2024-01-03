import apache_beam as beam
from apache_beam.metrics import Metrics


class CountingTransform(beam.PTransform):
    """
    A PTransform for counting the number of elements in a PCollection.

    This transform increments a counter for each element in the input PCollection,
    and outputs the same PCollection allowing for further transformations or sinks.

    Attributes:
        counter_name (str): The name of the counter.
    """

    def __init__(self, counter_name):
        """
        Initializes the CountingTransform with the given counter name.

        :param str counter_name: The name of the counter.
        """
        super().__init__()
        self.counter_name = counter_name

    def expand(self, pcoll):
        def count_element(element, counter_name):
            """
            Increments the counter and returns the original element.

            :param element: The input element from the PCollection.
            :param str counter_name: The name of the counter.
            :return: The same element that was input.
            """
            Metrics.counter("group", counter_name).inc()
            return element

        return pcoll | "Count " + self.counter_name >> beam.Map(count_element, self.counter_name)
