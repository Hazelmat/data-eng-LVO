import apache_beam as beam


def get_counter(result, counter_name):
    counter = result.metrics().query(beam.metrics.metric.MetricsFilter().with_name(counter_name))
    return counter["counters"][0].committed
