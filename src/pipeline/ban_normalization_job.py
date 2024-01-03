import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from src.metric.counter import CountingTransform
from src.metric.utils import get_counter
from src.pipeline.utils import (AssignBatchKey, ParseCsvLine,
                                fetch_address_from_reverse_api,
                                finalize_output,
                                process_reverse_geocoding_response,
                                process_search_api_response, send_batch_to_api)


def run_pipeline(input_csv, output_uri, batch_size=1000):
    # Create the Beam pipeline
    with beam.Pipeline(options=PipelineOptions()) as p:
        # Read data from a CSV file, skipping the header line
        rows = p | "ReadCSV" >> beam.io.ReadFromText(input_csv, skip_header_lines=1)
        rows | "Count Input" >> CountingTransform("input_count")

        # Parse each line of the CSV file
        parsed_rows = rows | "ParseCSV" >> beam.ParDo(ParseCsvLine())

        # Create mini-batches of CSV lines
        batched_rows = (
            parsed_rows
            | "AssignBatchKey" >> beam.ParDo(AssignBatchKey(batch_size))
            | "GroupIntoBatches" >> beam.GroupByKey()
            | "RemoveBatchKey" >> beam.Map(lambda kv: kv[1])  # Remove the batch key
        )

        # Send mini-batches to the API and receive responses
        api_responses = batched_rows | "SendBatchToAPI" >> beam.FlatMap(send_batch_to_api)

        # Process the responses from the API
        processed_rows = api_responses | "ProcessSearchApiResponse" >> beam.Map(process_search_api_response)
        # Filter out the responses with 'ok' status
        ok_responses = processed_rows | "FilterForOk" >> beam.Filter(lambda x: not x[1])

        # Process non-ok responses separately using reverse geocoding
        non_ok_responses = (
            api_responses
            | "FilterForNonOk" >> beam.Filter(lambda x: x["result_status"] != "ok")
            | "BatchNonOkElements" >> beam.BatchElements(min_batch_size=100, max_batch_size=500)
            | "FetchAddressesFromReverseApi" >> beam.FlatMap(fetch_address_from_reverse_api)
            | "ProcessFromReverseApi" >> beam.Map(process_reverse_geocoding_response)
        )

        # Combine the ok and non-ok responses
        final_output = (ok_responses, non_ok_responses) | "FlattenPCollection" >> beam.Flatten()
        final_output | "Count Output" >> CountingTransform("output_count")

        # Format the final output and write to a CSV file
        final_output | "FinalizeOutput" >> beam.Map(finalize_output) | "WriteToCSV" >> beam.io.WriteToText(
            output_uri, file_name_suffix=".csv"
        )

        # Run the pipeline and wait for it to finish
        result = p.run()
        result.wait_until_finish()
        print(f"Input Count: {get_counter(result,'input_count')} ")
        print(f"Output Count: {get_counter(result,'output_count')} ")


if __name__ == "__main__":
    # Set parameters and run the pipeline
    size = 1000
    input = "src/resources/sample.csv"
    output = "output/normalized_addresses"
    run_pipeline(input, output, size)
