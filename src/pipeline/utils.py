import csv
import io

import apache_beam as beam
import requests


def process_search_api_response(element):
    """
    Process each element returned from the search API and update address information.

    Args:
        element (dict): A dictionary containing data from the search API response.

    Returns:
        tuple: The updated element and a boolean indicating if the status is not 'ok'.
    """
    is_non_ok = element["result_status"] != "ok"
    if not is_non_ok:
        # Update address and city from the response
        element["adresse"] = element["result_name"]
        element["ville"] = element["result_city"]
        element["code_postal"] = element["result_postcode"]
        element["adresse_complement"] = element["adresse_complement"]
        element["st_x"] = element["longitude"]
        element["st_y"] = element["latitude"]
    return element, is_non_ok


def process_reverse_geocoding_response(element):
    """
    Process each element returned from the reverse geocoding API.

    Args:
        element (dict): A dictionary containing data from the reverse geocoding API response.

    Returns:
        tuple: The updated element and a boolean indicating if the status is not 'ok'.
    """
    is_non_ok = element["result_status"] != "ok"
    if not is_non_ok:
        # Update address and city from the response
        element["adresse"] = element["result_name"]
        element["ville"] = element["result_city"]
        element["code_postal"] = element["result_postcode"]
        element["st_x"] = element["longitude"]
        element["st_y"] = element["latitude"]
    return element, is_non_ok


def fetch_address_from_reverse_api(non_ok_elements):
    """
    Fetch address data from the reverse geocoding API for elements with non 'ok' status.

    Args:
        non_ok_elements (list): A list of elements with non 'ok' status.

    Returns:
        list: A list of updated elements with address information from the reverse geocoding API.
    """
    reverse_geocoding_data = prepare_reverse_geocoding_data(non_ok_elements)
    response = requests.post("https://api-adresse.data.gouv.fr/reverse/csv/", files={"data": reverse_geocoding_data})
    if response.status_code == 200:
        reader = csv.DictReader(io.StringIO(response.text))
        return [row for row in reader]
    else:
        return non_ok_elements


def send_batch_to_api(batch):
    """
    Send a batch of CSV lines to the geocoding API and return the response.

    Args:
        batch (list): A batch of elements to be sent to the API.

    Returns:
        list: A list of dictionaries containing the API response.
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["identifiant_unique", "adresse", "adresse_complement", "code_postal", "ville", "st_x", "st_y"])
    for element in batch:
        row = element
        writer.writerow(row)
    output.seek(0)
    response = requests.post(
        "https://api-adresse.data.gouv.fr/search/csv/",
        files={"data": output.getvalue()},
        data={"columns": ["adresse", "ville", "adresse_complement"], "postcode": "code_postal"},
    )
    reader = csv.DictReader(io.StringIO(response.text))
    return [row for row in reader]


def finalize_output(element):
    """
    Format the final output element into a CSV line.

    Args:
        element (dict): A dictionary representing a single record.

    Returns:
        str: A CSV-formatted string of the record.
    """
    element, _ = element
    fields = ["identifiant_unique", "adresse", "adresse_complement", "code_postal", "ville", "st_x", "st_y"]

    formatted_element = []
    for field in fields:
        if field in ["st_x", "st_y", "code_postal"]:
            # Do not add quotes
            formatted_element.append(str(element.get(field, "")))
        else:
            # Add quotes for other fields
            value = str(element.get(field, "")).replace('"', '""')  # Escape existing quotes in the value
            formatted_element.append(f'"{value}"')

    return ",".join(formatted_element)


class ParseCsvLine(beam.DoFn):
    """
    A DoFn for parsing each line of the input CSV file.
    """

    def process(self, line):
        reader = csv.reader(io.StringIO(line))
        for parsed_line in reader:
            yield parsed_line


class AssignBatchKey(beam.DoFn):
    """
    A DoFn for assigning a batch key to each element based on the batch size.

    Args:
        batch_size (int): The size of each batch.
    """

    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.counter = 0

    def process(self, element):
        batch_key = self.counter // self.batch_size
        self.counter += 1
        yield (batch_key, element)


def prepare_reverse_geocoding_data(non_ok_elements):
    """
    Prepare data for reverse geocoding by creating a CSV format with required headers.

    Args:
        non_ok_elements (list): A list of elements with non 'ok' status.

    Returns:
        str: CSV formatted string of latitude and longitude for reverse geocoding.
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["identifiant_unique", "latitude", "longitude"])
    for element in non_ok_elements:
        writer.writerow([element["identifiant_unique"], element["st_y"], element["st_x"]])
    output.seek(0)
    return output.getvalue()
