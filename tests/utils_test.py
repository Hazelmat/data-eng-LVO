import unittest

from src.pipeline.utils import (finalize_output,
                                process_reverse_geocoding_response,
                                process_search_api_response)


class TestProcessSearchApiResponse(unittest.TestCase):
    def test_process_search_api_response_ok(self):
        element = {
            "result_status": "ok",
            "result_name": "123 rue",
            "result_city": "Paris",
            "result_postcode": "12345",
            "longitude": "0.123",
            "latitude": "0.456",
        }
        processed_element, is_non_ok = process_search_api_response(element)
        self.assertEqual(processed_element["adresse"], "123 rue")
        self.assertEqual(processed_element["ville"], "Paris")
        self.assertEqual(processed_element["code_postal"], "12345")
        self.assertEqual(is_non_ok, False)

    def test_process_search_api_response_non_ok(self):
        element = {"result_status": "non-ok"}
        processed_element, is_non_ok = process_search_api_response(element)
        self.assertEqual(is_non_ok, True)


class TestProcessReverseGeocodingResponse(unittest.TestCase):
    def test_process_reverse_geocoding_response_ok(self):
        element = {
            "result_status": "ok",
            "result_name": "123 rue",
            "result_city": "Paris",
            "result_postcode": "12345",
            "longitude": "0.123",
            "latitude": "0.456",
        }
        processed_element, is_non_ok = process_reverse_geocoding_response(element)
        self.assertEqual(processed_element["adresse"], "123 rue")
        self.assertEqual(processed_element["ville"], "Paris")
        self.assertEqual(processed_element["code_postal"], "12345")
        self.assertEqual(is_non_ok, False)


class TestFinalizeOutput(unittest.TestCase):
    def test_finalize_output(self):
        element = {
            "identifiant_unique": "id123",
            "adresse": "123 rue",
            "adresse_complement": "",
            "code_postal": "12345",
            "ville": "Paris",
            "st_x": "0.123",
            "st_y": "0.456",
        }
        expected_output = "id123,123 rue,,12345,Paris,0.123,0.456"
        self.assertEqual(finalize_output((element, None)), expected_output)


if __name__ == "__main__":
    unittest.main()
