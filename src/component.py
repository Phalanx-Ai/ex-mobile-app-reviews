import csv
import logging
import requests
import json
import sys

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_USERNAME = 'username'
KEY_PASSWORD = '#password'
KEY_SERVER_HOSTNAME = 'hostname'
KEY_APPLICATIONS = 'applications'

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD, KEY_APPLICATIONS, KEY_SERVER_HOSTNAME]
REQUIRED_IMAGE_PARS = []

DATE_FROM = '2000-01-01'


def login(email, password, hostname):
    response = requests.request(
        "POST",
        "https://%s/api/token/new" % (hostname),
        data=dict(
            email=email,
            password=password,
        )
    )

    if not response.status_code == 200:
        logging.error("Unable to login to Sirius API")
        sys.exit(1)

    return json.loads(response.text)


def get_data(token, hostname, params):
    response = requests.request(
        "GET",
        "https://%s/api/reviews/getAllReviewsWithoutPaging" % (hostname),
        params=params,
        headers={
            'Content-Type': "application/json",
            'cache-control': "no-cache",
            "Authorization": "Bearer " + token["access"],
        }
    )

    if not response.status_code == 200:
        logging.error("Unable to post data: %s" % (response.text))
        sys.exit(2)

    return response


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        if len(self.configuration.tables_output_mapping) != 1:
            logging.error("Output table mapping with one entry is required")
            sys.exit(1)

        token = login(
            params[KEY_USERNAME],
            params[KEY_PASSWORD],
            params[KEY_SERVER_HOSTNAME]
        )

        applications = params[KEY_APPLICATIONS].split(",")

        response = get_data(
            token,
            params[KEY_SERVER_HOSTNAME],
            {
                'application': applications,
                'dateFrom': DATE_FROM
            }
        )
        reviews = json.loads(response.text).get('results')

        records = []
        for review in reviews:
            rec = {
                    'app_name': review['app_var']['name'],
                    'platform': review['app_var']['platform'],
                    'device_manufacturer': review['content']['device_manufacturer'],
                    'device_model': review['content']['device_model'],
                    'review_polarity': review['content'].get('polarity', None),
                    'review_tags': review['content'].get('tags', None),
                    'review_score': review['content']['score'],
                    'review_text': review['content']['text'],
                    'review_author': review['user_name'],
                    'review_time': review['content']['review_time']
            }
            if (review.get('response') is None):
                rec.update({
                    'response_time': None,
                    'response_text': None,
                    'response_author': None,
                })
            else:
                rec.update({
                    'response_time': review.get('response', {}).get('end_time', None),
                    'response_text': review.get('response', {}).get('text', None),
                    'response_author': review.get('response', {}).get('user', {}).get('email', None),
                })
            records.append(rec)

        result_filename = self.configuration.tables_output_mapping[0]['source']
        table = self.create_out_table_definition(
            result_filename,
        )

        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(
                out_file,
                fieldnames=[
                    'app_name', 'platform', 'device_manufacturer', 'device_model',
                    'review_polarity', 'review_tags', 'review_score', 'review_text',
                    'review_author', 'review_time', 'response_time', 'response_text',
                    'response_author'
                ]
            )
            writer.writeheader()
            writer.writerows(records)

        self.write_manifest(table)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
