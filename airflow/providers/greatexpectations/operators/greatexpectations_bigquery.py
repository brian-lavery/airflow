#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import datetime
import logging
import uuid
import enum
from urllib.parse import urlsplit

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.utils.email import send_email

from airflow.providers.greatexpectations.operators.great_expectations_base import GreatExpectationsOperator
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext

log = logging.getLogger(__name__)


class GreatExpectationsValidations(enum.Enum):
    SQL = "SQL"
    TABLE = "TABLE"


class GreatExpectationsBigQueryOperator(GreatExpectationsOperator):
    """
         Use Great Expectations to validate data expectations against a BigQuery table or the result of a SQL query.
         The expectations need to be stored in a JSON file sitting in an accessible GCS bucket.  The validation results
         are output to GCS in both JSON and HTML formats.
         Here's the current list of expectations types:
         https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html
         Here's how to create expectations files:
         https://docs.greatexpectations.io/en/latest/guides/tutorials/how_to_create_expectations.html
        :param gcp_project:  The GCP project which houses the GCS buckets where the expectations files are stored
            and where the validation files & data docs will be output (e.g. HTML docs showing if the data matches
            expectations).
        :type gcp_project: str
        :param expectations_suite_name: The name of the expectations suite containing the expectations for the data.
            The suite should be in a JSON file with the same name as the suite (e.g. if the expectations suite named
            in the expectations file is 'my_suite' then the expectations file should be called my_suite.json)
        :type expectations_suite_name: str
        :param gcs_bucket:  Google Cloud Storage bucket where expectation files are stored and where validation outputs
            and data docs will be saved.
            (e.g. gs://<gcs_bucket>/<gcs_expectations_prefix>/<expectations_file_name>
                  gs://mybucket/myprefix/myexpectationsfile.json )
        :type gcs_bucket: str
        :param gcs_expectations_prefix:  Google Cloud Storage prefix where the expectations file can be found.
            (e.g. 'ge/expectations')
        :type gcs_expectations_prefix: str
        :param gcs_validations_prefix:  Google Cloud Storage prefix where the validation output files should be saved.
            (e.g. 'ge/validations')
        :type gcs_validations_prefix: str
        :param gcs_datadocs_prefix:  Google Cloud Storage prefix where the validation datadocs files should be saved.
            (e.g. 'ge/datadocs')
        :type gcs_datadocs_prefix: str
        :param validation_type: For the set of data to be validated (i.e. compared against expectations), is it already
            sitting in a BigQuery table or do you want to validate the data returned by a SQL query?  The options are
            'TABLE' or 'SQL'.
        :type validation_type: str
        :param validation_type_input:  The name of the BigQuery table (dataset_name.table_name) if the validation_type
            is 'TABLE' or the SQL query string if the validation_type is 'SQL'.
        :type validation_type_input: str
        :param bigquery_conn_id: Name of the BigQuery connection that contains the connection and credentials
            info needed to connect to BigQuery.
        :type bigquery_conn_id: str
        :param bq_dataset_name:  The name of the BigQuery data set where any temp tables will be created that are needed
            as part of the GE validation process.
        :type bq_dataset_name: str
        :param send_alert_email:  Send an alert email if one or more expectations fail to be met?  Defaults to True.
        :type send_alert_email: boolean
        :param datadocs_link_in_email:  Include in the alert email a link to the data doc in GCS that shows the
            validation results?  Defaults to False because there's extra setup needed to serve HTML data docs stored in
            GCS.  When set to False, only a GCS path to the results are included in the email.
            Set up a GAE app to serve the data docs if you want a clickable link for the data doc to be included in the
            email.  See here for set up instructions:
            https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.html
        :type datadocs_link_in_email: boolean
        :param datadocs_domain: The domain from which the data docs are set up to be served (e.g. ge-data-docs-dot-my-gcp-project.ue.r.appspot.com).
            This only needs to be set if datadocs_link_in_email is set to True.
        :type datadocs_domain: str
        :param email_to:  Email address to receive any alerts when expectations are not met.
        :type email_to: str
        :param fail_if_expectations_not_met: Fail the Airflow task if expectations are not met?  Defaults to True.
        :type fail_if_expectations_not_met: boolean
    """

    _EMAIL_CONTENT = '''
            <html>
              <head>
                <meta charset="utf-8">
              </head>
              <body style="background-color: #fafafa; font-family: Roboto, sans-serif=;">
                <div style="width: 600px; margin:0 auto;">
                    <div style="background-color: white; border-top: 4px solid #22a667; border-left: 1px solid #eee; border-right: 1px solid #eee; border-radius: 6px 6px 0 0; height: 24px;"></div>
                        <div style="background-color: white; border-left: 1px solid #eee; border-right: 1px solid #eee; padding: 0 24px; overflow: hidden;">
                          <div style="margin-left: 35px;">
                            Great Expectations Alert<br>
                            One or more data expectations were not met in the {0} file. {1}
                       </div>
              </body>
            </html>
            '''

    @apply_defaults
    def __init__(self, *, gcp_project, expectation_suite_name, gcs_bucket, gcs_expectations_prefix,
                 gcs_validations_prefix, gcs_datadocs_prefix, validation_type, validation_type_input,
                 bq_dataset_name, email_to, datadocs_domain='none', send_alert_email=True,
                 datadocs_link_in_email=False,
                 fail_if_expectations_not_met=True, bigquery_conn_id='bigquery_default', **kwargs):

        great_expectations_valid_type = set(item.value for item in GreatExpectationsValidations)

        if validation_type.upper() not in GreatExpectationsValidations.__members__:
            raise AirflowException(f"argument 'validation_type' must be one of {great_expectations_valid_type}")
        self.validation_type = validation_type
        self.validation_type_input = validation_type_input
        self.bigquery_conn_id = bigquery_conn_id
        self.bq_dataset_name = bq_dataset_name
        self.email_to = email_to
        self.gcp_project = gcp_project
        self.gcs_bucket = gcs_bucket
        self.gcs_expectations_prefix = gcs_expectations_prefix
        self.gcs_validations_prefix = gcs_validations_prefix
        self.gcs_datadocs_prefix = gcs_datadocs_prefix
        self.datadocs_domain = datadocs_domain
        self.send_alert_email = send_alert_email
        self.datadocs_link_in_email = datadocs_link_in_email
        self.fail_if_expectations_not_met = fail_if_expectations_not_met

        # Create a data context and batch_kwargs that will then be handed off to the base operator to do the
        # data validation against expectations.
        data_context_config = self.create_data_context_config()
        data_context = BaseDataContext(project_config=data_context_config)
        batch_kwargs = self.get_batch_kwargs()
        super().__init__(data_context=data_context, batch_kwargs=batch_kwargs,
                         expectation_suite_name=expectation_suite_name, fail_task_on_validation_failure=False,
                         **kwargs)

    def create_data_context_config(self):
        # Get the credentials information for the BigQuery data source from the BigQuery Airflow connection
        conn = BaseHook.get_connection(self.bigquery_conn_id)
        connection_json = conn.extra_dejson
        credentials_path = connection_json['extra__google_cloud_platform__key_path']
        data_context_config = DataContextConfig(
            config_version=2,
            datasources={
                "bq_datasource": {
                    "credentials": {
                        "url": "bigquery://" + self.gcp_project + "/" + self.bq_dataset_name + "?credentials_path=" +
                               credentials_path
                    },
                    "class_name": "SqlAlchemyDatasource",
                    "module_name": "great_expectations.datasource",
                    "data_asset_type": {
                        "module_name": "great_expectations.dataset",
                        "class_name": "SqlAlchemyDataset"
                    }
                }
            },
            expectations_store_name="expectations_GCS_store",
            validations_store_name="validations_GCS_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            plugins_directory=None,
            validation_operators={
                "action_list_operator": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "store_evaluation_params",
                            "action": {"class_name": "StoreEvaluationParametersAction"},
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ],
                }
            },
            stores={
                'expectations_GCS_store': {
                    'class_name': 'ExpectationsStore',
                    'store_backend': {
                        'class_name': 'TupleGCSStoreBackend',
                        'project': self.gcp_project,
                        'bucket': self.gcs_bucket,
                        'prefix': self.gcs_expectations_prefix
                    }
                },
                'validations_GCS_store': {
                    'class_name': 'ValidationsStore',
                    'store_backend': {
                        'class_name': 'TupleGCSStoreBackend',
                        'project': self.gcp_project,
                        'bucket': self.gcs_bucket,
                        'prefix': self.gcs_validations_prefix
                    }
                },
                "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            },
            data_docs_sites={
                "GCS_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "project": self.gcp_project,
                        "bucket": self.gcs_bucket,
                        'prefix': self.gcs_datadocs_prefix
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                    },
                }
            },
            config_variables_file_path=None,
            commented_map=None,
        )
        return data_context_config

    def get_batch_kwargs(self):
        # Tell GE where to fetch the batch of data to be validated.
        batch_kwargs = {
            "datasource": "bq_datasource",
        }

        if self.validation_type == GreatExpectationsValidations.SQL.value:
            batch_kwargs["query"] = self.validation_type_input
            batch_kwargs["data_asset_name"] = self.bq_dataset_name
            batch_kwargs["bigquery_temp_table"] = self.get_temp_table_name(
                'temp_ge_' + datetime.datetime.now().strftime('%Y%m%d') + '_', 10)
        elif self.validation_type == GreatExpectationsValidations.TABLE.value:
            batch_kwargs["table"] = self.validation_type_input
            batch_kwargs["data_asset_name"] = self.bq_dataset_name

        self.log.info("batch_kwargs: " + str(batch_kwargs))

        return batch_kwargs

    # Generate a unique name for a temporary table.  For example, if desired_prefix= 'temp_ge_' and
    # desired_length_of_random_portion = 10 then the following table name might be generated: 'temp_ge_304kcj39rM'.
    def get_temp_table_name(self, desired_prefix, desired_length_of_random_portion):
        random_string = str(uuid.uuid4().hex)
        random_portion_of_name = random_string[:desired_length_of_random_portion]
        full_name = desired_prefix + random_portion_of_name
        log.info("Generated name for temporary table: %s", full_name)
        return full_name

    def execute(self, context):
        # Execute base operator's validation process
        results = super().execute()
        validation_result_identifier = list(results['run_results'].keys())[0]
        # For the given validation_result_identifier, get a link to the data docs that were generated by Great
        # Expectations as part of the validation.
        data_docs_url = \
            self.data_context.get_docs_sites_urls(resource_identifier=validation_result_identifier,
                                                  site_name='GCS_site')[0][
                'site_url']
        self.log.info("Data docs url is: %s", data_docs_url)
        if results["success"]:
            self.log.info('All expectations met')
        else:
            self.log.info('One or more expectations were not met.')
            if self.send_alert_email:
                self.log.info('Sending alert email...')
                self.send_alert(data_docs_url)
            if self.fail_if_expectations_not_met:
                raise AirflowException('One or more expectations were not met')

    def send_alert(self, data_docs_url='none'):
        results = self._format_email(data_docs_url)
        email_content = self._EMAIL_CONTENT.format(self.expectation_suite_name, results)
        send_email(self.email_to, 'expectations in suite ' + self.expectation_suite_name + ' not met', email_content,
                   files=None, cc=None, bcc=None,
                   mime_subtype='mixed', mime_charset='us_ascii')

    def _format_email(self, data_docs_url):
        # If data_docs_url is set to 'none' then only a generic warning will be included in the email.  No clickable
        # link or path to the data docs will be included.
        if data_docs_url != 'none':
            # A data docs url was passed in so a form of it will be added to the email as either a clickable link
            # or just a non-clickable directory path.
            if self.datadocs_link_in_email:
                # Get the domain name of the service serving the data docs.
                if self.datadocs_domain == 'none':
                    raise AirflowException(
                        "datadocs_link_in_email is set to true but datadocs_domain is 'none'.  datadocs_domain should be set to the domain from which datadocs are being served")
                # Replace the domain returned by ge with the domain set up to serve the data docs
                parsed = urlsplit(data_docs_url)
                new_url = parsed._replace(netloc=self.datadocs_domain)
                results = '  see the results <a href=' + new_url.geturl() + '>here</a>.'
            else:
                # From the data docs url, pull out just the directory path to the data docs and send it to the users in the email.
                parsed = urlsplit(data_docs_url)
                results = '  See the following location for results:' + parsed.path
        else:
            # No link or path to the results will be included in the email
            results = ''
        return results
