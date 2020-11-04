import unittest

# import mock

from airflow.providers.greatexpectations.operators.greatexpectations_bigquery import GreatExpectationsBigQueryOperator


class TestGreatExpectationsBigQueryOperator(unittest.TestCase):
    # @mock.patch('airflow.providers.greatexpectations.operators.greatexpectations_bigquery')
    def test_get_temp_table_name(self):
        operator = GreatExpectationsBigQueryOperator(
            task_id='tst',
            gcp_project='nyt-adtech-dev', expectations_file_name='warning.json',
            gcs_bucket='great-expectations-nyt-adtech-dev',
            gcs_validations_prefix='validations',
            gcs_datadocs_prefix='data-docs',
            validation_type='TABLE',
            validation_type_input="eddie_data.staq_test",
            gcs_expectations_prefix='expectations',
            bq_dataset_name='temp',
            email_to='brian.lavery@nytimes.com',
            datadocs_domain='none', send_alert_email=True,
            datadocs_link_in_email=False,
            fail_if_expectations_not_met=True, bigquery_conn_id='bigquery_default'
        )

        table_name = operator.get_temp_table_name('tmp_', 10)
        assert table_name is not None
