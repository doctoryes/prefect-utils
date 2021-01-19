import xml.etree.cElementTree as ET
from collections import OrderedDict
from io import StringIO
from unittest import TestCase

import httpretty
from ddt import data, ddt, unpack

from prefect_utils.paypal_xml import (ColumnMetadata,
                                      PaypalApiRequestFailedError,
                                      PaypalMalformedResponseError,
                                      PaypalReportDataRequest,
                                      PaypalReportMetadataRequest,
                                      PaypalReportRequest,
                                      PaypalReportResultsRequest,
                                      SettlementReportRecord)

TEST_URL = 'http://test.api/endpoint'


class XmlRequestMixin:

    SAMPLE_RESPONSE = ''

    def setUp(self):
        super().setUp()
        self.response_xml_root = ET.fromstring(self.SAMPLE_RESPONSE)

    def on_post_return_xml(self):
        element_tree = ET.ElementTree(self.response_xml_root)
        string_buffer = StringIO()
        element_tree.write(string_buffer, encoding='unicode', xml_declaration=True)
        response_xml_root_string = string_buffer.getvalue()
        httpretty.register_uri(httpretty.POST, TEST_URL, response_xml_root_string)

    def remove_xml_node(self, path):
        element = self.response_xml_root.findall(path)[0]
        parent = self.response_xml_root.findall(path + '/..')[0]
        parent.remove(element)

    def set_xml_node_text(self, path, value):
        element = self.response_xml_root.findall(path)[0]
        element.text = str(value)

    def parse_request_xml(self):
        http_request = httpretty.last_request()
        self.assertEqual(http_request.method, "POST")
        self.assertEqual(http_request.headers['Content-type'], 'text/plain')
        self.request_xml_root = ET.fromstring(http_request.body)

    def assert_request_xml_equals(self, path, expected_xml):
        element = self.request_xml_root.findall(path)[0]
        expected_root = ET.fromstring(expected_xml)
        self.assert_nodes_equal(expected_root, element, element.tag)

    def assert_nodes_equal(self, expected, actual, path):
        if len(actual) == 0:
            self.assertEqual(
                expected.text,
                actual.text,
                "Text does not match at path '{path}', expected: {exp}, actual: {act}".format(
                    path=path,
                    exp=expected.text,
                    act=actual.text
                )
            )
        expected_children_tags = [c.tag for c in expected]
        actual_children_tags = [c.tag for c in actual]

        self.assertEqual(
            expected_children_tags,
            actual_children_tags,
            "Children do not match at path '{path}', expected: {exp}, actual: {act}".format(
                path=path,
                exp=expected_children_tags,
                act=actual_children_tags
            )
        )
        for expected_child, actual_child in zip(expected, actual):
            self.assert_nodes_equal(expected_child, actual_child, path + '/' + expected_child.tag)


@ddt
@httpretty.activate
class TestReportRequest(XmlRequestMixin, TestCase):

    SAMPLE_RESPONSE = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<reportingEngineResponse>
    <baseResponse>
        <responseCode>100</responseCode>
        <responseMsg>Request has completed successfully</responseMsg>
    </baseResponse>
    <runReportResponse>
        <reportId>RE12345690123</reportId>
        <statusCode>3</statusCode>
        <statusMsg>Report has completed successfully</statusMsg>
    </runReportResponse>
</reportingEngineResponse>
"""

    def create_report_request(self):
        return PaypalReportRequest(
            "FooReport",
            "testpartner",
            "testvendor",
            "notsosecret",
            "testuser",
            TEST_URL,
            report_date="2015-08-28"
        )

    def test_complete_status(self):
        self.on_post_return_xml()
        report_request = self.create_report_request()
        response = report_request.execute()

        self.assertEqual(response.response_code, 100)
        self.assertEqual(response.response_message, 'Request has completed successfully')
        self.assertEqual(response.report_id, 'RE12345690123')
        self.assertEqual(response.status_code, 3)
        self.assertEqual(response.status_message, 'Report has completed successfully')
        self.assertFalse(response.is_running)
        self.assertTrue(response.is_ready)

        self.parse_request_xml()
        self.assert_request_xml_equals('.', """\
<reportingEngineRequest>
    <authRequest>
        <user>testuser</user>
        <vendor>testvendor</vendor>
        <partner>testpartner</partner>
        <password>notsosecret</password>
    </authRequest>
    <runReportRequest>
        <reportName>FooReport</reportName>
        <reportParam>
            <paramName>report_date</paramName>
            <paramValue>2015-08-28</paramValue>
        </reportParam>
        <pageSize>50</pageSize>
    </runReportRequest>
</reportingEngineRequest>
""")

    @data(
        (1, 'Report has been created'),
        (2, 'Report is currently executing')
    )
    @unpack
    def test_executing_status(self, status_code, status_msg):
        self.set_xml_node_text('./runReportResponse/statusCode', status_code)
        self.set_xml_node_text('./runReportResponse/statusMsg', status_msg)
        self.on_post_return_xml()

        report_request = self.create_report_request()
        response = report_request.execute()

        self.assertEqual(response.status_code, status_code)
        self.assertEqual(response.status_message, status_msg)
        self.assertTrue(response.is_running)
        self.assertFalse(response.is_ready)

    def test_missing_base_response(self):
        self.remove_xml_node('./baseResponse')
        self.on_post_return_xml()

        report_request = self.create_report_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, 'baseResponse'):
            report_request.execute()

    @data('responseCode', 'responseMsg')
    def test_missing_base_response_field(self, field_name):
        self.remove_xml_node('./baseResponse/' + field_name)
        self.on_post_return_xml()

        report_request = self.create_report_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, field_name):
            report_request.execute()

    @data(
        (101, 'Request has failed'),
        (102, 'An internal scheduler error has occurred'),
        (103, 'Unknown report requested'),
        (104, 'Invalid Report ID'),
        (105, 'A system error has occurred'),
        (106, 'A database error has occurred'),
        (107, 'Invalid XML request'),
        (108, 'User authentication failed'),
        (109, 'Invalid report parameters provided'),
        (110, 'Invalid merchant account'),
        (111, 'Invalid page number'),
        (112, 'Template already exists'),
        (113, 'Unknown template requested')
    )
    @unpack
    def test_unsuccessful_request(self, response_code, response_msg):
        self.set_xml_node_text('./baseResponse/responseCode', response_code)
        self.set_xml_node_text('./baseResponse/responseMsg', response_msg)
        self.on_post_return_xml()

        report_request = self.create_report_request()
        with self.assertRaisesRegex(
                PaypalApiRequestFailedError, 'API request failed with code {}: {}'.format(
                    response_code,
                    response_msg
                )
        ):
            report_request.execute()

    def test_missing_report_response(self):
        self.remove_xml_node('./runReportResponse')
        self.on_post_return_xml()

        report_request = self.create_report_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, 'runReportResponse'):
            report_request.execute()

    @data(
        (4, 'Report has failed'),
        (5, 'Report has expired'),
        (6, 'Report has expired')
    )
    @unpack
    def test_unsuccessful_report_generation_attempt(self, status_code, status_msg):
        self.set_xml_node_text('./runReportResponse/statusCode', status_code)
        self.set_xml_node_text('./runReportResponse/statusMsg', status_msg)
        self.on_post_return_xml()

        report_request = self.create_report_request()
        with self.assertRaisesRegex(
                PaypalApiRequestFailedError, 'report request failed with code {}: {}'.format(
                    status_code,
                    status_msg
                )
        ):
            report_request.execute()

    @data('reportId', 'statusCode', 'statusMsg')
    def test_missing_base_response_field_run_report_Response(self, field_name):
        self.remove_xml_node('./runReportResponse/' + field_name)
        self.on_post_return_xml()

        report_request = self.create_report_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, field_name):
            report_request.execute()


@ddt
@httpretty.activate
class TestReportMetadataRequest(XmlRequestMixin, TestCase):

    SAMPLE_RESPONSE = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<reportingEngineResponse>
    <baseResponse>
        <responseCode>100</responseCode>
        <responseMsg>Request has completed successfully</responseMsg>
    </baseResponse>
    <getMetaDataResponse>
        <reportId>RE1234567890</reportId>
        <numberOfRows>102</numberOfRows>
        <numberOfPages>3</numberOfPages>
        <pageSize>50</pageSize>
        <numberOfColumns>3</numberOfColumns>
        <columnMetaData colNum="1">
            <dataName>Transaction ID</dataName>
            <dataType>string</dataType>
        </columnMetaData>
        <columnMetaData colNum="2">
            <dataName>Time</dataName>
            <dataType>date</dataType>
        </columnMetaData>
        <columnMetaData colNum="3">
            <dataName>Type</dataName>
            <dataType>string</dataType>
        </columnMetaData>
    </getMetaDataResponse>
</reportingEngineResponse>
"""

    def create_report_metadata_request(self):
        return PaypalReportMetadataRequest(
            "RE1234567890",
            "testpartner",
            "testvendor",
            "notsosecret",
            "testuser",
            TEST_URL
        )

    def test_successful_response(self):
        self.on_post_return_xml()

        request = self.create_report_metadata_request()
        response = request.execute()

        self.assertEqual(response.response_code, 100)
        self.assertEqual(response.response_message, 'Request has completed successfully')
        self.assertEqual(response.num_rows, 102)
        self.assertEqual(response.num_pages, 3)
        self.assertEqual(response.page_size, 50)
        self.assertEqual(
            response.columns.popitem(last=False),
            ('Transaction ID', ColumnMetadata(name='Transaction ID', data_type='string'))
        )
        self.assertEqual(response.columns.popitem(last=False), ('Time', ColumnMetadata(name='Time', data_type='date')))
        self.assertEqual(
            response.columns.popitem(last=False),
            ('Type', ColumnMetadata(name='Type', data_type='string'))
        )

        self.parse_request_xml()
        self.assert_request_xml_equals('./getMetaDataRequest', """\
<getMetaDataRequest>
    <reportId>RE1234567890</reportId>
</getMetaDataRequest>
""")

    @data('numberOfRows', 'numberOfPages', 'pageSize')
    def test_missing_required_node(self, field_name):
        self.remove_xml_node('./getMetaDataResponse/' + field_name)
        self.on_post_return_xml()

        request = self.create_report_metadata_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, field_name):
            request.execute()

    @data('dataName', 'dataType')
    def test_malformed_column_metadata(self, field_name):
        self.remove_xml_node('./getMetaDataResponse/columnMetaData[@colNum="1"]/' + field_name)
        self.on_post_return_xml()

        request = self.create_report_metadata_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, field_name):
            request.execute()

    def test_missing_metadata_response(self):
        self.remove_xml_node('./getMetaDataResponse')
        self.on_post_return_xml()

        request = self.create_report_metadata_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, 'getMetaDataResponse'):
            request.execute()


@ddt
@httpretty.activate
class TestReportDataRequest(XmlRequestMixin, TestCase):

    SAMPLE_RESPONSE = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<reportingEngineResponse>
    <baseResponse>
        <responseCode>100</responseCode>
        <responseMsg>Request has completed successfully</responseMsg>
    </baseResponse>
    <getDataResponse>
        <reportId>RE1234567890</reportId>
        <reportDataRow rowNum="1">
            <columnData colNum="1">
                <data>ABTCRF9KLMZQ</data>
            </columnData>
            <columnData colNum="2">
                <data>2015-08-27 00:14:27</data>
            </columnData>
            <columnData colNum="3">
                <data>Sale</data>
            </columnData>
        </reportDataRow>
        <reportDataRow rowNum="2">
            <columnData colNum="1">
                <data>ABTMMK7332AR</data>
            </columnData>
            <columnData colNum="2">
                <data>2015-08-27 00:59:05</data>
            </columnData>
            <columnData colNum="3">
                <data/>
            </columnData>
        </reportDataRow>
        <pageNum>1</pageNum>
    </getDataResponse>
</reportingEngineResponse>
"""

    def create_report_data_request(self):
        return PaypalReportDataRequest(
            "RE1234567890",
            1,
            "testpartner",
            "testvendor",
            "notsosecret",
            "testuser",
            TEST_URL
        )

    def test_successful_response(self):
        self.on_post_return_xml()

        request = self.create_report_data_request()
        response = request.execute()

        self.assertEqual(response.response_code, 100)
        self.assertEqual(response.response_message, 'Request has completed successfully')
        self.assertEqual(response.rows, [
            ['ABTCRF9KLMZQ', '2015-08-27 00:14:27', 'Sale'],
            ['ABTMMK7332AR', '2015-08-27 00:59:05', ''],
        ])

        self.parse_request_xml()
        self.assert_request_xml_equals('./getDataRequest', """\
<getDataRequest>
    <reportId>RE1234567890</reportId>
    <pageNum>1</pageNum>
</getDataRequest>
""")

    def test_missing_data_element(self):
        self.remove_xml_node('./getDataResponse/reportDataRow[@rowNum="1"]/columnData[@colNum="1"]/data')
        self.on_post_return_xml()

        request = self.create_report_data_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, 'data'):
            request.execute()

    def test_missing_data_response(self):
        self.remove_xml_node('./getDataResponse')
        self.on_post_return_xml()

        request = self.create_report_data_request()
        with self.assertRaisesRegex(PaypalMalformedResponseError, 'getDataResponse'):
            request.execute()


@ddt
@httpretty.activate
class TestReportResultsRequest(XmlRequestMixin, TestCase):

    SAMPLE_RESPONSE = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<reportingEngineResponse>
    <baseResponse>
        <responseCode>100</responseCode>
        <responseMsg>Request has completed successfully</responseMsg>
    </baseResponse>
    <getResultsResponse>
        <Results>
            <reportId>RE1234567890</reportId>
            <statusCode>3</statusCode>
            <statusMsg>Report has completed successfully</statusMsg>
        </Results>
    </getResultsResponse>
</reportingEngineResponse>
"""

    def create_report_results_request(self):
        return PaypalReportResultsRequest(
            "RE1234567890",
            "testpartner",
            "testvendor",
            "notsosecret",
            "testuser",
            TEST_URL
        )

    def test_successful_response(self):
        self.on_post_return_xml()

        report_request = self.create_report_results_request()
        response = report_request.execute()

        self.assertEqual(response.response_code, 100)
        self.assertEqual(response.response_message, 'Request has completed successfully')
        self.assertEqual(response.report_id, 'RE1234567890')
        self.assertEqual(response.status_code, 3)
        self.assertEqual(response.status_message, 'Report has completed successfully')
        self.assertFalse(response.is_running)
        self.assertTrue(response.is_ready)

        self.parse_request_xml()
        self.assert_request_xml_equals('./getResultsRequest', """\
<getResultsRequest>
    <reportId>RE1234567890</reportId>
</getResultsRequest>
""")

    @data(
        (1, 'Report has been created'),
        (2, 'Report is currently executing')
    )
    @unpack
    def test_executing_status(self, status_code, status_msg):
        self.set_xml_node_text('./getResultsResponse/Results/statusCode', status_code)
        self.set_xml_node_text('./getResultsResponse/Results/statusMsg', status_msg)
        self.on_post_return_xml()

        report_request = self.create_report_results_request()
        response = report_request.execute()

        self.assertTrue(response.is_running)
        self.assertFalse(response.is_ready)


SAMPLE_TRANSACTION = OrderedDict([
    ('transaction_id', 'BTP123456'),
    ('time', '2015-08-28 00:14:27'),
    ('type', 'Sale'),
    ('tender_type', 'PayPal'),
    ('account_number', 'foobarbaz@foo.com'),
    ('expires', ''),
    ('amount', '5000'),
    ('result_code', '0'),
    ('response_msg', 'Approved'),
    ('comment_1', ''),
    ('comment_2', ''),
    ('batch_id', '0'),
    ('currency_symbol', 'USD'),
    ('paypal_transaction_id', '1FW12345678901234'),
    ('paypal_fees', '140'),
    ('paypal_email_id', 'foobarbaz@foo.com'),
    ('original_pnref', ''),
    ('original_type', ''),
    ('original_amount', ''),
    ('original_time', ''),
    ('invoice_number', 'EDX-123456'),
    ('purchase_order', ''),
    ('customer_ref', '')
])


@ddt
class TestSettlementReportRecord(TestCase):

    @data(
        ('Sale', 'sale'),
        ('Credit', 'refund')
    )
    @unpack
    def test_transaction_type(self, trans_type, internal_trans_type):
        transaction = OrderedDict(SAMPLE_TRANSACTION)
        transaction['type'] = trans_type

        self.assertEqual(SettlementReportRecord(**transaction).transaction_type, internal_trans_type)

    @data(
        ('0', '0.00'),
        ('-1', '-0.01'),
        ('10', '0.10'),
        ('100', '1.00'),
        ('1000', '10.00')
    )
    @unpack
    def test_converting_to_decimal(self, paypal_representation, decimal_amount):
        transaction = OrderedDict(SAMPLE_TRANSACTION)
        transaction['amount'] = paypal_representation
        transaction['paypal_fees'] = paypal_representation
        record = SettlementReportRecord(**transaction)

        self.assertEqual(record.decimal_amount, decimal_amount)
        self.assertEqual(record.decimal_fees, decimal_amount)

    def test_refund(self):
        transaction = OrderedDict(SAMPLE_TRANSACTION)
        transaction['type'] = 'Credit'
        record = SettlementReportRecord(**transaction)

        self.assertEqual(record.decimal_amount, '-50.00')
        self.assertEqual(record.decimal_fees, '-1.40')
