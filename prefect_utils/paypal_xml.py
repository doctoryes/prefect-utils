import xml.etree.cElementTree as ET
from collections import OrderedDict, namedtuple
from decimal import Decimal
from io import BytesIO

import requests


class PaypalApiResponse:
    """
    A generic API response. All API responses include the "baseResponse" element which is parsed by this class so any
    children overriding `params_from_xml` are expected to call this class.

    Subclasses are expected to take in the response_code and response_message in their constructor and pass them up to
    the constructor of this class.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
    """

    SUCCESSFUL_RESPONSE_CODE = 100

    def __init__(self, response_code, response_message):
        # NOTE: subclasses overriding this method should also include these parameters in their signature since the
        # constructor call is generated dynamically during the XML parsing process.
        self.response_code = response_code
        self.response_message = response_message

    @classmethod
    def from_http_response(cls, response):
        """
        Parse an HTTP response from the API. Raises errors if the response is not usable. Returns the populated response
        object if it is.

        Args:
            response: a requests.HTTPResponse object

        Raises:
            HTTPError: a requests.exceptions.HTTPError object if the API returned an HTTP status code other than 200 OK.
            PaypalError: If either the response was malformed or Paypal indicated that it could not process the
                request for some reason.
        """
        response.raise_for_status()
        response_root = ET.fromstring(response.content)
        response_obj = cls.from_xml(response_root)
        response_obj.raise_for_status()
        return response_obj

    @classmethod
    def params_from_xml(cls, _root_node):
        """
        Subclasses are expected to override this method to parse fields out of the XML document to be passed to their
        constructor.

        Args:
            root_node: an ElementTree.Element object representing the root node of the XML response.

        Returns:
            A dictionary mapping constructor arguments to the values extracted from the XML document.
        """
        return {}

    @classmethod
    def from_xml(cls, root_node):
        """
        Parse the status of the request from the response. This request status should be included in all responses.

        Args:
            root_node: an ElementTree.Element object representing the root node of the XML response.

        Returns:
            A valid response object.
        """
        base_response_node = find_or_raise(root_node, 'baseResponse')
        params = {
            'response_code': int(find_text_or_raise(base_response_node, 'responseCode')),
            'response_message': find_text_or_raise(base_response_node, 'responseMsg')
        }
        params.update(cls.params_from_xml(root_node))
        return cls(**params)

    def raise_for_status(self):
        """Raise an error if the request failed or the response is malformed."""
        if self.response_code != self.SUCCESSFUL_RESPONSE_CODE:
            raise PaypalApiRequestFailedError(self.response_code, self.response_message)


def find_or_raise(node, child_name):
    """
    Find the direct descendant of `node` with the tag `child_name`. If the no child is found with that tag, an error is
    raised.

    Args:
        node: an ElementTree.Element object representing a node in an XML document that is expected to have a child
            with the name specified by the `child_name` parameter.
        child_name: a string containing the name of the tag that is expected to be a direct descendant of `node`.

    Raises:
        PaypalMalformedResponseError: If the document does not have the expected structure.

    Returns:
        A reference to the child node when it is found.
    """
    child = node.find(child_name)
    if child is None:
        raise PaypalMalformedResponseError(
            f'The required element "{child_name}" was not found in the API response', node
        )
    return child


def find_text_or_raise(node, child_name):
    """
    Extract the text from the direct descendant of `node` with the tag `child_name`. If the no child is found with that
    tag, an error is raised.

    Args:
        node: an ElementTree.Element object representing a node in an XML document that is expected to have a child
            with the name specified by the `child_name` parameter.
        child_name: a string containing the name of the tag that is expected to be a direct descendant of `node`.

    Raises:
        PaypalMalformedResponseError: If the document does not have the expected structure.

    Returns:
        The text contents of the child when it is found.
    """
    text = node.findtext(child_name)
    if text is None:
        raise PaypalMalformedResponseError(
            f'The required element "{child_name}" was not found in the API response', node
        )
    return text


class PaypalError(Exception):
    """A base class for Paypal API related exceptions."""
    pass


class PaypalApiRequestFailedError(PaypalError):
    """Paypal responded with an error code specifying a generic failure with processing the request."""

    def __init__(self, response_code, response_message, request_type="API"):
        super().__init__(
            'Paypal {request_type} request failed with code {code}: {message}'.format(
                request_type=request_type,
                code=response_code,
                message=response_message
            )
        )


class PaypalMalformedResponseError(PaypalError):
    """The response from paypal did not match the expected format."""

    def __init__(self, message, root_node=None):
        with_tree = message
        if root_node:
            with_tree = '{}:{}'.format(
                message,
                ET.tostring(root_node, encoding='UTF-8', method='xml').decode('utf-8'),
            )

        super().__init__(with_tree)


class PaypalApiRequest:
    """
    A generic API request. Subclasses are expected to override `append_request_node` to specify the details of the
    particular request in question.

    A particular request expects a particular type of response. The class used to parse the response is specified by
    overriding the `RESPONSE_CLASS` variable.
    """

    # WARNING: the XML parser used by the paypal service will not recognize fields in a different order than the one
    # specified below. DO NOT CHANGE the ordering of fields within requests! They could have avoided this by using
    # <xsd:all> instead of <xsd:sequence> for their types that shouldn't care about the ordering of fields (many of
    # their complex types fall into this category).

    RESPONSE_CLASS = PaypalApiResponse

    def __init__(self, partner, vendor, password, user, url):
        self.partner = partner
        self.vendor = vendor
        self.password = password
        self.user = user
        self.url = url

    def create_request_document(self):
        """Get the string representation of the XML request."""
        root_node = ET.Element('reportingEngineRequest')
        self.append_authentication_node(root_node)
        self.append_request_node(root_node)

        # NOTE: we have to use this API to get the XML declaration, it is suboptimal that we have to construct a
        # BytesIO buffer to write to.
        tree = ET.ElementTree(root_node)
        bytes_buffer = BytesIO()
        tree.write(bytes_buffer, encoding='UTF-8', xml_declaration=True)
        return bytes_buffer.getvalue()

    def append_authentication_node(self, root_node):
        """Inject the authentication elements into the request."""
        auth_node = ET.SubElement(root_node, 'authRequest')

        for attribute in ('user', 'vendor', 'partner', 'password'):
            child_node = ET.SubElement(auth_node, attribute)
            child_node.text = str(getattr(self, attribute))

    def append_request_node(self, root_node):
        """Inject the request-specific elements into the request."""
        pass

    def execute(self):
        """
        Execute the request and return the parsed response object.

        Returns:
            A subclass of PaypalApiResponse
        """
        request_document = self.create_request_document()
        headers = {
            'Content-Type': 'text/plain'
        }
        response = requests.post(
            self.url,
            data=request_document,
            headers=headers,
        )
        return self.RESPONSE_CLASS.from_http_response(response)


class PaypalReportResponse(PaypalApiResponse):
    """
    A response that details the status of a reporting request. This is used to parse responses from both the
    PaypalReportRequest and PaypalReportResultsRequest request types since they are very similar and they are
    interpreted the same way.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
        report_id: A unique report identifier that can be used to identify the report when making subsequent requests.
        status_code: The numeric status code for the report generation request. Note that this code differs from the
            response_code and refers to the report status, not the request status.
    """

    REPORT_CREATED_STATUS_CODE = 1
    REPORT_EXECUTING_STATUS_CODE = 2
    REPORT_COMPLETE_STATUS_CODE = 3

    def __init__(self, response_code, response_message, report_id, status_code, status_message):
        super().__init__(response_code, response_message)
        self.report_id = report_id
        self.status_code = status_code
        self.status_message = status_message

    @classmethod
    def params_from_xml(cls, root_node):
        maybe_report_node = root_node.find('runReportResponse')
        if maybe_report_node is None:
            maybe_report_node = root_node.find('getResultsResponse')
            if maybe_report_node is not None:
                maybe_report_node = maybe_report_node.find('Results')

        if maybe_report_node is None:
            raise PaypalMalformedResponseError(
                'Unable to find a valid response body, must be either "runReportResponse" or "getResultsResponse".',
                root_node
            )

        params = {}
        params['report_id'] = find_text_or_raise(maybe_report_node, 'reportId')
        params['status_code'] = int(find_text_or_raise(maybe_report_node, 'statusCode'))
        params['status_message'] = find_text_or_raise(maybe_report_node, 'statusMsg')
        return params

    @property
    def is_running(self):
        """Returns True iff the report is still running on the backend."""
        return self.status_code in (self.REPORT_CREATED_STATUS_CODE, self.REPORT_EXECUTING_STATUS_CODE)

    @property
    def is_ready(self):
        """Returns True iff the report completed successfully and data can be retrieved."""
        return self.status_code == self.REPORT_COMPLETE_STATUS_CODE

    def raise_for_status(self):
        """Raise an error if the report failed to execute on the backend."""
        super().raise_for_status()
        if self.status_code > self.REPORT_COMPLETE_STATUS_CODE:
            raise PaypalApiRequestFailedError(self.status_code, self.status_message, "report")


class PaypalReportRequest(PaypalApiRequest):
    """
    Request a report. The different types of reports require different parameters, which are passed into the constructor
    as keyword arguments.

    Args:
        report_name: The name of the report to generate.
        report_params: The parameters to pass along to the backend. This is commonly a date range, but may include other
            types of parameters as well.
    """

    DEFAULT_PAGE_SIZE = 50
    RESPONSE_CLASS = PaypalReportResponse

    def __init__(self, report_name, partner, vendor, password, user, url, **report_params):
        super().__init__(partner, vendor, password, user, url)
        self.report_name = report_name
        self.report_params = report_params
        self.page_size = self.report_params.pop('page_size', self.DEFAULT_PAGE_SIZE)

    def append_request_node(self, root_node):

        # WARNING: the paypal XML parser is position sensitive. Do NOT change the ordering of the fields in the request.
        request_node = ET.SubElement(root_node, 'runReportRequest')
        name_node = ET.SubElement(request_node, 'reportName')
        name_node.text = str(self.report_name)

        for param_name, param_value in self.report_params.items():
            param_node = ET.SubElement(request_node, 'reportParam')
            param_name_node = ET.SubElement(param_node, 'paramName')
            param_name_node.text = str(param_name)
            param_value_node = ET.SubElement(param_node, 'paramValue')
            param_value_node.text = str(param_value)

        page_size_node = ET.SubElement(request_node, 'pageSize')
        page_size_node.text = str(self.page_size)


ColumnMetadata = namedtuple('ColumnMetadata', ('name', 'data_type'))  # pylint: disable=invalid-name


class PaypalReportMetadataResponse(PaypalApiResponse):
    """
    A response that details the format and size of the generated report.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
        num_rows: The number of rows in the report.
        num_pages: The number of pages of data that can be requested.
        page_size: The number of rows per page of data.
        columns: An OrderedDict mapping column names to ColumnMetadata objects.
    """

    def __init__(self, response_code, response_message, num_rows, num_pages, page_size, columns):
        super().__init__(response_code, response_message)
        self.num_rows = num_rows
        self.num_pages = num_pages
        self.page_size = page_size
        self.columns = columns

    @classmethod
    def params_from_xml(cls, root_node):
        node = find_or_raise(root_node, 'getMetaDataResponse')

        params = {}
        params['num_rows'] = int(find_text_or_raise(node, 'numberOfRows'))
        params['num_pages'] = int(find_text_or_raise(node, 'numberOfPages'))
        params['page_size'] = int(find_text_or_raise(node, 'pageSize'))

        columns = OrderedDict()
        for column_node in node.iterfind('columnMetaData'):
            name = find_text_or_raise(column_node, 'dataName')
            data_type = find_text_or_raise(column_node, 'dataType')
            columns[name] = ColumnMetadata(name=name, data_type=data_type)
        params['columns'] = columns

        return params


class PaypalReportMetadataRequest(PaypalApiRequest):
    """
    A request for the format and size of a particular report.

    Args:
        report_id: The report identifier.
    """

    RESPONSE_CLASS = PaypalReportMetadataResponse

    def __init__(self, report_id, partner, vendor, password, user, url):
        super().__init__(partner, vendor, password, user, url)
        self.report_id = report_id

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getMetaDataRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = str(self.report_id)


class PaypalReportDataResponse(PaypalApiResponse):
    """
    A page of data.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
        rows: A 2-dimensional array that represents the data table.
    """

    def __init__(self, response_code, response_message, rows):
        super().__init__(response_code, response_message)
        self.rows = rows

    @classmethod
    def params_from_xml(cls, root_node):
        node = find_or_raise(root_node, 'getDataResponse')

        params = {}
        rows = []
        for row_node in node.iterfind('reportDataRow'):
            row = []
            for column_node in row_node.iterfind('columnData'):
                # NOTE: data types are not used to parse the text data here!
                row.append(find_text_or_raise(column_node, 'data'))

            rows.append(row)

        params['rows'] = rows
        return params


class PaypalReportDataRequest(PaypalApiRequest):
    """
    Request a page of data.

    Args:
        report_id: The report identifier.
        page_num: The page of data to get.
    """

    RESPONSE_CLASS = PaypalReportDataResponse

    def __init__(self, report_id, page_num, partner, vendor, password, user, url):
        super().__init__(partner, vendor, password, user, url)
        self.report_id = report_id
        self.page_num = page_num

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getDataRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = str(self.report_id)
        page_num_node = ET.SubElement(request_node, 'pageNum')
        page_num_node.text = str(self.page_num)


class PaypalReportResultsRequest(PaypalApiRequest):
    """
    Request the status of a report.

    Args:
        report_id: The report identifier.
    """

    RESPONSE_CLASS = PaypalReportResponse

    def __init__(self, report_id, partner, vendor, password, user, url):
        super().__init__(partner, vendor, password, user, url)
        self.report_id = report_id

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getResultsRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = str(self.report_id)


BaseSettlementReportRecord = namedtuple('SettlementReportRecord', [  # pylint: disable=invalid-name
    'transaction_id',
    'time',
    'type',
    'tender_type',
    'account_number',
    'expires',
    'amount',
    'result_code',
    'response_msg',
    'comment_1',
    'comment_2',
    'batch_id',
    'currency_symbol',
    'paypal_transaction_id',
    'paypal_fees',
    'paypal_email_id',
    'original_pnref',
    'original_type',
    'original_amount',
    'original_time',
    'invoice_number',
    'purchase_order',
    'customer_ref'
])


class SettlementReportRecord(BaseSettlementReportRecord):
    """A record in the settlement report."""
    # pylint: disable=no-member

    @property
    def transaction_type(self):
        """Internal transaction type code from the paypal transaction type code"""
        if self.type == 'Sale':
            return 'sale'
        elif self.type == 'Credit':
            return 'refund'
        else:
            raise TypeError(f"Unknown transaction type: {self.type}")

    @property
    def decimal_amount(self):
        """A string representing the amount of currency as a floating point number"""
        return str(self.amount_string_to_decimal(self.amount))

    @property
    def decimal_fees(self):
        """A string representing the amount of currency of the fee as a floating point number"""
        return str(self.amount_string_to_decimal(self.paypal_fees))

    def amount_string_to_decimal(self, amount):
        """
        Paypal represents currency as whole numbers. $50.00 will be represented as "5000" in the report since different
        locales will use different decimal place separators etc. We do two things here:

        1) Ensure the decimal value uses 2 fractional decimal places by appending the string ".00" to the input string
        2) Divide by 100 to get the dollar value amount

        The sequence looks like: "5000" -> "5000.00" -> "50.00"

        By convention, refunds have a negative dollar value.
        """
        decimal_amount = Decimal(amount + '.00') / Decimal(100)
        if self.transaction_type == 'refund':
            decimal_amount = decimal_amount * Decimal(-1)

        return decimal_amount


class PaypalTimeoutError(PaypalError):
    """The requested report did not finish generating in time."""

    def __init__(self, start_time):
        super().__init__(
            "Aborting since the report did not finish generating within the acceptable time range. Started generation"
            " at {start_time}.".format(
                start_time=start_time
            )
        )
