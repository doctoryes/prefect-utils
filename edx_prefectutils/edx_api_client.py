"""A simple client for authenticated access to Open edX REST APIs."""

from datetime import datetime, timedelta

import backoff
import requests
# from prefect.utilities.logging import get_logger
import logging
from requests.auth import AuthBase

DEFAULT_RETRY_STATUS_CODES = (
    requests.codes.request_timeout,         # HTTP Status Code 408
    requests.codes.too_many_requests,       # HTTP Status Code 429
    requests.codes.service_unavailable,     # HTTP Status Code 503
    requests.codes.gateway_timeout,         # HTTP Status Code 504
    requests.codes.bad_gateway,             # HTTP Status Code 502
    520,                                    # This is a custom Cloudwatch code for "Unknown error".
)
DEFAULT_TIMEOUT_SECONDS = 7200


class EdxApiClient(object):
    """
    Simplifies authentication and pagination logic when communicating with Open edX REST APIs.

    Arguments:
        auth_url (str): The full URL of the authentication endpoint. It should look like
            https://example.com/oauth2/v1/access_token. If this is not provided it is read from the "auth_url" field
            of the "edx-rest-api" section of the configuration file.
        client_id (str): This is the OAuth2 client_id for the application. If it is not provided it is read from the
            "client_id" field of the "edx-rest-api" section of the configuration file.
        client_secret (str): This is the OAuth2 client_secret for the application. If it is not provided it is read from
            the "client_secret" field of the "edx-rest-api" section of the configuration file.
        token_type (str): The type of authentication token required for the API call.  Should be one of 'jwt' (default)
            or 'bearer'.
    """

    def __init__(self, auth_url=None,
                 client_id=None, client_secret=None,
                 token_type=None):

        self._expires_at = None
        self._session = requests.Session()
        self._session.hooks = {
            'response': log_response_hook
        }

        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_url = auth_url
        self.token_type = token_type or 'jwt'

    @property
    def authenticated_session(self):
        """A session that has a valid access token associated with it and can make authenticated requests."""
        self.ensure_oauth_access_token()
        return self._session

    def ensure_oauth_access_token(self):
        """Retrieves OAuth 2.0 access token using the client credentials grant and stores it in the request session."""
        # logger = get_logger()
        logger = logging.getLogger()
        now = datetime.utcnow()
        if self._expires_at is None or now >= self._expires_at:
            logger.info('Token is expired or missing, requesting a new one.')

            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'token_type': self.token_type,
            }

            response = requests.post(
                self.auth_url,
                data=data,
                hooks={
                    'response': log_response_hook
                }
            )
            data = response.json()
            self._session.auth = SuppliedAuth(data['access_token'], data.get('token_type', self.token_type))
            self._expires_at = now + timedelta(seconds=data['expires_in'])
            logger.info("Acquired a token that expires at {}".format(self._expires_at.isoformat()))

    def get(self, url, params=None, timeout_seconds=DEFAULT_TIMEOUT_SECONDS, retry_on=DEFAULT_RETRY_STATUS_CODES):
        """
        Fetches a single page of the given resource.

        Arguments:
            url (str): The URL of the resource.
            params (dict): This is a dictionary of key-value pairs that are URL encoded and injected into the query
                string when making the request.
            timeout_seconds (float): When requesting a page, keep retrying unless this much time has elapsed. This timer
                resets after every successful fetch of a page. Raise an error if it takes longer than this amount of
                time to fetch an individual page.
            retry_on (iterable): This is a set of HTTP status codes that should trigger a retry of the request if they
                are received from the server in the response. If one is received the system implements an exponential
                back-off and repeatedly requests the page until either the timeout expires, a fatal exception occurs, or
                an OK response is received.

        Returns: A single requests.Response object for the first page of data received from the server.
        """
        return next(self.paginated_get(url, params=params, timeout_seconds=timeout_seconds, retry_on=retry_on,
                                       pagination_key=None))

    def paginated_get(self, url, params=None, timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                      retry_on=DEFAULT_RETRY_STATUS_CODES, pagination_key='next'):
        """
        Fetches a paginated resource.

        Arguments:
            url (str): The URL of the resource.
            params (dict): This is a dictionary of key-value pairs that are URL encoded and injected into the query
                string when making the request.
            timeout_seconds (float): When requesting a page, keep retrying unless this much time has elapsed. This timer
                resets after every successful fetch of a page. Raise an error if it takes longer than this amount of
                time to fetch an individual page.
            retry_on (iterable): This is a set of HTTP status codes that should trigger a retry of the request if they
                are received from the server in the response. If one is received the system implements an exponential
                back-off and repeatedly requests the page until either the timeout expires, a fatal exception occurs, or
                an OK response is received.
            pagination_key (str or lambda): Used to locate the URL for the next page of data from the JSON-parsed
                response.  If "pagination_key" is a string, then it names a field at the root of the response containing
                the "next page" URL.  E.g., use "pagination_key='next'" for a response that looks like this:
                {"results": [...], "next": "http://...", "previous": "http://..."}

                If "pagination_key" is a lambda, it should return the next page url from the given response object.
                E.g., use "pagination_key=lambda r: r['pagination']['next']" for a response that looks like this:

                {"results": [...], "pagination": {"next": "http://...", "previous": "http://..."}}

        Yields: A single requests.Response object for each page of data received from the server.
        """

        def get_next_url_from_response(response):
            """Returns the next page's URL from the response, as located by pagination_key."""
            response_obj = response.json()
            if isinstance(pagination_key, str):
                return response_obj.get(pagination_key)
            elif callable(pagination_key):
                return pagination_key(response_obj)
            else:
                return None

        def should_giveup(error):
            """
            Give up if the response is `None` or if the status code is not in the set of status codes
            that are retryable.
            """
            error_response = getattr(error, 'response', None)
            if error_response is None:
                return True

            return error_response.status_code not in retry_on

        @backoff.on_exception(backoff.expo,
                              requests.exceptions.RequestException,
                              max_time=timeout_seconds,
                              giveup=should_giveup)
        def get_resource_with_retry(next_url=None):
            """
            Attempt to get the resource, using an exponetial back-off to retry recoverable, failed requests.

            Arguments:
                next_url (str): The url of the next page to fetch. If this is `None` this is the first page, so we
                    should use the provided `url` and `params` passed into the outer function to fetch this first page.
                    The next url is provided in the first response with all of the appropriate parameters needed to
                    fetch the next page of data. We don't want to accidentally override existing parameters so we omit
                    the `params` kwarg from the call.
            """
            if next_url is None:
                raw_response = self.authenticated_session.get(url, params=params)
            else:
                raw_response = self.authenticated_session.get(next_url)

            raw_response.raise_for_status()

            # Get next URL if pagination was requested
            next_url = get_next_url_from_response(raw_response)

            return raw_response, next_url

        next_url = None
        while True:
            response, next_url = get_resource_with_retry(next_url)
            yield response
            if next_url is None:
                break


class SuppliedAuth(AuthBase):
    """Attaches a supplied authentication to the given Request object."""

    def __init__(self, token, token_type):
        """Instantiate the auth class."""
        self.token = token
        self.token_type = token_type

    def __call__(self, r):
        """Update the request headers."""
        r.headers['Authorization'] = '{token_type} {token}'.format(token_type=self.token_type, token=self.token)
        return r


def log_response_hook(response, *args, **kwargs):  # pylint: disable=unused-argument
    """Log summary information about every request made."""
    # logger = get_logger()
    logger = logging.getLogger()
    logger.info(
        "[{}] [{}] [{}] {}".format(
            response.request.method, response.status_code, response.elapsed.total_seconds(), response.url
        )
    )
