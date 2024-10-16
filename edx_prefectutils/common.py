"""
Utility methods and tasks for use from a Prefect flow.
"""

import datetime
import itertools
import re

import prefect
import six
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from prefect import task
from prefect.engine.results import PrefectResult
import yaml


@task
def get_date(date: str):
    """
    Return today's date string if date is None. Otherwise return the passed parameter value.
    prefect.context.today is only available at task level, so we cannot use it as a default parameter value.
    """
    if date is None:
        return prefect.context.today
    else:
        return date


@task(result=PrefectResult())
def generate_dates(start_date: str, end_date: str, date_format: str = "%Y%m%d"):
    """
    Generates a list of date strings in the format specified by `date_format` from
    start_date up to but excluding end_date.
    """
    if not start_date:
        start_date = prefect.context.yesterday
    if not end_date:
        end_date = prefect.context.today

    parsed_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    parsed_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    while parsed_start_date < parsed_end_date:
        dates.append(parsed_start_date)
        parsed_start_date = parsed_start_date + datetime.timedelta(days=1)

    return [date.strftime(date_format) for date in dates]


@task
def generate_month_start_dates(start_date: str, end_date: str, date_format: str = "%Y-%m-%d"):
    """
    Return a list of first days of months within the specified date range.
    If start_date or end_date is not provided, defaults to yesterday or today respectively.
    prefect.context.today is only available at task level, so we cannot use it as a default parameter value.
    """
    if not start_date:
        start_date = prefect.context.yesterday
    if not end_date:
        end_date = prefect.context.today

    # Since our intention is to extract first day of months, we will start by modifying the start and end date
    # to represent the first day of month.
    parsed_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    parsed_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(day=1)
    dates = []
    current_date = parsed_start_date
    while current_date <= parsed_end_date:
        dates.append(current_date)
        # The addition of 32 days to current_date and then setting the day to 1 is a way to ensure that we move to
        # the beginning of the next month, even if the month doesn't have exactly 32 days.
        current_date += datetime.timedelta(days=32)
        current_date = current_date.replace(day=1)

    return [date.strftime(date_format) for date in dates]


@task
def get_unzipped_cartesian_product(input_lists: list):
    """
    Generate an unzipped cartesian product of the given list of lists, useful for
    generating task parameters for mapping.

    For example, get_unzipped_cartesian_product([[1, 2, 3], ["a", "b", "c"]]) would return:

    [
      [1, 1, 1, 2, 2, 3, 3, 3],
      ["a", "b", "c", "a", "b", "c", "a", "b", "c"]
    ]

    Args:
      input_lists (list): A list of two or more lists.
    """
    return list(zip(*itertools.product(*input_lists)))


def get_filename_safe_course_id(course_id, replacement_char='_'):
    """
    Create a representation of a course_id that can be used safely in a filepath.
    """
    try:
        course_key = CourseKey.from_string(course_id)
        # Ignore the namespace of the course_id altogether, for backwards compatibility.
        filename = course_key._to_string()  # pylint: disable=protected-access
    except InvalidKeyError:
        # If the course_id doesn't parse, we will still return a value here.
        filename = course_id

    # The safest characters are A-Z, a-z, 0-9, <underscore>, <period> and <hyphen>.
    # We represent the first four with \w.
    # TODO: Once we support courses with unicode characters, we will need to revisit this.
    return re.sub(r'[^\w\.\-]', six.text_type(replacement_char), filename)


def get_value_from_key(yaml_config_file, key_name):
    """
    utility function that returns content of YAML for a given key
    :param yaml_config_file: YAML file path
    :param key_name: Name of the key
    :return: rendered template as unicode string
    """
    with open(yaml_config_file, 'r') as config_reader:
        raw_config = yaml.load(config_reader, Loader=yaml.FullLoader)
        if key_name not in raw_config.keys():
            raise Exception(f'Key {key_name} does not exist in file {yaml_config_file}')
        return raw_config[key_name]

def get_secret(secret_key, file_path):
    return get_value_from_key(file_path, secret_key)
