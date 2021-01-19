"""
Utility methods and tasks for use from a Prefect flow.
"""

import datetime
import itertools
import re

import prefect
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from prefect import task
from prefect.engine.results import PrefectResult


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
    return re.sub(r'[^\w\.\-]', str(replacement_char), filename)
