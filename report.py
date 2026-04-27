# report.py
# -----------------------------------------------------------------------------
# Lightweight console reporting helpers.
#
# This module centralizes a few simple print-based utilities used by the
# key-finder pipeline:
#   - Unadorned line reporting
#   - Timestamped reporting (useful for phase markers in CLI output)
#   - Formatting helpers for lists that should appear as: [a], [b], [c]
#
# Design notes:
# - This module intentionally does *not* use Python's `logging` library.
#   The surrounding scripts treat output as a human-readable CLI transcript,
#   so direct printing keeps things simple and predictable.
# - All functions are small and side-effectful only through stdout.
# - `__all__` controls what is exported when using: `from report import *`
# -----------------------------------------------------------------------------

from datetime import datetime

__all__ = [ "report", "report_with_timestamp", "format_list_bracketed_comma_separated" ]

def report(report_line):
    """
    Print a single line to stdout.

    Parameters
    ----------
    report_line:
        The text to print. This is printed exactly as provided.

    Typical usage
    -------------
    report("Starting analysis")
    report("")  # blank line for spacing
    """

    # f-string kept for consistency with other reporting functions.
    print(f"{report_line}")


def report_with_timestamp(report_line):
    """
    Print a single line to stdout, appending a human-friendly timestamp.

    Output format
    -------------
    "<report_line> (@ MM/DD/YYYY HH:MM:SS)"

    Parameters
    ----------
    report_line:
        The text to print before the timestamp.

    Typical usage
    -------------
    report_with_timestamp("Starting analysis")
    """

    # Local time is used intentionally for CLI readability.
    curr_date_time = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f"{report_line} (@ {curr_date_time})")


def format_list_bracketed_comma_separated(unformatted_list):
    """
    Format an iterable into a bracketed, comma-separated string.

    Example
    -------
    ["id", "name", "email"]  ->  "[id], [name], [email]"

    Parameters
    ----------
    unformatted_list:
        Any iterable of values. Each value is converted to string via f-string
        formatting and wrapped in square brackets.

    Returns
    -------
    str
        A single string containing the formatted values separated by ", ".
    """

    # Wrap each element in square brackets (preserving its string form).
    bracketed_list = [
        f"[{list_value}]"
        for list_value in unformatted_list
    ]

    # Join into the CLI-friendly representation.
    return ", ".join(bracketed_list)