import os, sys

from db import connect_to_db, close_db_connection
from dependency_analysis import (
    select_table_for_analysis
    , get_table_row_count
    , get_column_cardinalities
    , find_unique_columns
    , find_trivial_columns
    , discover_equivalencies
    , analyze_dependencies
    , report_dependencies
)
from report import *

def main():
    if len(sys.argv) != 4:
        print("Usage: python dependency_analyzer.pl <connection URL> <schema name> <table name>")
        sys.exit(1)
    
    # Take in command line arguments:
    #   1) database connection URL, which may be immediately used to connect to the target db
    #   2) name of the schema within which the table to be analyzed resides
    #   3) name of the table to be analyzed
    connect_to_db(sys.argv[1])
    schema_name = sys.argv[2]
    table_name  = sys.argv[3]

    report("")
    report_with_timestamp("Starting analysis")

    # Indicate the table to be analyzed, and obtain its row count
    select_table_for_analysis(schema_name, table_name)
    row_count = get_table_row_count()

    report("")
    report(f"Row count for table [{table_name}] is {row_count}")

    # Get the cardinalities (distinct counts) of each of the columns in the table
    get_column_cardinalities()

    # Determine which (if any) columns are unique, report them and drop them from further analysis
    unique_columns = find_unique_columns()
    unique_columns_formatted = format_list_bracketed_comma_separated(unique_columns)
    if unique_columns:
        report("")
        report(f"Unique columns: {unique_columns_formatted}")

    # Determine which (if any) columns are trivial, report them and drop them from further analysis
    trivial_columns = find_trivial_columns()
    trivial_columns_formatted = format_list_bracketed_comma_separated(trivial_columns)
    if trivial_columns:
        report("")
        report(f"Trivial columns: {trivial_columns_formatted}")

    equivalent_columns = discover_equivalencies()
    if equivalent_columns:
        report("")
        report("Equivalent columns:")
        
        for key_column in equivalent_columns.keys():

            for equivalent_column in equivalent_columns[key_column]:

                report(f"[{key_column}] is equivalent to [{equivalent_column}]")

    report("")
    report_with_timestamp("Analyzing dependencies")

    dependencies = analyze_dependencies()

    report("")
    report("Dependencies discovered:")
    report("")
    report_dependencies(dependencies)

    report("")
    report_with_timestamp("Completed analysis")
    report("")

    # Close the database connection
    close_db_connection()
    
if __name__ == "__main__":
    main()
