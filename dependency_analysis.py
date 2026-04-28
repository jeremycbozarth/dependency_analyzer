from report import *

from db import ( 
    get_engine
    , get_conn
)

from collections import ( 
    defaultdict
    , deque
)

from sqlalchemy import (
    select
    , bindparam
    , func
    , exists
    , Table
    , Column
    , MetaData
)

from sqlalchemy.types import ( 
    String
    , Integer
    , Boolean
    , Numeric
    , Text
)


_table     = ()
_row_count = -1
_columns   = ()

SIMPLE_TYPES = ( String, Integer, Boolean, Numeric, Text )

INITIAL_SAMPLE_SIZE = 10
SAMPLE_SIZE_INCREASE_FACTOR = 10


def select_table_for_analysis(schema_name, table_name):

    global _table
    engine = get_engine()
    metadata = MetaData()

    _table = Table(
        table_name,
        metadata,
        schema=schema_name,
        autoload_with=engine
    )


def get_table_row_count():

    global _table
    global _row_count
    conn = get_conn()

    stmt = select(func.count()).select_from(_table)
    _row_count = conn.execute(stmt).scalar_one()
    return _row_count


def get_column_cardinalities():

    global _table
    global _columns
    conn = get_conn()

    _columns = {}
    columns_list = list(_table.columns)

    for column in columns_list:
        stmt = select(func.count(func.distinct(column)))
        distinct_count = conn.execute(stmt).scalar_one()
        _columns[column] = distinct_count


def find_unique_columns():

    global _row_count
    global _columns

    unique_columns = {
        column: distinct_count
        for column, distinct_count in _columns.items()
        if distinct_count == _row_count
    }

    for unique_column in unique_columns:
        del _columns[unique_column]
    
    unique_column_names = [unique_column.name for unique_column in unique_columns]
    return unique_column_names


def find_trivial_columns():

    global _columns

    trivial_columns = {
        column: distinct_count
        for column, distinct_count in _columns.items()
        if ( distinct_count == 0 or distinct_count == 1 )
    }

    for trivial_column in trivial_columns:
        del _columns[trivial_column]
    
    trivial_column_names = [trivial_column.name for trivial_column in trivial_columns]
    return trivial_column_names


def determines_over_sample(
        key_column: str
        , dependent_column: str
        , sample_size: int
):

    global _table
    _conn = get_conn()

    key       = _table.c[key_column]
    dependent = _table.c[dependent_column]

    sample = (
        select(key, dependent)
        .limit(bindparam("sample_size"))
        .subquery("s")
    )

    disproof = (
        select(sample.c[key_column])
        .group_by(sample.c[key_column])
        .having(func.count(func.distinct(sample.c[dependent_column])) > 1)
        .limit(1)
    )
    disproof_stmt = select(exists(disproof))

    disproved = _conn.execute(disproof_stmt, {"sample_size": sample_size}).scalar_one()
    return not disproved


def determines(
        key_column: str
        , dependent_column: str
):
    return determines_over_sample( key_column, dependent_column, None )


def discover_equivalencies():

    global _columns
    equivalent_columns = defaultdict(list)

    columns = deque( sorted(_columns, key=_columns.get) )

    while columns:
        key_column = columns.popleft()

        for dependent_column in columns.copy():

            if _columns[key_column] == _columns[dependent_column]:

                if determines(key_column.name, dependent_column.name):

                    if ( isinstance(key_column.type, SIMPLE_TYPES) and
                         not isinstance(dependent_column.type, SIMPLE_TYPES) ):
                        
                        equivalent_columns[key_column.name].append(dependent_column.name)
                        del _columns[dependent_column]
                        columns.remove(dependent_column)

                    else:
                        equivalent_columns[dependent_column.name].append(key_column.name)
                        del _columns[key_column]
                        break
            
            else:
                break

    return equivalent_columns


def initialize_dependencies():

    global _columns

    dependencies_premise = {}

    for column in _columns:
        dependencies_premise[column] = {}
    
    return dependencies_premise


def premised_dependency(
        key_column: Column
        , dependent_column: Column
        , dependencies_premise: dict
):
    
    if dependent_column in dependencies_premise[key_column]:
        return True
    
    for intermediate_column in dependencies_premise[key_column]:

        if premised_dependency(intermediate_column, dependent_column, dependencies_premise):
            return True
    
    return False


def discover_dependencies(
        dependencies_premise: dict
        , prior_dependencies_premise: dict
        , sample_size: int
        , starting_column: Column = None
):
    
    global _columns

    if ( starting_column is None ):
        columns = deque( sorted(_columns, key=_columns.get, reverse=True) )
    
    else:
        possible_dependent_columns = {
            column: distinct_count
            for column, distinct_count in _columns.items()
            if ( distinct_count < _columns[starting_column] or column is starting_column )
        }
        columns = deque( sorted(possible_dependent_columns, key=possible_dependent_columns.get, reverse=True) )

    while (columns):
        key_column = columns.popleft()

        for dependent_column in columns:

            if _columns[key_column] == _columns[dependent_column]:
                continue

            if ( prior_dependencies_premise is not None and
                 not premised_dependency(key_column, dependent_column, prior_dependencies_premise)
            ):
                continue

            if premised_dependency(key_column, dependent_column, dependencies_premise):
                continue

            if determines_over_sample(key_column.name, dependent_column.name, sample_size):
                dependencies_premise[key_column][dependent_column] = True
                discover_dependencies(dependencies_premise, prior_dependencies_premise, sample_size, dependent_column)


def analyze_dependencies():

    global _row_count

    sample_size                = INITIAL_SAMPLE_SIZE

    dependencies_premise      : dict
    prior_dependencies_premise: dict = None

    while ( sample_size < _row_count * SAMPLE_SIZE_INCREASE_FACTOR ):

        dependencies_premise = initialize_dependencies()
        discover_dependencies(dependencies_premise, prior_dependencies_premise, sample_size)

        prior_dependencies_premise = dependencies_premise
        sample_size *= SAMPLE_SIZE_INCREASE_FACTOR
    
    return dependencies_premise


def report_dependency_tree(
        column: Column
        , level: int
        , dependencies: dict
):
    if not dependencies[column] and level == 0:
        return
    
    report("  " * level + "[" + column.name + "]")

    for dependent_column in dependencies[column].keys():

        report_dependency_tree(dependent_column, level + 1, dependencies)


def has_parent(
        column: Column
        , dependencies: dict
):
    for key_column in dependencies.keys():

        if column in dependencies[key_column]:

            return True
    
    return False


def report_dependencies(dependencies: dict):

    for column in dependencies.keys():

        if not has_parent(column, dependencies):

            report_dependency_tree(column, 0, dependencies)
