from sqlalchemy import MetaData, Table, Column
from sqlalchemy import types
from sqlalchemy.schema import CreateTable, DropTable


TYPE_MAP = {
    'bigint': types.BigInteger(),
    'boolean': types.Boolean(),
    'date': types.DATE(),
    'timestamp': types.TIMESTAMP(),
    'datetime': types.TIMESTAMP(),
    'double precision': types.FLOAT(),
    'enum': types.String(length=256),
    'guid': types.String(length=256),
    'int': types.Integer(),
    'integer': types.Integer(),
    'text': types.Text(),
}


def ddl_from_json(schema_json):
    """
    This function takes the schema definition in JSON format that's returned by
    the Canvas Data API and returns SQL DDL statements that can be used to create
    all of the tables necessary to hold the archived data.
    """
    metadata = MetaData()
    create_ddl = []
    drop_ddl = []
    for artifact in schema_json:
        table_name = schema_json[artifact]['tableName']
        json_columns = schema_json[artifact]['columns']

        t = Table(table_name, metadata)

        for j_col in json_columns:
            sa_col = _get_column(table_name, j_col)
            if sa_col is not None:
                t.append_column(sa_col)

        create_ddl.append(str(CreateTable(t)))
        drop_ddl.append(str(DropTable(t)))

    return create_ddl, drop_ddl


def _get_column(table, column):
    """
    Returns a Column with the appropriate sqlalchemy data type for a column from
    the JSON schema description. Some column definitions are incorrect in the
    JSON data, so this function has some manual overrides.
    """
    if table == 'group_membership_dim' and column['name'] in [u'id', u'canvas_id']:
        """
        The group_membership_dim.id and group_membership_dim.canvas_id columns
        are specified as varchars but they should be bigints
        """
        return Column(
            column['name'],
            types.BigInteger(),
        )

    elif table == 'quiz_question_answer_dim' and column['name'] in [
        u'answer_match_left',
        u'answer_match_right',
        u'matching_answer_incorrect_matches'
    ]:
        """
        These three columns in the quiz_question_answer_dim table are specified
        as having a length of 256, but the actual dumps contain longer values.
        Using a length of 4096 instead.
        """
        return Column(
            column['name'],
            types.String(length=4096)
        )
    elif table == 'quiz_question_dim' and column['name'] == u'name':
        """
        The quiz_question_dim.name column is specified as having a length of 256,
        but the actual dumps contain longer values. Using a length of 4096 instead.
        """
        return Column(
            column['name'],
            types.String(length=4096)
        )
    elif column['type'] == 'varchar':
        return Column(
            column['name'],
            types.String(length=column['length']),
        )
    elif column['type'] in TYPE_MAP:
        return Column(
            column['name'],
            TYPE_MAP[column['type']],
        )
    else:
        return None
