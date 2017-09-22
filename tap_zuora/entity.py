import pendulum
import singer.utils


ZOQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def format_zoql_datetime(datetime_str):
    datetime_dt = pendulum.parse(datetime_str).in_timezone('utc')
    return datetime_dt.strftime(ZOQL_DATETIME_FORMAT)


def format_datetime(datetime_str):
    datetime_dt = pendulum.parse(datetime_str).in_timezone('utc')
    return singer.utils.strftime(datetime_dt)


def format_value(value, field_type):
    if value == "":
        return None

    if field_type == "integer":
        return int(value)

    if field_type == "number":
        return float(value)

    if field_type in ["date", "datetime"]:
        return format_datetime(value)

    if field_type == "boolean":
        if isinstance(value, bool):
            return value

        return value.lower() == "true"

    return value


class Entity:
    def __init__(self, name, schema=None, replication_key=None, key_properties=None):
        self.name = name
        self.schema = schema
        self.replication_key = replication_key
        self.key_properties = key_properties

    def get_fields(self):
        return sorted([f for f, p in self.schema.properties.items() if p.selected])

    def get_base_query(self):
        return "select {} from {}".format(", ".join(self.get_fields()), self.name)

    def get_zoqlexport(self, start_date=None):
        start_date = format_zoql_datetime(start_date) if start_date else None
        query = self.get_base_query()
        if start_date and self.replication_key:
            query += " where {} >= '{}'".format(self.replication_key, start_date)
            query += " order by {} asc".format(self.replication_key)

        return query

    def get_zoql(self, start_date=None, end_date=None):
        start_date = format_zoql_datetime(start_date) if start_date else None
        end_date = format_zoql_datetime(end_date) if end_date else None
        query = self.get_base_query()
        if start_date and end_date:
            query += " where {} >= '{}'".format(self.replication_key, start_date)
            query += " and {} < '{}'".format(self.replication_key, end_date)

        return query

    def format_values(self, row):
        return {k: format_value(v, self.schema.properties[k].type)
                for k, v in row.items() if k in self.get_fields()}

    def update_bookmark(self, record, state):
        if self.replication_key and record[self.replication_key] > state.get_bookmark(self):
            state.set_bookmark(self, record[self.replication_key])
