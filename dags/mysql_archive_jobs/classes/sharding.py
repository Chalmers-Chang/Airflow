class timestamp_tag:
    def __init__(self, datetime_str):
        self.datetime_str = datetime_str


class sharding_table_log_opject:
    def __init__(
        self,
        sharding_table_name,
        shard_id,
        source_db_name,
        source_table_name,
        id_field_name,
        min_id,
        max_id,
        matchdate_field_name,
        min_matchdate,
        max_matchdate,
        timestamp_field_name,
        min_timestamp,
        max_timestamp,
        latest_update_time,
        current_columns,
    ):
        self.sharding_table_name = sharding_table_name
        self.shard_id = shard_id
        self.source_db_name = source_db_name
        self.source_table_name = source_table_name
        self.id_field_name = id_field_name
        self.min_id = min_id
        self.max_id = max_id
        self.matchdate_field_name = matchdate_field_name
        self.min_matchdate = min_matchdate
        self.max_matchdate = max_matchdate
        self.timestamp_field_name = timestamp_field_name
        self.min_timestamp = min_timestamp
        self.max_timestamp = max_timestamp
        self.latest_update_time = latest_update_time
        self.current_columns = current_columns

    def reset(self):
        self.sharding_table_name = None
        self.shard_id = None
        self.source_db_name = None
        self.source_table_name = None
        self.id_field_name = None
        self.min_id = 0
        self.max_id = 0
        self.matchdate_field_name = None
        self.min_matchdate = None
        self.max_matchdate = None
        self.timestamp_field_name = None
        self.min_timestamp = None
        self.max_timestamp = None
        self.latest_update_time = None
        self.current_columns = None


def gen_empty_sharding_table_log_opject():
    return sharding_table_log_opject(
        sharding_table_name=None,
        shard_id=0,
        source_db_name=None,
        source_table_name=None,
        id_field_name=None,
        min_id=0,
        max_id=0,
        matchdate_field_name=None,
        min_matchdate=None,
        max_matchdate=None,
        timestamp_field_name=None,
        min_timestamp=None,
        max_timestamp=None,
        latest_update_time=None,
        current_columns=None,
    )
