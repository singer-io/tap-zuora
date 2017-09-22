import datetime


class State:
    def __init__(self, state=None, default_start_date=None):
        state = state or {}
        self.current_stream = state.get("current_stream", None)
        self.bookmarks = state.get("bookmarks", {})
        self.default_start_date = default_start_date

    def to_dict(self):
        return {
            "bookmarks": self.bookmarks,
            "current_stream": self.current_stream,
        }

    def get_entity_state(self, entity):
        if entity.name not in self.bookmarks:
            self.bookmarks[entity.name] = {}

        return self.bookmarks[entity.name]

    def get_version(self, entity):
        """
        Version is used to create the AQuA cursor on Zuora's end. The
        version should only ever change when there is no version in the
        stored state. This could be due to this being the first run of
        an entity or if the bookmark has been reset.
        """
        entity_state = self.get_entity_state(entity)
        if "version" not in entity_state:
            entity_state["version"] = int(datetime.datetime.utcnow().timestamp())

        return entity_state["version"]

    def get_bookmark(self, entity):
        if not entity.replication_key:
            raise Exception("Entities without replication keys do not support bookmarking")

        return self.get_entity_state(entity).get(entity.replication_key, self.default_start_date)

    def set_bookmark(self, entity, bookmark):
        if not entity.replication_key:
            raise Exception("Entities without replication keys do not support bookmarking")

        if bookmark > self.get_bookmark(entity):
            self.get_entity_state(entity)[entity.replication_key] = bookmark

    def get_file_ids(self, entity):
        return self.get_entity_state(entity).get("file_ids", [])

    def set_file_ids(self, entity, file_ids):
        self.get_entity_state(entity)["file_ids"] = file_ids
