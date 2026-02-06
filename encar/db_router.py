class EncarRouter:
    encar_tables = {
        "car_queue",
        "vehicle_raw",
        "inspection_raw",
        "record_raw",
        "options_choice_raw",
        "user_raw",
    }

    def db_for_read(self, model, **hints):
        if model._meta.db_table in self.encar_tables:
            return "encar"
        return "default"

    def db_for_write(self, model, **hints):
        # encar 데이터는 어드민에서 수정 안 하게 막음
        if model._meta.db_table in self.encar_tables:
            return "encar"
        return "default"

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        # Django 기본 앱들은 default에만 migrate
        if db == "default":
            return True
        # encar DB에는 migrate 절대 하지 않음
        if db == "encar":
            return False
        return None

