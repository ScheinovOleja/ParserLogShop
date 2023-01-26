from datetime import date

from peewee import SqliteDatabase, Model, IntegerField, CharField, DateField

sqlite_db = SqliteDatabase('parser.db')


class BaseModel(Model):
    """A base model that will use our Sqlite database."""

    class Meta:
        database = sqlite_db


class Parser(BaseModel):
    shop = CharField(max_length=64)
    name_position = CharField(max_length=255)
    sold_count = IntegerField(default=0)
    past_value_accounts = IntegerField()
    date = DateField(default=date.today())
