from wtforms import DateTimeField, SelectField, StringField
from flask_wtf import Form


class AdHocForm(Form):
    dag = StringField(name='dag')


