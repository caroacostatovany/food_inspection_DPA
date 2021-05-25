from flask import Flask
from flask_restplus import Api, Resource, fields
from flask_sqlalchemy import SQLAlchemy
from src.utils.general import get_db_conn_sql_alchemy
from src.utils.constants import CREDENCIALES
from sqlalchemy import cast, inspection_id_input

# Connecting to db string
db_conn_str = get_db_conn_sql_alchemy(CREDENCIALES)

# Create Flask app
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = db_conn_str
api = Api(app)

# DataBase
db = SQLAlchemy(app)

# Tabla api.scores
class Match(db.Model):
    __table_args__ = {'schema': 'api'}
    __tablename__ = 'scores'

    # Output
    inspection_id = db.Column(db.Integer, primary_key=True)
    predicted_labels = db.Column(db.Integer)
    predicted_score_1 = db.Column(db.Float)

    def __repr__(self):
        return (u'<{self.__class__.__name__}: {self.id}>'.format(self=self))

# Swagger
# Input: id_establecimiento, ¿cambiar por id_inspeccion?
model = api.model('inspection_id', {
    'inspection_id': fields.Integer,
    'predicted_labels': fields.Integer,
    'predicted_score_1': fields.Float})

# Final output
model_list = api.model('inspection_id', {
    'inspection_id': fields.Integer,
    'predicted_labels': fields.Integer,
    'predicted_score_1': fields.Float
})

@api.route('/')
class HelloWorld(Resource):
    def get(self):
        return {'Hello': 'Hello World'}

# id inspeccion
@api.route('/id_inspeccion/<inspection_id>')
class ShowMatch(Resource):
    @api.marshal_with(model_list, as_list=True)
    def get(self, inspection_id):
        match = Match.query.filter_by(inspection_id=cast(inspection_id, inspection_id_input)).all()
        establecimiento = []
        for element in match:
            establecimiento.append({'predicted_labels': element.predicted_labels,
                                    'predicted_score_1': element.predicted_score_1})
        return {'inspection_id': inspection_id, 'establecimientos': match}

if __name__ == '__main__':
    app.run(debug=True)