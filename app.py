import zipfile
import  pandas as pd
import re
from flask import Flask, request, jsonify
from sqlalchemy import create_engine

engine = create_engine('sqlite:///banco.db', echo=False)
nome_tabela = 'dados_finais'

def unZip(file):
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall('unzipped')

def fill_columm_nome_tipo(tipo):
    df = pd.read_csv('unzipped/tipos.csv', sep=',')
    df = df[(df['id'] == tipo)]
    return df['nome'].values[0]


def search(pattern):
    df = pd.read_csv('unzipped/origem-dados.csv', sep = ',')
    df = df[(df['status'].str.contains(pattern))]
    df['nome_tipo'] = df['tipo'].apply(fill_columm_nome_tipo)
    df = df.sort_values(by=['created_at'], ascending=True)

    SQL = []
    SQL.append(get_sql_schema_text(df))
    SQL.append(get_insert_query_from_df(df))

    with open('insert-dados.sql','w') as fileSQL:
        for line in SQL:
            if not line.isspace():
               fileSQL.write(line)

        fileSQL.close()

    fill_database(df, 'dados_finais')
    return df.to_string()

def get_sql_schema_text(df):

    sql_text = pd.io.sql.get_schema(df.reset_index(), nome_tabela)
    return  sql_text + ';'

def get_insert_query_from_df(df):

    insert = """
    INSERT INTO `{dest_table}` (
        """.format(dest_table=nome_tabela)

    columns_string = str(list(df.columns))[1:-1]
    columns_string = re.sub(r' ', '\n        ', columns_string)
    columns_string = re.sub(r'\'', '', columns_string)

    values_string = ''

    for row in df.itertuples(index=False,name=None):
        values_string += re.sub(r'nan', 'null', str(row))
        values_string += ',\n'

    return insert + columns_string + ')\n     VALUES\n' + values_string[:-2] + ';'

def create_table_tipos():
    df = pd.read_csv('unzipped/tipos.csv', sep=',')
    df.to_sql('tipos', con=engine, if_exists='replace')

def fill_database(df, tabela):
    df.to_sql(tabela,con=engine, if_exists='replace')

def list_dados_finais():
    qry = 'select created_at,nome_tipo, count(product_code) as quantidade from dados_finais group by nome_tipo order by nome_tipo'
    df = pd.read_sql(qry, con=engine)
    print(df.to_string())

unZip('dados.zip')
search("CRITICO")
create_table_tipos()
list_dados_finais()

#SERVER FLASK
#URL_EXT = http://127.0.0.1:5000/tipo?id=9 ou http://127.0.0.1:5000/tipos
########################################################################################
app = Flask(import_name=__name__)
@app.route('/tipos', methods=['GET'])
def tipos():
    qry = 'select * from tipos order by id'
    df = pd.read_sql_query(qry, con=engine)

    if df.empty:
        return jsonify({'msg': 'not found'}), 404


    return df.to_json(), 200

@app.route('/tipo', methods=['GET'])
def tipo():

    tipo_id =   id = request.args.get('id')

    if not tipo_id.isdigit():
        return jsonify({'msg': 'Id invalido'}), 400

    qry = f'select * from tipos where id = {tipo_id}'
    df = pd.read_sql_query(qry, con=engine)

    if df.empty:
        return jsonify({'msg': 'not found'}), 404

    return jsonify({'id': f'{df['id'].values[0]}', 'nome': f'{df['nome'].values[0]}'}),200

if __name__ == '__main__':
    app.run(debug=True)