import json
import sql_metadata
from get_from import read_schema, gen_from, get_candidate_tables, remove_bra
from nltk.tokenize import word_tokenize
import sqlite3
from tqdm import tqdm
from process_sql import get_sql

files = ['spider_dev_multi_col','spider_train_multi_col']

class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema, table):
        self._schema = schema
        self._table = table
        self._idMap = self._map(self._schema, self._table)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema, table):
        column_names_original = table['column_names_original']
        table_names_original = table['table_names_original']
        #print 'column_names_original: ', column_names_original
        #print 'table_names_original: ', table_names_original
        for i, (tab_id, col) in enumerate(column_names_original):
            if tab_id == -1:
                idMap = {'*': i}
            else:
                key = table_names_original[tab_id].lower()
                val = col.lower()
                idMap[key + "." + val] = i

        for i, tab in enumerate(table_names_original):
            key = tab.lower()
            idMap[key] = i

        return idMap

def execute_query(conn, query):
    # give sql 10 seconds to execute
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)
    conn.text_factory = lambda b: b.decode(errors = 'ignore')
    return conn

def construct_table(conn, db):
    raw_t_names = execute_query(conn, "SELECT name FROM sqlite_master WHERE type='table';")
    t_names = ["{}".format(t) for (t,) in raw_t_names]
    c_names = [[-1, '*']]
    for i, t in enumerate(t_names):
        c = conn.execute('select * from {}'.format(t))
        names = [d[0] for d in c.description]
        for n in names:
            c_names.append([i, "{}".format(n)])
    table = {
        "table_names_original": t_names,
        "column_names_original": c_names
    }
    return table 

def construct_schema(db):
    db_file = f'../database/{db}/{db}.sqlite'
    conn = create_connection(db_file)
    schema = {}
    with conn:
        table = construct_table(conn, db_id)
        for i, t in enumerate(table["table_names_original"]):
            schema["{}".format(t.lower())] = ["{}".format(col.lower()) for td, col in table["column_names_original"] if td == i]

        schema = Schema(schema, table)
    return schema

def rm_value_toks(sql):
    ops = ['=','<','>','OP']
    sql_split = sql.split()
    for i in range(len(sql_split)):
        word = sql_split[i]
        if word in ops:
            sql_split[i+1] = 'value'
    sql_no_value = ' '.join(sql_split)
    sql_no_value_toks = word_tokenize(sql_no_value)
    return sql_no_value_toks

def rm_select(sql):
    sql_split = sql.split('FROM')
    select_part = 'SELECT count(*)'
    try:
        where_part = sql_split[1]
    except:
        print(sql)
        raise Exception
    res = select_part + ' FROM ' + where_part
    return res

def rm_where(sql):
    if 'WHERE' in sql:
        res = sql.split('WHERE')[0]
    elif 'GROUP BY' in sql:
        res = sql.split('GROUP BY')[0]
    elif 'ORDER BY' in sql:
        res = sql.split('ORDER BY')[0]
    else:
        res = sql
    return res

def build_from(sql, schema, rm_where):
    table_aliases = sql_metadata.get_query_table_aliases(sql)
    for key, value in table_aliases.items():
        sql = sql.replace(key, value)
    if 'JOIN' not in sql:
        sql_split = sql.split()
        from_index = sql_split.index('FROM')
        table = sql_split[from_index + 1]
        from_part = f'from {table}'
    else:
        if rm_where:
            select_part = sql.split('FROM')[0]
            select_where = f'{select_part}'
        else:
            if 'WHERE' in sql:
                where_part = 'WHERE ' + sql.split('WHERE')[1]
            elif 'GROUP BY' in sql:
                where_part = 'GROUP BY ' + sql.split('GROUP BY')[1]
            elif 'ORDER BY' in sql:
                where_part = 'ORDER BY ' + sql.split('ORDER BY')[1]
            else:
                where_part = ''
            select_where = f'SELECT count(*) {where_part}'

        try:
            candidate_tables_id, table_names_original = get_candidate_tables(remove_bra(select_where), schema)
        except:
            return 0
        
        _, from_part = gen_from(candidate_tables_id, schema)

    if rm_where:
        select_part = sql.split('FROM')[0]
        sql = f'{select_part} {from_part}'
    else:
        if 'WHERE' in sql:
            where_part = 'WHERE ' + sql.split('WHERE')[1]
        elif 'GROUP BY' in sql:
            where_part = 'GROUP BY ' + sql.split('GROUP BY')[1]
        elif 'ORDER BY' in sql:
            where_part = 'ORDER BY ' + sql.split('ORDER BY')[1]
        else:
            where_part = ''
        sql = f'SELECT count(*) {from_part} {where_part}'
    return sql

table_path = '../tables/spider_tables.json'
schemas = read_schema(table_path)
for path in files:
    fname = path + '.json'
    data = json.load(open(fname,'rb'))
    where_rmd = []
    select_rmd = []
    sql_schemas = {}
    for i in data:
        query = i['query']
        db_id = i['db_id']
        if 'INTERSECT' in query: 
            continue #skip for now. Deal with it later
        if db_id in sql_schemas:
            sql_schema = sql_schemas[db_id]
        else:
            sql_schema = construct_schema(db_id)
            sql_schemas[db_id] = sql_schema
        schema = schemas[db_id]
        item = {key:value for key,value in i.items()}
        rm_where_query =  build_from(rm_where(query), schema, True)
        add_rm_where = True
        if rm_where_query == 0:
            add_rm_where = False
            rm_where_query = query
        
        rm_where_query_toks = word_tokenize(rm_where_query)
        item['query'] = rm_where_query
        item['query_toks'] = rm_where_query_toks
        item['query_toks_no_value'] = rm_value_toks(rm_where_query)
        try:
            item['sql'] = get_sql(sql_schema, rm_where_query)
        except:
            add_rm_where = False
            print(query)
            print(rm_where_query)
            print()

        item = {key:value for key,value in i.items()}
        rm_select_query = build_from(rm_select(query), schema, False)
        add_rm_select = True
        if rm_select_query  == 0:
            add_rm_select = False
            rm_select_query = query
            
        rm_select_query_toks = word_tokenize(rm_select_query)
        item['query'] = rm_select_query
        item['query_toks'] = rm_select_query_toks
        item['query_toks_no_value'] = rm_value_toks(rm_select_query)
        try:
            item['sql'] = get_sql(sql_schema, rm_select_query)
        except:
            print(query)
            print(rm_where_query)
            print()
            add_rm_select = False

        if add_rm_where:
            where_rmd.append(item)
        if add_rm_select:
            select_rmd.append(item)

    save_select_rmd = f'{path}_select_rmd.json'
    save_where_rmd = f'{path}_where_rmd.json'
    print(len(select_rmd))
    print(len(where_rmd))
    with open(save_select_rmd,'w') as f:
        json.dump(select_rmd, f)

    with open(save_where_rmd,'w') as f:
        json.dump(where_rmd, f)
        
