import sqlite3
import os
import json
import sqlparse
import argparse
import subprocess
import numpy as np
from collections import defaultdict


def isnumber(word):
    try:
        float(word)
        return True
    except:
        return False

def find_shortest_path(start, end, graph):
    stack = [[start, []]]
    visited = set()
    while len(stack) > 0:
        ele, history = stack.pop()
        if ele == end:
            return history
        for node in graph[ele]:
            if node[0] not in visited:
                stack.append((node[0], history + [(node[0], node[1])]))
                visited.add(node[0])


def gen_from(candidate_tables, schema):
    if len(candidate_tables) <= 1:
        if len(candidate_tables) == 1:
            ret = "from {}".format(schema["table_names_original"][list(candidate_tables)[0]])
        else:
            ret = "from {}".format(schema["table_names_original"][0])
        return {}, ret

    table_alias_dict = {}
    uf_dict = {}
    for t in candidate_tables:
        uf_dict[t] = -1
    idx = 1
    graph = defaultdict(list)
    for acol, bcol in schema["foreign_keys"]:
        t1 = schema["column_names"][acol][0]
        t2 = schema["column_names"][bcol][0]
        graph[t1].append((t2, (acol, bcol)))
        graph[t2].append((t1, (bcol, acol)))
    candidate_tables = list(candidate_tables)
    start = candidate_tables[0]
    table_alias_dict[start] = idx
    idx += 1
    ret = "from {}".format(schema["table_names_original"][start])
    try:
        for end in candidate_tables[1:]:
            if end in table_alias_dict:
                continue
            path = find_shortest_path(start, end, graph)
            prev_table = start
            if not path:
                table_alias_dict[end] = idx
                idx += 1
                ret = "{} join {} as T{}".format(ret, schema["table_names_original"][end],
                                                 table_alias_dict[end],
                                                 )
                continue
            for node, (acol, bcol) in path:
                if node in table_alias_dict:
                    prev_table = node
                    continue
                table_alias_dict[node] = idx
                idx += 1
                ret = "{} join {} on {}.{} = {}.{}".format(ret, schema["table_names_original"][node],
                                                                    schema["table_names_original"][prev_table],
                                                                    schema["column_names_original"][acol][1],
                                                                    schema["table_names_original"][node],
                                                                    schema["column_names_original"][bcol][1])
                prev_table = node
    except:
        traceback.print_exc()
        print("db:{}".format(schema["db_id"]))
        return table_alias_dict, ret
    return table_alias_dict, ret

def get_candidate_tables(format_sql, schema):
  candidate_tables = []

  tokens = format_sql.split()
  for ii, token in enumerate(tokens):
      if '.' in token and '"' not in token and not isnumber(token):
        table_name = token.split('.')[0]
        candidate_tables.append(table_name)

  candidate_tables = list(set(candidate_tables))

  table_names_original = [table_name.lower() for table_name in schema['table_names_original']]
  candidate_tables_id = [table_names_original.index(table_name.lower()) for table_name in candidate_tables]

  assert -1 not in candidate_tables_id
  table_names_original = schema['table_names_original']

  return candidate_tables_id, table_names_original

def read_schema(table_schema_path):
  with open(table_schema_path) as f:
    database_schema = json.load(f)

  database_schema_dict = {}
  for table_schema in database_schema:
    db_id = table_schema['db_id']
    database_schema_dict[db_id] = table_schema

  return database_schema_dict

def remove_bra(test_str):
    ret = ''
    skip1c = 0
    skip2c = 0
    for i in test_str:
        if i == '"':
            skip1c += 1
        elif i == '"' and skip1c > 0:
            skip1c -= 1
        elif i == '[':
            skip1c += 1
        elif i == '(':
            skip2c += 1
        elif i == ']' and skip1c > 0:
            skip1c -= 1
        elif i == ')'and skip2c > 0:
            skip2c -= 1
        elif skip1c == 0 and skip2c == 0:
            ret += i
    return ret

