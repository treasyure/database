# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os
import json

# The logging level to use should be an environment variable, not hard coded.
# from src.RDBDataTable import RDBDataTable

logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


def t_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    res = csv_tbl.find_by_template(template=tmp, field_list=fields)
    print("query result=\n", json.dumps(res, indent=2))


def t_find_by_pk():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = ['willite01', 'BOS', '1960', '1']
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    res = csv_tbl.find_by_primary_key(key_vals, fields)
    print("Query result= \n", json.dumps(res, indent=2))


def t_insert():
    connect_info = {
        "directory": "../Data",
        "file_name": "orderdetails.csv"
    }

    new_item = {
        "orderNumber": "10025",
        "productCode": "s19_3171"
    }
    fields = ['orderNumber', 'productCode']

    csv_tbl = CSVDataTable("orderdetails", connect_info, key_columns=['productCode'])
    tmp = {'orderNumber': "10025"}
    res_bef = csv_tbl.find_by_template(template=tmp, field_list=fields)
    print("Query result= \n", json.dumps(res_bef, indent=2))

    res = csv_tbl.insert(new_item)
    res_aft = csv_tbl.find_by_template(template=tmp, field_list=fields)
    print("Query result= \n", json.dumps(res_aft, indent=2))


def t_delete_by_template():
    connect_info = {
        "directory": "../Data",
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    csv_tbl = CSVDataTable("orderdetails", connect_info,
                           key_columns=["orderNumber", "orderLineNumber"])
    fields = ['orderNumber', 'productCode']
    tmp = {'productCode': "S18_1749"}

    r1 = csv_tbl.find_by_template(template=tmp, field_list=fields)
    print("Details for order '10100' = \n", json.dumps(r1, indent=2))

    print("\nDeleting productCode 'S18_1749':")
    del_tmp = {'orderNumber': "10100", 'productCode': 'S18_1749'}
    r2 = csv_tbl.delete_by_template(template=del_tmp)

    print("Delete returned ", r2, "\n")

    tmp2 = {'orderNumber': "10100"}
    r3 = csv_tbl.find_by_template(template=tmp2, field_list=fields)
    print("Details for order '10100' after delete = \n", json.dumps(r3, indent=2))

    # print("Loaded table = \n", csv_tbl)

def t_delete_by_key():
    connect_info = {
        "directory": "../Data",
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    csv_tbl = CSVDataTable("orderdetails", connect_info,
                           key_columns=["orderNumber", "orderLineNumber"])
    fields = ['orderNumber', 'productCode']
    key_vals = ['10100', '2']
    r = csv_tbl.delete_by_key(key_fields=key_vals)

    print("Delete returned ", r, "\n")


def t_update_by_template():
    connect_info = {
        "directory": "../Data",
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    csv_tbl = CSVDataTable("orderdetails", connect_info,
                           key_columns=['orderNumber', "orderLineNumber"])

    fields = ['orderNumber', 'productCode']

    r1 = csv_tbl.find_by_template({"orderNumber": "10025"}, field_list=fields)

    print("Details for order '10025' = \n", json.dumps(r1, indent=2))

    print("\nUpdate productCode 'S18_1749':")
    r2 = csv_tbl.update_by_template({"orderNumber": "10100", "productCode": 'S18_4409'},{"orderNumber": "10025", "productCode": 'S18_3171'})

    print("update returned ", r2, "\n")

    r3 = csv_tbl.find_by_template({"orderNumber": "10025"}, field_list=fields)
    print("Details for order '10025' after update = \n", json.dumps(r3, indent=2))

    # print("Loaded table = \n", csv_tbl)
    print("This is the correct answer")

def t_update_by_key():
    connect_info = {
        "directory": "../Data",
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    csv_tbl = CSVDataTable("orderdetails", connect_info,
                           key_columns=['orderNumber', "orderLineNumber"])

    fields = ['orderNumber', 'productCode']
    key_vals = ['10100', '2']

    r1 = csv_tbl.find_by_primary_key(key_vals, fields)

    print("Find result= \n", json.dumps(r1, indent=2))

    r = csv_tbl.update_by_key(key_vals, {"orderNumber": "10100", "productCode": 'S18_3171'})

    print("Update returned ", r, "\n")

    r2 = csv_tbl.find_by_primary_key(key_vals, fields)
    print("Find result= \n", json.dumps(r2, indent=2))



t_load()
t_find_by_template()
t_find_by_pk()
t_insert()
t_delete_by_template()
t_delete_by_key()
t_update_by_template()
t_update_by_key()

