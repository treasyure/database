
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)
class Index():
    def __init__(self,index_name, index_columns, index_kind):
        self._data={
            "index_name":index_name,
            "index_columns":index_columns,
            "index_kind":index_kind,
        }
        self.buckets = {}

    def compute_index_value(self,row):
        vs=[row[k]for k in self.data["index_columns"]]
        i_string = ("_").join(vs)
        return i_string

    def add_row(self,rid,row):
        i_value = self.compute_index_value(row)
        b = self._buckets.get(i_value)
        if b is None:
            b = []
        b.append(rid)
        self._buckets[i_value]=b

    def get_by_key(self,cols):
        k = "_".join(cols)
        result = self.buckets.get(k)
        return result

    def get_key_columns(self):
        return self._data["index_columns"]

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "table_columns": None,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        self._indexes={}

        if key_columns is not None and len(key_columns)>0:
            self._add_index("PRIMARY",key_columns)

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def _add_index(self, primary, columns):
        self._indexes[primary] = Index(primary,columns,"PRIMARY")

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            if "delimiter" in self._data["connect_info"].keys():
                csv_d_rdr = csv.DictReader(txt_file, delimiter=self._data["connect_info"]["delimiter"])
            else:
                csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                if self._data["table_columns"] is None:
                    table_cols = list(r.keys())
                    # if self._data["key_columns"] is not None:
                    #     key_cols = set(self._data["key_columns"])
                    #     if not key_cols.issubset(set(table_cols)):
                    #         raise Exception("Key column not in table")
                    self._data["table_columns"] = table_cols
                self._add_row(r)


        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")


    def _validate_template_and_fields(self, tmp, fields):
        c_set = set(self._data["table_columns"])


        if tmp is not None:
            t_set = set(tmp.keys())
        else:
            t_set = None


        if fields is not None:
            f_set = set(fields)
        else:
            f_set = None

        if t_set is not None and not t_set.issubset(c_set):
            raise Exception("Template are not valid.")
        if f_set is not None and not f_set.issubset(c_set):
            raise Exception("Fields are not valid.")

        return True

    def matches_template(row, template):

        result = True

        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        idx=self._indexes['PRIMARY']
        key_cols=idx.get_key_columns()

        tmp=dict(zip(key_cols,key_fields))
        result=self.find_by_template(template=tmp,field_list=field_list)

        if result is not None and len(result)>0:
            result=result[0]
        else:
            result=None
        return result

    def _project(row,field_list):
        result={}
        for f in field_list:
            result[f]=row[f]
        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """
        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        self._validate_template_and_fields(template, field_list)
        result=[]

        for r in self._rows:
            if CSVDataTable.matches_template(r,template):
                new_r=CSVDataTable._project(r,field_list)
                result.append(new_r)
        return result


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        tmp = dict(zip(self._data["key_columns"], key_fields))
        return self.delete_by_template(tmp)


    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        self._validate_template_and_fields(template, None)
        count = 0
        for r in self._rows:
            if CSVDataTable.matches_template(r,template):
                self._rows.remove(r)
                count += 1

        return count


    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        tmp = dict(zip(self._data["key_columns"], key_fields))
        return self.update_by_template(tmp, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        self._validate_template_and_fields(template, None)
        count = 0
        rows_copy = copy.copy(self._rows)
        for r in rows_copy:
            if CSVDataTable.matches_template(r, template):
                count += 1
                keys = new_values.keys()
                n_rows = copy.copy(r)
                for k in keys:
                    n_rows[k] = new_values[k]

                new_k = [n_rows[k] for k in self._data["key_columns"]]
                self._rows.remove(r)
                k = self.find_by_primary_key(new_k)
                if k is not None:
                    self._add_row(r)
                    raise ValueError('...')
                else:
                    self._add_row(n_rows)
        return count


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        if new_record is None:
            raise ValueError("You are a twit.")
        new_cols=set(new_record.keys())
        # tbl_cols=set(self._data["columns"])
        # if not new_cols.issubset(tbl_cols):
        #     raise ValueError("Totally a twit.")
        key_cols=self._data.get("key_columns",None)
        if key_cols is not None:
            key_cols=set(key_cols)
            if not key_cols.issubset(new_cols):
                raise ValueError("...")
            for k in key_cols:
                if new_record.get(k,None) is None:
                    raise ValueError("...")
            key_tmp = new_record
            if self.find_by_template(key_tmp) is not None\
                    and len(self.find_by_template(key_tmp))>0:
                raise ValueError("...")
        self._rows.append(new_record)
    @staticmethod


    def get_rows(self):
        return self._rows

