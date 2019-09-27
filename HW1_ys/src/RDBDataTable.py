from src.BaseDataTable import BaseDataTable
import src.dbutils as dbutils
import json
import pandas as pd


pd.set_option("display.width", 196)
pd.set_option('display.max_columns', 16)


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }
        self._table_name=table_name
        cnx = dbutils.get_connection(connect_info)
        if cnx is not None:
            self._cnx = cnx
        else:
            raise Exception("Could not get a connection.")

    def __str__(self):

        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
        )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):

        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = dbutils.run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0][0]
            self._data['"row_count'] = row_count

        return row_count


    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        key_columns = self._data['key_columns']

        tmp = dict(zip(key_columns, key_fields))
        result = self.find_by_template(tmp, field_list=field_list)
        if result is not None:
            return result[1]
        else:
            return None

    def _construct_where(self, t):
        keys = []
        vals = []
        for k, v in t.items():
            temp = k + "=%s "
            keys.append(temp)
            vals.append(v)

        terms = "where " + " and ".join(keys) if len(keys) > 0 else ""
        vals = None if len(keys) <= 0 else vals
        return terms, vals

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
        s =self._construct_where(template)
        if field_list:
            q = "select {} from " + self._table_name + " " + s[0]
        else:
            q = "select * from " + self._table_name + " " + s[0]
        result = dbutils.run_q(q, args=s[1], fields=field_list, cur=None, conn=self._cnx, commit=True, fetch=True)
        return result


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        key_columns = self._data['key_columns']
        tmp = dict(zip(key_columns, key_fields))
        return self.delete_by_template(tmp)



    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        s = self._construct_where(template)
        q = "delete from " + self._table_name + " " + s[0]
        result = dbutils.run_q(q, args=s[1], conn=self._cnx, fetch=False)
        return result

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        key_columns = self._data['key_columns']
        tmp = dict(zip(key_columns, key_fields))
        return self.update_by_template(tmp, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        terms = []
        args = []
        for k, v in new_values.items():
            terms.append(k + "=%s")
            args.append(v)
        terms = ",".join(terms)
        s = self._construct_where(template)
        args.extend(s[1])
        q = "update " + self._table_name + " set " + str(terms) + " " + s[0]
        result = dbutils.run_q(q, args, conn=self._cnx, fetch=False)
        return result


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql = "Insert into " + self._table_name + " "
        keys = []
        vals = []

        # This is paranoia. I know that calling keys() and values() should return in matching order,
        # but in the long term only the paranoid survive.
        for k, v in new_record.items():
            keys.append(k)
            vals.append(v)

        terms = "(" + ",".join(keys) + ") "
        subs = ["%s"] * len(keys)
        subs = "values ("+",".join(subs)+") "

        sql += terms + subs
        res=dbutils.run_q(sql, args=vals, fetch=True, conn=self._cnx, commit=True)
        return res


    def get_rows(self):
        return self._rows




