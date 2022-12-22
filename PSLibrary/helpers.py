from contextlib import redirect_stderr, contextmanager
import codecs
import mmap
import io
from typing import Optional, List, Union
import datetime
import csv
import os

import pandas as pd
from pandas.io.parsers import TextFileReader


def read_user_defined_table(visum, msg_prefix: str, table_name: str, attributes: Optional[list] = None, column_names: Optional[list] = None) -> pd.DataFrame:
    table = visum.Net.TableDefinitions.ItemByKey(table_name).TableEntries

    if not attributes:
        attributes = [attribute.Code for attribute in table.Attributes.GetAll]

    if not column_names:
        column_names = attributes

    visum.Log(20480, f"{msg_prefix}: Reading from table: {table_name}, attributes: {','.join(attributes)}")
    df_udt = pd.DataFrame(table.GetMultipleAttributes(attributes), columns=column_names)
    return df_udt


def update_visum_table(visum, msg_prefix: str, table_name: str, dataframe: pd.DataFrame, attributes: Optional[list] = None, remove_entries=False):
    table = visum.Net.TableDefinitions.ItemByKey(table_name)
    visum.Log(20480, f"{msg_prefix}: Updating table: {table_name}. Erasing results: {remove_entries}")

    if remove_entries:
        table.TableEntries.RemoveAll()
        table.AddMultiTableEntries(len(dataframe))

    if not attributes:
        attributes = dataframe.columns

    table.TableEntries.SetMultipleAttributes(attributes, dataframe.values.tolist())


def read_visum_file(path: str, block_names: Optional[Union[List, str]], **kwargs) -> Optional[Union[List, pd.DataFrame]]:
    """
    Reads the net file from given path.
    :param path: path of the Visum net/att file
    :param block_names: str or list for table names within file to be read.
    :return: a dataframe or list of dataframes.
    """
    # Open net/att file.
    # look for the "$" decorator which denote blocks using a file map.
    # Line starting with "$" contains the column names for pandas DataFrame constructor
    # Preceding lines contain the values associated with these attributes.
    # The end of block is marked with "*"
    # Read the stream in between indexes for start and end and construct dataframe from the buffer.

    list_required = True
    if isinstance(block_names, str):
        block_names = [block_names]
        list_required = False

    number_of_blocks = len(block_names)
    found_block = [False] * number_of_blocks
    data = [pd.DataFrame([])] * number_of_blocks
    with _bom_aware_open(path) as file:
        file_map = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        while False in found_block:
            # search for the beginning of a table - first sign is a $
            dollar_sign_index = file_map.find(b"$")
            if dollar_sign_index == -1:
                break

            file_map.seek(dollar_sign_index)
            line_with_dollar = file_map.readline()
            for index, name in enumerate(block_names):
                if f"${name}:" not in str(line_with_dollar):
                    break

                found_block[index] = True

                # search for the end of this table - next table starts with a comment line this starts with *
                start_sign_index = file_map.find(b"*")

                # be careful: sometimes a string within the table also contains a * - skip those lines here:
                while start_sign_index != -1:
                    file_map.seek(start_sign_index)
                    line_with_star = file_map.readline()
                    index_star = line_with_star.find(b"*")
                    index_semicolon = line_with_star.find(b";")
                    if index_semicolon == -1 and index_star in [0, 1]:
                        break

                    start_sign_index = file_map.find(b"*")

                if start_sign_index == -1:
                    start_sign_index = file_map.size()

                # now we have beginning and end of the table within the file - read table as CSV
                count = start_sign_index - dollar_sign_index
                file_map.seek(dollar_sign_index)
                _df = _read_csv_file(io.BytesIO(file_map.read(count)), sep=";", **kwargs)
                if isinstance(_df, TextFileReader):
                    df_block = pd.concat(chunk for chunk in _df)
                    _df.close()
                else:
                    df_block = _df

                df_block.columns = map(lambda x, table_name=name: x.split(f"${table_name.upper()}:")[-1], df_block.columns)

                data[index] = df_block

    return data if list_required else data[0]


def export_visum_file(dfs, path_out: str, block_names: Optional[Union[List, str]], file_type: str, mode="w"):
    """
    Aggregates the given dfs and stores them in a desired Visum accepted file format.
    :param dfs: DataFrames for net objects. str or list
    :param path_out: path of the export file. str
    :param block_names: Table name of the blocks. str or list
    :param file_type: Visum file format. str. [Net, Att, Demand]
    :param mode: Write mode. For creating a new file: "w". For appending to an existing file: "a+".
    :return:
    """
    # dfs, table_names, net_objects should be iterables
    if not isinstance(dfs, list):
        dfs = [dfs]
        block_names = [block_names]

    now = datetime.datetime.now()
    datetime_string = now.strftime('%d/%m/%Y')

    dfs = _replace_invalid_visum_chars(dfs)

    # create dir if missing
    base_path, _ = os.path.split(path_out)
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    with open(path_out, mode, encoding='utf-8-sig') as file:
        # if mode is 'w', header block is inserted first. It's skipped otherwise.
        if mode == 'w':
            file.write(f'$VISION\n* {datetime_string}\n$VERSION:VERSNR;FILETYPE;LANGUAGE;UNIT\n')
            file.write(f'\n10.000;{file_type};ENG;KM\n\n')
        # iterate through the df, tableName, netObject and write them to file.
        for df_out, table_name, netObject in zip(dfs, block_names, block_names):
            attr_names = ";".join(df_out.columns).upper()
            file.write(f'*\n* Table: {table_name}\n*\n${netObject}:{attr_names}\n')
            df_out.to_csv(file, ";", mode="a", index=False, header=False, lineterminator='\n',
                          encoding='utf-8', quoting=csv.QUOTE_NONE, quotechar="", escapechar="")


@contextmanager
def _bom_aware_open(file_name: str, mode="r", **kwargs):
    encoding = _get_encoding(file_name)
    with open(file_name, mode, encoding=encoding, **kwargs) as file_stream:
        yield file_stream


def _get_encoding(file_name: str) -> str:
    with open(file_name, "rb") as file_stream:
        is_utf8 = file_stream.read(3) == codecs.BOM_UTF8

    return "utf-8-sig" if is_utf8 else "ansi"


def _read_csv_file(data, **kwargs):
    """
        Read the csv file and log the errors.
        :param data: data for constructing dataframe. e.g. str for path of the import file, or buffer reader.
        :param feedbackWrapper: The feedback wrapper object.
        :param kwargs: any other arguments to be used within read_csv.
        :return: a text file reader.
        """
    try:
        std_err_log = io.StringIO()
        with redirect_stderr(std_err_log):
            file_reader = pd.read_csv(data,
                                      encoding='UTF-8',
                                      skipinitialspace=True,
                                      on_bad_lines="warn",
                                      **kwargs)
            error_messages = std_err_log.getvalue()
            if error_messages and isinstance(error_messages, str) and error_messages != '':
                print(error_messages)

        return file_reader
    except Exception as err:
        mess = f"Error reading CSV file {data}: {str(err)}"
        raise Exception(mess) from err


def _replace_invalid_visum_chars(dataframes: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    Replace invalid Visum characters from string column values.
    :param dataframes: List of DataFrames containing Visum net objects.
    :return: List of DataFrames without invalid characters in string column values
    """
    results = []

    for dataframe in dataframes:
        for column in dataframe.columns:
            try:
                if len(dataframe[column]) > 0 and isinstance(dataframe[column][0], str):
                    mask1 = dataframe[column].astype(str).str.contains("$", na=False, regex=False)
                    mask2 = dataframe[column].astype(str).str.contains(";", na=False, regex=False)
                    dataframe.loc[mask1, column] = dataframe.loc[mask1, column].apply(lambda x: x.replace("$", "§"))
                    dataframe.loc[mask2, column] = dataframe.loc[mask2, column].apply(lambda x: x.replace(";", ","))
            except Exception:
                continue

        results.append(dataframe)

    return results
