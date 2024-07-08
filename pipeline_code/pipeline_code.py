# Databricks notebook source
from pyspark.sql.functions import when, regexp_extract, col, regexp_replace, trim, split
import pyspark.sql.utils


class DataSource:
    """
    A class to represent a datasource.

    Methods
    -------
    fetch_data():
        Fetches data form a source. To be implemented by subclasses.
    """

    def fetch_data(self):
        """
        Fetches data from a source. To be implemented by subclasses.
        """
        pass


class CsvDataSource(DataSource):
    """
    A class to represent a csv datasource

    Attributes
    ----------
    filepath : str
        path to the csv file to read
    header : bool (default=True)
        boolean to indicate whether or not the csv file defined in filepath contains headers
    multiline : bool (default=False)
        boolean to indicate whether or not the csv file defined in filepath contains rows that span multiple lines

    Methods
    -------
    fetch_data():
        Fetches the csv data source
    """

    def __init__(
        self, filepath: str, header: bool = True, multiline: bool = False
    ) -> None:
        """
        Constructs all the necessary attributes for the CsvDataSource object.

        Parameters
        ----------
        filepath : str
            path to the csv file to read
        header : bool (default=True)
            boolean to indicate whether or not the csv file defined in filepath contains headers
        multiline : bool (default=False)
            boolean to indicate whether or not the csv file defined in filepath contains rows that span multiple lines
        """

        self.filepath = filepath
        self.header = header
        self.multiline = multiline

    def fetch_data(self) -> pyspark.sql.dataframe.DataFrame:
        """
        Fetches the CsvDataSource

        Returns
        -------
        pyspark.sql.dataframe.Dataframe
        """

        try:
            return spark.read.csv(
                path=self.filepath, header=self.header, multiLine=self.multiline
            )
        except pyspark.sql.utils.AnalysisException:
            print(f"Could not open file {self.filename}, check if filepath exists")


class DataTransformer:
    def __init__(self, df: pyspark.sql.dataframe.DataFrame) -> None:
        """
        Constructs all the necessary attributes for the DataTransformer object.

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            dataframe that should be transformed
        """
        self.df = df

    def get_df(self):
        """
        returns self.df

        Parameters
        ----------
        None

        Returns
        -------
        self.df : pyspark.sql.dataframe.DataFrame
        """
        return self.df

    def normalize_columns(self) -> None:
        """
        Strips leading and trailing spaces from column names in self.df
        Replaces '-' chars in column names with '_' chars
        Replaces ' ' chars in column names with '_' chars
        Makes sure all column names are lowercase

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        for name in self.df.schema.names:
            self.df = self.df.withColumnRenamed(name, name.strip())
        for name in self.df.schema.names:
            self.df = self.df.withColumnRenamed(name, name.lower())
        for name in self.df.schema.names:
            self.df = self.df.withColumnRenamed(name, name.replace("-", "_"))
        for name in self.df.schema.names:
            self.df = self.df.withColumnRenamed(name, name.replace(" ", "_"))

    def drop_cols_with_high_nullperc(self, max_null_perc: int) -> None:
        """
        Drops columns in self.df for which null_percentage is higher than max_null_perc

        Parameters
        ----------
        max_null_perc : int
            maximum allowed percentage of null values in a column, should be int between 0 and 100

        Returns
        -------
        None
        """
        assert (
            max_null_perc <= 100 and max_null_perc >= 0
        ), "max_null_perc should be between 0 and 100"
        row_count = self.df.count()
        columns = self.df.select(self.df["*"])
        for column in columns:
            null_count = self.df.where(column.isNull()).count()
            null_percentage = null_count / row_count * 100
            if null_percentage > max_null_perc:
                self.df = self.df.drop(column)

    def drop_rows_with_null_values(self) -> None:
        """
        Drops rows in self.df that contain null values

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.df = self.df.na.drop()

    def drop_duplicates(self) -> None:
        """
        Drops duplicate rows from self.df

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.df = self.df.dropDuplicates()

    def typecast_str(self, column: str) -> None:
        """
        Makes sure column is of type str in self.df

        Parameters
        ----------
        column: str
            column to typecast to str

        Returns
        -------
        None
        """
        self.df = self.df.withColumn(column, self.df[column].cast("string"))

    def strip_column(self, column: str) -> None:
        """
        Removes newline characters and strips spaces (from begin and end of string) from column in self.df

        Paramters
        ---------
        column: str
            column to strip

        Returns
        -------
        None
        """
        self.df = self.df.withColumn(column, regexp_replace(column, "\\n", ""))
        self.df = self.df.withColumn(column, trim(col(column)))

    def extract_field_from_field(
        self, src_field: str, dest_field: str, regex: str
    ) -> None:
        """
        Extracts dest_field from src_field using a regex on src_field and adds the dest_field to self.df

        Parameters
        ----------
        src_field: str
            The field in self.df to run the regex on
        dest_field: str
            The field to store the match in in self.df
        regex: str
            The regular expression to run on src_field

        Returns
        -------
        None
        """
        self.df = self.df.withColumn(
            dest_field, regexp_extract(col(src_field), regex, 1)
        )

    def typecast_strnumber_to_int(self, column: str) -> None:
        """
        Removes ',' char from number and typecasts the number to int

        Parameters
        ----------
        column: str
            Column to typecast to int in self.df
        """
        self.df = self.df.withColumn(column, regexp_replace(column, ",", ""))
        self.df = self.df.withColumn(column, self.df[column].cast("int"))

    def drop_column(self, column: str) -> None:
        """
        Drops column from self.df

        Parameters
        ----------
        column: str
            Column to drop in self.df

        Returns
        -------
        None
        """
        self.df = self.df.drop(column)

    def split_col_by_delim(self, column: str, delim: str) -> None:
        """
        Splits column value by a delimiter

        Parameters
        ----------
        column: str
            Column to split in self.df
        delim: str
            Delimiter to split column on

        Returns
        -------
        None
        """
        self.df = self.df.withColumn(column, split(col(column), delim))

    def typecast_strnumber_to_float(self, column: str) -> None:
        """
        Removes ',' char from column and typecasts the column to float in self.df

        Parameters
        ---------
        column: str
            Column to typecast to float in self.df

        Returns
        -------
        None
        """
        self.df = self.df.withColumn(column, regexp_replace(column, ",", ""))
        self.df = self.df.withColumn(column, self.df[column].cast("float"))

    def regex_replace(self, column: str, regex: str, replacement: str) -> None:
        """
        Finds regex in column andn replaces with replacement in self.df

        Parameters
        ----------
        column: str
            Column to run regex_replace on in self.df
        regex: str
            Regex to find strings to replace
        replacement: str
            String to replace matches with in self.df

        Returns
        -------
        None
        """
        self.df = self.df.withColumn(column, regexp_replace(column, regex, replacement))

    def blank_as_null(self, column_array: list = None) -> None:
        """
        Replaces all empty string values in with None in columns of DataFrame.

        Parameters
        ----------
        column_array: list (default=None)
            List of columns in self.df in which empty string values will be replaced with None. If left default, the method will apply to all columns

        Returns
        -------
        None
        """
        if column_array == None:
            for column in self.df.columns:
                if self.df.select(column).dtypes[0][1] == "string":
                    self.df = self.df.withColumn(
                        column, when(col(column) == "", None).otherwise(col(column))
                    )
        else:
            for column in column_array:
                if self.df.select(column).dtypes[0][1] == "string":
                    self.df = self.df.withColumn(
                        column, when(col(column) == "", None).otherwise(col(column))
                    )

    def clean_data(self) -> None:
        """
        Steps to clean data. To be implemented in specific data processing pipelines.
        """
        pass
