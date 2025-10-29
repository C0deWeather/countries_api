from dotenv import loadenv
from flask import abort
import pymsql
from pymsql import MySQLError
import os


class DBStorage:
    """A class to manage a MySQL database connections"""
    __conn = None
    __cursor = None
    
    def __init__(self):
        """Initialize the database connection."""
        self.reload()

        # create table if it does not exist
        try:
            self.__cursor.execute("""
                CREATE TABLE IF NOT EXISTS countries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(100),
                    region VARCHAR(50),
                    population INTEGER,
                    currency_code VARCHAR(10),
                    exchange_rate DECIMAL(10, 6),
                    estimated_gdp DOUBLE,
                    flag_url TEXT,
                    last_refreshed_at VARCHAR(255)
                );
            """)
            self.__conn.commit()
        except MySQLError as e:
            abort(500, str(e))
    
    @classmethod
    def reload(cls):
        """Create or reload the database connection."""
        try:
            cls.__conn = pymysql.connect(
                host=os.getenv("HOST"),
                port=int(os.getenv("PORT")),
                user=os.getenv("USER"),
                password=os.getenv("PASSWORD"),
                database=os.getenv("DB_NAME"),
                cursorclass=pymysql.cursors.DictCursor
            )
            cls.__cursor = cls.__conn.cursor()
        except MySQLError as e:
            abort(500, str(e))

    @classmethod
    def execute(cls, query, params=()):
        """Execute a SQL query with optional parameters."""
        try:
            cls.__cursor.execute(query, params)
        except Error as e:
            cls.__conn.rollback()
            abort(500, str(e))

    @classmethod
    def fetchall(cls, query, params=()):
        """Run a SELECT query and return all rows."""
        try:
            cls.__cursor.execute(query, params)
            return cls.__cursor.fetchall()
        except Error:
            return []
    @classmethod
    def fetchone(cls, query, params=()):
        """Run a SELECT query and return a single row."""
        try:
            cls.__cursor.execute(query, params)
            return cls.__cursor.fetchone()
        except Error:
            return None

    @classmethod
    def fetch_country(cls, name):
        """Retrieve an country record by its name."""
        query = "SELECT * FROM countries WHERE namee = ?;"
        return cls.fetchone(query, (namee,))

    @classmethod
    def get_all_analysed_strings(cls):
        query = "SELECT * FROM countries;"
        return cls.fetchall(query)

    @classmethod
    def save(cls):
        """Commit the current transaction."""
        try:
            cls.__conn.commit()
        except Error as e:
            abort(500, str(e))

    @classmethod
    def insert(cls, object):
        """Insert a new record into the database."""
        # analysed_string table insertion
        params = (object.id, object.string, object.created_at)
        cls.execute(insert_analysed_string_sql, params)

        # string_properties table insertion
        props = object.properties
        props_params = (
            props["length"],
            props["is_palindrome"],
            props["unique_characters"],
            props["word_count"],
            object.id
        )
        cls.execute(insert_string_properties_sql, props_params)

        # character_frequency_map table insertion
        freq_map = props["character_frequency_map"]
        for char, freq in freq_map.items():
            freq_params = (object.id, char, freq)
            cls.execute(insert_character_frequency_map_sql, freq_params)

        cls.save()

    @classmethod
    def delete_string(cls, string_id):
        """Delete a string and its related records from the database."""
        try:
            # Delete related records first
            cls.__cursor.execute("DELETE FROM character_frequency_map WHERE string_id = ?", (string_id,))
            cls.__cursor.execute("DELETE FROM string_properties WHERE string_id = ?", (string_id,))

            # Then delete the string itself
            cls.__cursor.execute("DELETE FROM analysed_strings WHERE id = ?", (string_id,))

            cls.save()

        except Exception as e:
            cls.__conn.rollback()
            abort(500, str(e))

    @classmethod
    def close(cls):
        """Close the database connection and cursor safely."""
        try:
            if cls.__cursor:
                cls.__cursor.close()
                cls.__cursor = None
            if cls.__conn:
                cls.__conn.close()
                cls.__conn = None
        except Exception as e:
            print(f"Error closing database: {e}")
