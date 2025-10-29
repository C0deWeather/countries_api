"""This module defines the DBStorage class for managing MySQL database connections and operations."""
from dotenv import load_dotenv
from flask import abort
import pymysql
from pymysql import MySQLError
from pymysql.cursors import DictCursor
import os
from typing import List


load_dotenv()

class DBStorage:
    """A class to manage a MySQL database connection
    and perform operations.
    """
    __conn = None
    __cursor = None
    
    def __init__(self):
        """Initialize the database connection."""
        self.reload()

        # create table if it does not exist
        try:
            self.__cursor.execute("""
                CREATE TABLE IF NOT EXISTS countries (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    region VARCHAR(50),
                    population INT,
                    currency_code VARCHAR(10),
                    exchange_rate DECIMAL(10, 6),
                    estimated_gdp FLOAT,
                    flag_url TEXT,
                    last_refreshed_at VARCHAR(255)
                );

            """)
            self.__conn.commit()
        except MySQLError as e:
            abort(500, description=f"couldn't create table: {str(e)}")
    
    @classmethod
    def reload(cls):
        """Create or reload the database connection."""
        try:
            cls.__conn = pymysql.connect(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT")),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
            )
            cls.__cursor = cls.__conn.cursor(DictCursor)
        except MySQLError as e:
            abort(500, str(e))

    @classmethod
    def execute(cls, query: str, params: tuple = ()):
        """Execute a SQL query with optional parameters."""
        try:
            cls.__cursor.execute(query, params)
        except MySQLError as e:
            cls.__conn.rollback()
            abort(500, description=f"couldn't execute query: {str(e)}")

    @classmethod
    def fetchall(cls, query: str, params: tuple = ()) -> List[dict]:
        """Run a SELECT query and return all rows."""
        try:
            cls.__cursor.execute(query, params)
            result = cls.__cursor.fetchall()
            print("fetchall result type:", type(result))
            print("first record type:", type(result[0]) if result else "no records")
            if result and not isinstance(result[0], dict):
                abort(500, description="Database returned unexpected data format")
            return list[result]
        except MySQLError:
            return []

    @classmethod
    def fetchone(cls, query: str, params: tuple = ()):
        """Run a SELECT query and return a single row."""
        try:
            cls.__cursor.execute(query, params)
            return cls.__cursor.fetchone()
        except MySQLError:
            return None

    @classmethod
    def fetch_country(cls, name: str) -> dict:
        """Retrieve a country record by its name."""
        query = "SELECT * FROM countries WHERE name = %s;"
        return cls.fetchone(query, (name,))

    @classmethod
    def get_all_countries(cls):
        print("The cursor being used is ", type(cls.__cursor))
        query = "SELECT * FROM countries;"
        return cls.fetchall(query)

    @classmethod
    def save(cls):
        """Commit the current transaction."""
        try:
            cls.__conn.commit()
        except MySQLError as e:
            abort(
                500,
                description=f"couldn't save the current transaction: {str(e)}"
            )

    @classmethod
    def populate_countries(cls, records: List[dict]):
        """populate the countries table with a list of records."""
        
        for record in records:
            insert_sql = """
                INSERT INTO countries (
                    name,
                    region,
                    population,
                    currency_code,
                    exchange_rate,
                    estimated_gdp,
                    flag_url,
                    last_refreshed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            params = (
                record["name"],
                record["region"],
                record["population"],
                record["currency_code"],
                record["exchange_rate"],
                record["estimated_gdp"],
                record["flag_url"],
                record["last_refreshed_at"]
            )
            cls.execute(insert_sql, params)

        cls.save()

    @classmethod
    def query_by_filter(cls, args: dict) -> List[dict]:
        """Build SQL query based on filter arguments and execute it."""

        filters = []
        params = []

        # if name filter is provided, fetch by name only
        if name := args.get(name):
            data = fetch_country(name)
            if not data:
                abort(
                    404,
                    description="Country not found")
            return [data]

        if currency_code := args.get("currency_code"):
            filters.append("currency_code = %s")
            params.append(currency_code)

        elif region := args.get("region"):
            filters.append("region = %s")
            params.append(region)

            if args.get('sort') == "gdp_desc":
                filters.append("estimated_gdp IS NOT NULL")
                order_clause = "ORDER BY estimated_gdp DESC"
            elif args.get('sort') == "gdp_asc":
                filters.append("estimated_gdp IS NOT NULL")
                order_clause = "ORDER BY estimated_gdp ASC"
            elif args.get('sort') is None:
                order_clause = ""
            else:
                abort(400, description="Invalid sort parameter")

        # no filters, select all
        where_clause = " AND ".join(filters) if filters else "1=1"

        # build query
        query = f"""
            SELECT countries.*
            FROM countries
            WHERE {where_clause}
            {order_clause};
        """
        data = storage.fetchall(query, tuple(params))
        if not data:
            abort(
                404,
                description="No countries found matching the criteria"
            )
        return data 


    @classmethod
    def update_countries(cls, parameters: List[dict]):
        """Update multiple country records in the database."""

        update_sql = """
            UPDATE countries
            SET population = %s,
                currency_code = %s,
                exchange_rate = %s,
                estimated_gdp = %s,
                flag_url = %s,
                last_refreshed_at = %s
            WHERE name = %s;
        """

        for params in parameters:
            cls.execute(update_sql, (
                params["population"],
                params["currency_code"],
                params["exchange_rate"],
                params["estimated_gdp"],
                params["flag_url"],
                params["last_refreshed_at"],
                params["name"]
            ))

        cls.save()
            
    @classmethod
    def delete_country(cls, name: str):
        """Delete a country record by its name."""
        cls.__cursor.execute("DELETE FROM countries WHERE name = ?", (name,))
        cls.save()

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
            abort(500, description=f"Error closing database: {e}")
