# Packages
import json
import re
import shapely

from datetime import datetime
from pyspark.sql import SparkSession
from typing import Any
from uuid import UUID


def generate_schema(json_file_path: str) -> dict[str, Any]:
    spark = SparkSession.builder.appName("JSONSchemaInference").getOrCreate()
    df = spark.read.json(json_file_path, multiLine=True)
    schema = df.schema.json()
    spark.stop()
    return json.loads(schema)


def is_int(string: str) -> bool:
    try:
        int(string)
        return True
    except ValueError:
        return False


def is_float(string: str) -> bool:
    try:
        float(string)
        # Ensure it's not just an integer (e.g., "123" is not considered a float)
        return not is_int(string) and float(string).is_integer() is False
    except ValueError:
        return False


def is_uuid(string: str):
    try:
        UUID(string)
        return True
    except ValueError:
        return False


def check_date(string: str, date_formats: list[str] | None = None) -> bool:
    try:
        datetime.fromisoformat(string)
        return True, "TIMESTAMP"
    except ValueError:
        pass
    try:
        datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True, "TIMESTAMP"
    except ValueError:
        pass
    if date_formats is None:
        # Common date formats to check
        date_formats = [
            "%Y-%m-%d",  # 2023-10-25
            "%Y/%m/%d",  # 2023/10/25
            "%d-%m-%Y",  # 25-10-2023
            "%d/%m/%Y",  # 25/10/2023
            "%m/%d/%Y",  # 10/25/2023 (US format)
        ]
    for fmt in date_formats:
        try:
            datetime.strptime(string, fmt)
            return True, "DATE"
        except ValueError:
            pass
    return False, None


def check_geometry(string: str) -> tuple[bool, str | None]:
    """Check if a string represents valid geospatial data (WKT or GeoJSON) and return PostGIS type."""
    # Try parsing as WKT
    try:
        geom = shapely.from_wkt(string)
        geom_type = geom.geom_type.upper()
        return True, f"GEOMETRY({geom_type})"
    except (shapely.errors.ShapelyError, ValueError):
        pass
    except TypeError:
        print("ERROR!: " + string)

    # Try parsing as GeoJSON
    try:
        geojson = json.loads(string)
        if isinstance(geojson, dict) and "type" in geojson and "coordinates" in geojson:
            geom = shapely.geometry.shape(geojson)
            geom_type = geom.geom_type.upper()
            return True, f"GEOMETRY({geom_type})"
    except (json.JSONDecodeError, ValueError):
        pass
    except TypeError:
        print("ERROR!: " + string)

    # Try parsing as WKB hex
    try:
        if re.match(r"^[0-9A-Fa-f]+$", string):
            geom = shapely.from_wkb(bytes.fromhex(string))
            geom_type = geom.geom_type.upper()
            return True, f"GEOMETRY({geom_type})"
    except (ValueError, shapely.errors.ShapelyError):
        pass
    except TypeError:
        print("ERROR!: " + string)

    return False, None


def get_type(string: str) -> str:
    is_geom, geom_type = check_geometry(string)
    is_date, date_type = check_date(string)
    if is_int(string):
        return "INTEGER"
    elif is_float(string):
        return "DECIMAL"
    elif is_date:
        return date_type
    elif is_geom:
        return geom_type
    elif is_uuid(string):
        return "UUID"
    return "TEXT"


def set_type(data_types: list[str]) -> str:
    if "TEXT" in data_types:
        return "TEXT"
    elif "GEOMETRY(POINT)" in data_types:
        return "GEOMETRY(POINT)"
    elif "GEOMETRY" in data_types:
        return "GEOMETRY"
    elif "TIMESTAMP" in data_types:
        return "TIMESTAMP"
    elif "DATE" in data_types:
        return "DATE"
    elif "DECIMAL" in data_types:
        return "DECIMAL"
    elif "INTEGER" in data_types:
        return "INTEGER"
    elif "UUID" in data_types:
        return "UUID"
    return "TEXT"


def sqlify_names(s: str) -> str:
    sql_str = ""
    for i in range(len(s)):
        match s[i].lower():
            case "æ":
                utf_8_str = "ae"
            case "ø":
                utf_8_str = "oe"
            case "å":
                utf_8_str = "aa"
            case _:
                utf_8_str = s[i].lower()
        if i == len(s) - 1:
            sql_str += utf_8_str
            return sql_str
        if (not i == 0) and (s[i].isupper()):
            if s[i + 1].islower():
                sql_str += "_"
            sql_str += utf_8_str
        else:
            sql_str += utf_8_str
