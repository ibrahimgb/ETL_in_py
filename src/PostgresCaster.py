from datetime import datetime, date, time
from typing import Optional, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresCaster:

    @staticmethod
    def to_boolean(value: Optional[Union[str, bool]]) -> Union[bool, str, None]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            val = value.lower()
            if val in ("y", "yes", "t", "true", "on", "1"):
                return True
            elif val in ("n", "no", "f", "false", "off", "0"):
                return False
        logger.warning(f"Could not cast value to boolean: {value}")
        return value

    @staticmethod
    def to_integer(value: Optional[Union[str, int]]) -> Union[int, str, None]:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not cast value to integer: {value}")
            return value

    @staticmethod
    def to_float(value: Optional[Union[str, float]]) -> Union[float, str, None]:
        if value is None:
            return None
        if isinstance(value, float):
            return value
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not cast value to float: {value}")
            return value

    @staticmethod
    def to_date(value: Optional[Union[str, date]]) -> Union[date, str, None]:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
        logger.warning(f"Could not cast value to date: {value}")
        return value

    @staticmethod
    def to_timestamp(value: Optional[Union[str, datetime]]) -> Union[datetime, str, None]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
        logger.warning(f"Could not cast value to timestamp: {value}")
        return value
    
    @staticmethod
    def to_timestamptz(value: Optional[Union[str, datetime]]) -> Union[datetime, str, None]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=pytz.UTC)
            return value
        if isinstance(value, str):
            formats = (
                "%Y-%m-%d %H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S%z", 
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",   
            )
            value = value.replace('+00', '+0000') 
            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=pytz.UTC)
                    return dt
                except ValueError:
                    continue
        logger.warning(f"Could not cast value to timestamptz: {value}")
        return value
    @staticmethod
    def to_string(value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return str(value)
        except Exception as e:
            logging.warning(f"Could not cast value to string: {value} ({e})")
            return value
