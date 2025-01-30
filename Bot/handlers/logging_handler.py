import os
import logging
import pytz
from datetime import datetime
import telebot
from config import BOT_TOKEN, CHAT_ID, TIMEZONE


# Define a custom log level (importance greater than Warning(30) and less than Errors(40)) for ALERTS
ALERTS_LEVEL = 35
logging.addLevelName(ALERTS_LEVEL, "ALERTS")

def alerts(self, message, *args, **kwargs):
    if self.isEnabledFor(ALERTS_LEVEL):
        self._log(ALERTS_LEVEL, message, args, **kwargs)
logging.Logger.alerts = alerts


class ISTLogFormatter(logging.Formatter):
    """
    Custom log formatter that converts timestamps to Indian Standard Time (IST)
    """
    def __init__(self, fmt=None, datefmt=None, style='%'):
        """
        Initialize the formatter with IST timezone
        
        Args:
            fmt (str): Log message format
            datefmt (str): Date format string
            style (str): Format style (%, {, or $)
        """
        super().__init__(fmt, datefmt, style)
        self.utc_tz = pytz.UTC
        self.ist_tz = pytz.timezone('Asia/Kolkata')

    def formatTime(self, record, datefmt=None):
        """
        Convert log record timestamp to IST
        
        Args:
            record (logging.LogRecord): Log record
            datefmt (str): Date format string
        
        Returns:
            str: Formatted timestamp in IST
        """
        # Create datetime from record's timestamp
        timestamp = datetime.fromtimestamp(record.created, tz=self.utc_tz)
        
        # Convert to IST
        ist_time = timestamp.astimezone(self.ist_tz)
        
        # If no specific date format provided, use a default
        if datefmt is None:
            datefmt = '%Y-%m-%d %H:%M:%S IST'
        
        return ist_time.strftime(datefmt)

class CustomLoggerHandler:
    def __init__(self, base_dir=None, bot_token=None, chat_id=None):
        """
        Initialize a custom logger for CrossTrend Bot
        
        Args:
            base_dir (str, optional): Base directory for log storage. 
                                      If None, uses the parent directory of the current script.
            bot_token (str, optional): Telegram bot token for message notifications
            chat_id (str, optional): Telegram chat ID to send messages to
        """
        # Determine the base directory
        if base_dir is None:
            # Get the parent directory of the current script
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Create logs directory
        logs_dir = os.path.join(base_dir, 'logs')
        os.makedirs(logs_dir, exist_ok=True)

        # Full path to the log file
        log_file = os.path.join(logs_dir, 'crosstrend_bot_log.log')

        # Set up timezones
        self.utc_tz = pytz.UTC
        self.ist_tz = pytz.timezone('Asia/Kolkata')

        # Setup file logger
        self.log_file = log_file
        self.file_logger = self._setup_file_logger()

        # Setup telegram bot if credentials provided
        self.bot = telebot.TeleBot(bot_token) if bot_token else None
        self.chat_id = chat_id

    def _setup_file_logger(self):
        """
        Configure the logger to write to the CrossTrend Bot log file
        
        Returns:
            logging.Logger: Configured logger instance
        """
        # Create logger
        logger = logging.getLogger('crosstrend_bot_logger')
        logger.setLevel(logging.INFO)

        # Clear any existing handlers to prevent duplicate logging
        if logger.handlers:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

        # Create file handler that appends to the single log file
        file_handler = logging.FileHandler(self.log_file, mode='a')
        file_handler.setLevel(logging.INFO)

        # Create custom IST formatter
        formatter = ISTLogFormatter('%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        return logger

    def log_entry_analysis(self, message):
        """
        Log entry analysis message to file and optionally send to Telegram
        
        Args:
            message (str): The message to log and potentially send
        """
        # Log to file with IST timestamp
        self.file_logger.alerts(f"Entry Analysis: {message}")

        # Send to Telegram if bot is configured
        if self.bot and self.chat_id:
            try:
                # Format message with IST timestamp
                formatted_message = f"📊 {datetime.now(self.ist_tz).strftime('%Y-%m-%d %H:%M:%S IST')}\n{message}"
                
                # Handle long messages by splitting
                max_length = 4096
                for i in range(0, len(formatted_message), max_length):
                    chunk = formatted_message[i:i + max_length]
                    self.bot.send_message(self.chat_id, chunk)
            except Exception as e:
                self.file_logger.error(f"Failed to send Telegram message: {e}")

    def log_error(self, message):
        """
        Log error messages to the CrossTrend Bot log file
        
        Args:
            message (str): Error message to log
        """
        self.file_logger.error(message)


    def log_error_with_code(self, error_code, message):
        error_msg = f"ERROR CODE: {error_code} | {message}"
        self.file_logger.error(error_msg)
        if self.bot and self.chat_id:
            try:
                self.bot.send_message(self.chat_id, f"Error occurred: {error_code}")
            except Exception as e:
                self.file_logger.error(f"Failed to send Telegram message for error: {e}")


    def log_info(self, message):
        """
        Log informational messages to the CrossTrend Bot log file
        
        Args:
            message (str): Informational message to log
        """
        self.file_logger.info(message)