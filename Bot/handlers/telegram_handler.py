import telebot
import sys
import os
from config import BOT_TOKEN, CHAT_ID
from handlers.logging_handler import CustomLoggerHandler

class TelegramHandler:
    def __init__(self):
        self.bot = telebot.TeleBot(BOT_TOKEN)
        self.chat_id = CHAT_ID
        self.logger = CustomLoggerHandler(bot_token=BOT_TOKEN, chat_id=CHAT_ID)
        self._setup_stdout_redirect()
        self._setup_handlers()

    def _setup_stdout_redirect(self):
        class StdoutRedirect:
            def __init__(self, telegram_handler):
                self.telegram_handler = telegram_handler
                self.buffer = ""

            def write(self, text):
                self.buffer += text
                if '\n' in self.buffer:
                    lines = self.buffer.split('\n')
                    for line in lines[:-1]:
                        if line.strip():  # Only log non-empty lines
                            self.telegram_handler.logger.log_error(line)
                    self.buffer = lines[-1]
                sys.__stdout__.write(text)

            def flush(self):
                if self.buffer:
                    self.telegram_handler.logger.log_error(self.buffer)
                    self.buffer = ""
                sys.__stdout__.flush()

        sys.stdout = StdoutRedirect(self)

    def send_message(self, message):
        try:
            max_length = 4096
            for i in range(0, len(message), max_length):
                chunk = message[i:i + max_length]
                self.bot.send_message(self.chat_id, chunk)
        except Exception as e:
            self.logger.log_error_with_code("E003", f"Error sending message to Telegram: {e}")

    def _setup_handlers(self):
        @self.bot.message_handler(commands=['start'])
        def handle_start(message):
            self.send_message("Welcome! Type /help for available commands.")

        @self.bot.message_handler(commands=['help'])
        def handle_help(message):
            commands = (
                "/about - Details about the bot\n"
                "/logs - Fetches the log file\n"
                "/errorcodes - Returns the list of all the error codes raised due to exceptions, if any"
            )
            self.send_message(commands)

        @self.bot.message_handler(commands=['about'])
        def handle_about(message):
            about_text = (
                "📊 CrossTrend Bot V1.0\n\n"
                "This bot gives smart alerts based on custom made crosstrend strategy.\n"
                "It fetches realtime OHCLV data, calculates necessary indicators based on historical data, performs entry analysis, and updates it into the MongoDB database while providing alerts.\n"
                "You can get error logs and other information about the bot here.\n"
                "\n"
                "The backtest results of the strategy is in the notion url - https://www.notion.so/Crosstrend-strategy-backtest-1354e48dab1f8074a8d8fcc8e14aaff1?pvs=4"
            )
            self.send_message(about_text)

        @self.bot.message_handler(commands=['logs'])
        def handle_log(message):
            log_file_path = 'D:/TelegramBot/Bot/logs/crosstrend_bot_log.log'
            if os.path.exists(log_file_path):
                with open(log_file_path, 'rb') as log_file:
                    self.bot.send_document(self.chat_id, log_file)
            else:
                self.send_message("Error: Log file not found.")

        @self.bot.message_handler(commands=['errorcodes'])
        def handle_errorcodes(message):
            error_codes = (
                "E001: 'Insufficient Data' — Triggered when the required data is not enough for calculations or analysis.\n"
                "E002: 'Database Error' — Indicates any issue when interacting with the database (in MongoDB).\n"
                "E003: 'Telegram Notification Failure' — Failure in sending a message to Telegram.\n"
                "E004: 'Data Fetch Error' — When there is an error while fetching new data.\n"
                "E005: 'Unexpected Error' — A general fallback error for unexpected or unhandled issues.\n"
                "E006: 'Calculation Error' — Error encountered during any kind of data processing or calculation.\n"
                "E007: 'Polling Error' - Error encountered during bot polling"
            )
            self.send_message(error_codes)
