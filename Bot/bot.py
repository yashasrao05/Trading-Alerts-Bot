import schedule
import time
from threading import Thread
from handlers import TelegramHandler
import traceback
import requests
from http.client import RemoteDisconnected
import telebot
from main import main


def run_analysis():
    """
    Executes the main analysis logic. Handles errors and sends appropriate Telegram notifications.
    """
    try:
        main()
    except ValueError:
        telegram_handler = TelegramHandler()
        telegram_handler.send_message("Error occurred: E001 - Insufficient data")
    except Exception as e:
        error_msg = f"Error in analysis: {str(e)}\n{traceback.format_exc()}"
        telegram_handler = TelegramHandler()
        telegram_handler.send_message("Error occurred: E005")
        print(error_msg)


def schedule_thread():
    """
    Manages scheduled tasks.
    """
    while True:
        try:
            schedule.run_pending()
            time.sleep(30)  # Check pending tasks every 30 seconds
        except Exception as e:
            telegram_handler = TelegramHandler()
            error_msg = f"Error in schedule loop: {str(e)}\n{traceback.format_exc()}"
            telegram_handler.send_message("Error occurred: E004")
            print(error_msg)
            time.sleep(60)  # Pause for a minute on error


def polling_thread():
    """
    Manages Telegram bot polling without blocking other tasks.
    """
    telegram_handler = TelegramHandler()
    while True:
        try:
            telegram_handler.bot.polling(none_stop=True, interval=5)
        
        except telebot.apihelper.ApiException as e:
            error_text = str(e)
            if 'Too Many Requests' in error_text:
                print(f"⚠️ Rate limit hit: {error_text}. Retrying in 30s...")
                time.sleep(30)  # Longer delay for rate limiting
            else:
                error_msg = f"API Error in polling: {error_text}\n{traceback.format_exc()}"
                telegram_handler.send_message("Error occurred: E007")
                print(error_msg)
                time.sleep(60)

        except (requests.exceptions.ConnectionError, RemoteDisconnected) as e:
            print(f"⚠️ Network issue: {str(e)}. Retrying in 10s...")
            time.sleep(10)  # Short retry delay for network issues

        except requests.exceptions.ReadTimeout as e:
            print(f"⚠️ Read timeout: {str(e)}. Retrying in 10s...")
            time.sleep(10)

        except Exception as e:
            error_msg = f"Unexpected error in polling: {str(e)}\n{traceback.format_exc()}"
            telegram_handler.send_message("Error occurred: E007")
            print(error_msg)
            time.sleep(60)  # Wait before retrying


def main_bot():
    """
    Main function for starting the bot, scheduling tasks, and handling polling.
    """
    # Initialize Telegram handler
    telegram_handler = TelegramHandler()

    # Schedule the analysis tasks
    schedule.every().day.at("05:31").do(run_analysis)
    schedule.every().day.at("09:31").do(run_analysis)
    schedule.every().day.at("13:31").do(run_analysis)
    schedule.every().day.at("17:31").do(run_analysis)
    schedule.every().day.at("21:31").do(run_analysis)
    schedule.every().day.at("01:31").do(run_analysis)

    run_analysis() # Run immediately once at startup

    # Start threads for scheduling and polling
    Thread(target=schedule_thread, daemon=True).start()
    Thread(target=polling_thread, daemon=True).start()

    # Keep the main thread alive
    while True: 
        time.sleep(1)


if __name__ == "__main__":
    main_bot()
