import schedule
import time
from threading import Thread
from handlers import TelegramHandler
import traceback
import telebot
from main import main

# Global variable to ensure `run_analysis` runs only once per cycle
analysis_ran = False

def run_analysis():
    """
    Executes the main analysis logic. Handles errors and sends appropriate Telegram notifications.
    """
    global analysis_ran

    # If already executed, skip this call
    if analysis_ran:
        return

    analysis_ran = True
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

    # Reset the flag after execution
    analysis_ran = False


def schedule_thread():
    """
    Manages scheduled tasks and ensures `run_analysis` executes only once per pending period.
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
            if 'Too Many Requests' in str(e):  # Handle rate limiting
                time.sleep(10)  # Wait before retrying to avoid excessive retries
            else:
                error_msg = f"Error in Telegram polling: {str(e)}\n{traceback.format_exc()}"
                telegram_handler.send_message("Error occurred: E007")
                print(error_msg)
                time.sleep(60)  # Wait for a minute before retrying
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

    # Run once immediately at startup
    #run_analysis()

    # Start threads for scheduling and polling
    Thread(target=schedule_thread, daemon=True).start()
    Thread(target=polling_thread, daemon=True).start()

    # Keep the main thread alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main_bot()
