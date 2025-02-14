

import logging
import asyncio
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telethon import TelegramClient, errors
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, PeerIdInvalidError
)

########################################
# Logging
########################################
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

########################################
# Globale Variablen
########################################
BOT_TOKEN = "7774783720:AAH9VuJYRaL6k0Ey4pfkUXwY5LhMsCAdbmQ"
USER_STATE = {}        # user_id -> Zustand
USER_TAGGER_TASKS = {} # user_id -> asyncio.Task

########################################
# Überschreiben der FloodWait-Behandlung
########################################
async def no_flood_wait(wait_time):
    raise FloodWaitError(wait_time)

########################################
# Tastaturen
########################################
def start_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Weiter ▶️", callback_data="continue")]
    ])

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Konten", callback_data="menu_accounts")],
        [
            InlineKeyboardButton("Tagger starten 🚀", callback_data="launch_tagger"),
            InlineKeyboardButton("Tagger stoppen 🛑", callback_data="stop_tagger")
        ],
        [InlineKeyboardButton("Anleitung 📚", callback_data="instructions")],
    ])

def accounts_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Konto Nr. 1", callback_data="account_1")],
        [InlineKeyboardButton("Konto Nr. 2", callback_data="account_2")],
        [InlineKeyboardButton("<< Zurück", callback_data="go_back_main_menu")]
    ])

def digit_keyboard(current_code=""):
    kb = [
        [InlineKeyboardButton("1", callback_data="digit_1"),
         InlineKeyboardButton("2", callback_data="digit_2"),
         InlineKeyboardButton("3", callback_data="digit_3")],
        [InlineKeyboardButton("4", callback_data="digit_4"),
         InlineKeyboardButton("5", callback_data="digit_5"),
         InlineKeyboardButton("6", callback_data="digit_6")],
        [InlineKeyboardButton("7", callback_data="digit_7"),
         InlineKeyboardButton("8", callback_data="digit_8"),
         InlineKeyboardButton("9", callback_data="digit_9")],
        [InlineKeyboardButton("0", callback_data="digit_0"),
         InlineKeyboardButton("Löschen ⬅️", callback_data="digit_del"),
         InlineKeyboardButton("OK✅", callback_data="digit_submit")]
    ]
    return InlineKeyboardMarkup(kb)

########################################
# /start
########################################
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    USER_STATE[user_id] = "MAIN_MENU"
    if 'accounts' not in context.user_data:
        context.user_data['accounts'] = {
            1: {'client': None, 'api_id': None, 'api_hash': None, 'phone': None, 'is_authorized': False},
            2: {'client': None, 'api_id': None, 'api_hash': None, 'phone': None, 'is_authorized': False},
        }
    await update.message.reply_text(
        "Hallo! Drücke 'Weiter', um das Menü zu sehen:",
        reply_markup=start_keyboard()
    )

########################################
# Hole die letzte NICHT-Systemnachricht
########################################
async def get_last_non_service_message(client: TelegramClient, source_group: str):
    entity = await client.get_entity(source_group)
    raw_msgs = await client.get_messages(entity, limit=10)
    for m in raw_msgs:
        if not m.action:
            return m
    return None

########################################
# Hauptfunktion des Spams (paralleles Senden)
########################################
async def run_tagger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    source_group = context.user_data.get('source_group')
    spam_interval = context.user_data.get('spam_interval', 60.0)
    rotation_interval = context.user_data.get('rotation_interval', 300.0)
    acc_data = context.user_data['accounts']
    client1 = acc_data[1]['client']
    client2 = acc_data[2]['client']

    if not (acc_data[1]['is_authorized'] and acc_data[2]['is_authorized']):
        await update.effective_message.reply_text("Beide Konten sind nicht autorisiert! Richte sie über das Menü 'Konten' ein.")
        return
    if not source_group:
        await update.effective_message.reply_text("Keine Quellgruppe angegeben.")
        return

    stop_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Tagger stoppen 🛑", callback_data="stop_tagger")]
    ])
    await update.effective_message.reply_text(
        f"🚀 Starte den Versand!\nVersandintervall: {spam_interval} Sek.\nKontenwechsel alle: {rotation_interval} Sek.",
        reply_markup=stop_keyboard
    )

    current_account = 1
    next_switch_time = time.time() + rotation_interval

    try:
        while True:
            try:
                active_client = client1 if current_account == 1 else client2
                last_msg = await get_last_non_service_message(active_client, source_group)
                if last_msg:
                    dialogs = await active_client.get_dialogs(limit=None)
                    target_chats = [d for d in dialogs if (d.is_group or d.is_channel)]
                    tasks = []
                    for chat in target_chats:
                        tasks.append(asyncio.create_task(
                            send_message_to_chat(active_client, chat, last_msg, current_account)
                        ))
                    await asyncio.gather(*tasks)
                await asyncio.sleep(spam_interval)
                if time.time() >= next_switch_time:
                    current_account = 2 if current_account == 1 else 1
                    next_switch_time = time.time() + rotation_interval
                    logger.info(f"Wechsel zu Konto Nr. {current_account}")
            except asyncio.CancelledError:
                logger.info("Tagger gestoppt.")
                break
            except Exception as e:
                logger.error(f"Fehler in der Hauptschleife: {e}")
                await asyncio.sleep(5)
    finally:
        if client1 and client1.is_connected():
            await client1.disconnect()
        if client2 and client2.is_connected():
            await client2.disconnect()
        USER_TAGGER_TASKS.pop(user_id, None)
        USER_STATE[user_id] = "MAIN_MENU"
        await update.effective_message.reply_text("🛑 Tagger gestoppt.", reply_markup=main_menu_keyboard())

async def send_message_to_chat(client, chat, last_msg, account):
    try:
        if last_msg.message:
            await client.send_message(chat.id, last_msg.message)
        elif last_msg.media:
            await client.send_file(chat.id, last_msg.media)
        logger.info(f"[Konto {account}] Gesendet an {chat.name or chat.id}")
    except FloodWaitError:
        # FloodWaitError ignorieren, ohne Verzögerung abzuwarten
        pass
    except Exception as e:
        logger.error(f"[Konto {account}] Fehler beim Senden an {chat.name or chat.id}: {e}")

########################################
# Callback-Handler
########################################
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id
    await query.answer()
    if data == "continue":
        USER_STATE[user_id] = "MAIN_MENU"
        await query.edit_message_text("Hauptmenü:", reply_markup=main_menu_keyboard())
    elif data == "menu_accounts":
        USER_STATE[user_id] = "CHOOSE_ACCOUNT"
        await query.edit_message_text("Wähle ein Konto:", reply_markup=accounts_menu_keyboard())
    elif data == "go_back_main_menu":
        USER_STATE[user_id] = "MAIN_MENU"
        await query.edit_message_text("Hauptmenü:", reply_markup=main_menu_keyboard())
    elif data == "account_1":
        USER_STATE[user_id] = "ENTER_API_ID_1"
        await query.edit_message_text("Gib die API-ID (Zahl) für Konto Nr. 1 ein:")
    elif data == "account_2":
        USER_STATE[user_id] = "ENTER_API_ID_2"
        await query.edit_message_text("Gib die API-ID (Zahl) für Konto Nr. 2 ein:")
    elif data == "launch_tagger":
        USER_STATE[user_id] = "WAITING_SOURCE_GROUP"
        await query.edit_message_text("Gib den @Link oder Benutzernamen der Quellgruppe ein:")
    elif data == "stop_tagger":
        task = USER_TAGGER_TASKS.get(user_id)
        if task and not task.done():
            task.cancel()
        else:
            await query.edit_message_text("Tagger ist nicht gestartet.", reply_markup=main_menu_keyboard())
    elif data == "instructions":
        text_instructions = (
            "1) Öffne 'Konten' und richte beide Konten ein.\n"
            "2) Starte den Tagger, indem du die Quellgruppe, das Versandintervall und das Kontowechselintervall angibst.\n"
            "3) Der Bot sendet die neuesten 'normalen' Nachrichten (ohne Systemnachrichten)."
        )
        await query.edit_message_text(text_instructions, reply_markup=main_menu_keyboard())
    elif data.startswith("digit_"):
        action = data.split("_")[1]
        state = USER_STATE.get(user_id, "")
        if "WAITING_CODE_1" in state:
            acc_number = 1
        elif "WAITING_CODE_2" in state:
            acc_number = 2
        else:
            await query.answer("Unerwartete Code-Eingabe.", show_alert=True)
            return
        current_code = context.user_data.get(f'code_{acc_number}', '')
        if action.isdigit():
            if len(current_code) < 6:
                current_code += action
                context.user_data[f'code_{acc_number}'] = current_code
            else:
                await query.answer("Maximale Code-Länge 6", show_alert=True)
        elif action == "del":
            current_code = current_code[:-1]
            context.user_data[f'code_{acc_number}'] = current_code
        elif action == "submit":
            await confirm_code(update, context, acc_number)
            return
        masked_code = '*' * len(current_code) + '_' * (6 - len(current_code))
        await query.edit_message_text(
            f"Konto Nr. {acc_number}. Gib den Code ein: {masked_code}",
            reply_markup=digit_keyboard(current_code)
        )

########################################
# Text-Handler
########################################
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if 'accounts' not in context.user_data:
        context.user_data['accounts'] = {
            1: {'client': None, 'api_id': None, 'api_hash': None, 'phone': None, 'is_authorized': False},
            2: {'client': None, 'api_id': None, 'api_hash': None, 'phone': None, 'is_authorized': False},
        }
    state = USER_STATE.get(user_id, "")
    if state == "ENTER_API_ID_1":
        if not update.message.text.strip().isdigit():
            await update.message.reply_text("Gib eine Zahl (API-ID) ein:")
            return
        acc_data = context.user_data['accounts'][1]
        acc_data['api_id'] = int(update.message.text.strip())
        USER_STATE[user_id] = "ENTER_API_HASH_1"
        await update.message.reply_text("Gib den API-Hash für Konto Nr. 1 ein:")
        return
    if state == "ENTER_API_HASH_1":
        acc_data = context.user_data['accounts'][1]
        acc_data['api_hash'] = update.message.text.strip()
        USER_STATE[user_id] = "ENTER_PHONE_1"
        await update.message.reply_text("Gib die Telefonnummer (Format +9999999999) für Konto Nr. 1 ein:")
        return
    if state == "ENTER_PHONE_1":
        phone = update.message.text.strip()
        if not phone.startswith('+') or not phone[1:].isdigit():
            await update.message.reply_text("Telefonformat: +123456789")
            return
        acc_data = context.user_data['accounts'][1]
        acc_data['phone'] = phone
        USER_STATE[user_id] = "WAITING_CODE_1"
        await update.message.reply_text("Fordere den Code von Telegram an...")
        await create_telethon_client(update, context, acc_number=1)
        return
    if state == "ENTER_API_ID_2":
        if not update.message.text.strip().isdigit():
            await update.message.reply_text("Gib eine Zahl (API-ID) ein:")
            return
        acc_data = context.user_data['accounts'][2]
        acc_data['api_id'] = int(update.message.text.strip())
        USER_STATE[user_id] = "ENTER_API_HASH_2"
        await update.message.reply_text("Gib den API-Hash für Konto Nr. 2 ein:")
        return
    if state == "ENTER_API_HASH_2":
        acc_data = context.user_data['accounts'][2]
        acc_data['api_hash'] = update.message.text.strip()
        USER_STATE[user_id] = "ENTER_PHONE_2"
        await update.message.reply_text("Gib die Telefonnummer (Format +9999999999) für Konto Nr. 2 ein:")
        return
    if state == "ENTER_PHONE_2":
        phone = update.message.text.strip()
        if not phone.startswith('+') or not phone[1:].isdigit():
            await update.message.reply_text("Telefonformat: +123456789")
            return
        acc_data = context.user_data['accounts'][2]
        acc_data['phone'] = phone
        USER_STATE[user_id] = "WAITING_CODE_2"
        await update.message.reply_text("Fordere den Code von Telegram an...")
        await create_telethon_client(update, context, acc_number=2)
        return
    if state == "WAITING_PASSWORD_1":
        pw = update.message.text.strip()
        acc_data = context.user_data['accounts'][1]
        client = acc_data['client']
        if not client:
            await update.message.reply_text("Client nicht initialisiert. Starte von vorn.")
            return
        try:
            await client.sign_in(password=pw)
            acc_data['is_authorized'] = True
            USER_STATE[user_id] = "MAIN_MENU"
            await update.message.reply_text("Konto Nr. 1 erfolgreich autorisiert!", reply_markup=main_menu_keyboard())
        except errors.PasswordHashInvalidError:
            await update.message.reply_text("Falsches Passwort. Versuche es erneut.")
        except FloodWaitError as e:
            await update.message.reply_text(f"Zu viele Versuche. Warte {e.seconds} Sek.")
            USER_STATE[user_id] = "MAIN_MENU"
        except Exception as e:
            await update.message.reply_text(f"Fehler: {e}")
        return
    if state == "WAITING_PASSWORD_2":
        pw = update.message.text.strip()
        acc_data = context.user_data['accounts'][2]
        client = acc_data['client']
        if not client:
            await update.message.reply_text("Client nicht initialisiert. Starte von vorn.")
            return
        try:
            await client.sign_in(password=pw)
            acc_data['is_authorized'] = True
            USER_STATE[user_id] = "MAIN_MENU"
            await update.message.reply_text("Konto Nr. 2 erfolgreich autorisiert!", reply_markup=main_menu_keyboard())
        except errors.PasswordHashInvalidError:
            await update.message.reply_text("Falsches Passwort. Versuche es erneut.")
        except FloodWaitError as e:
            await update.message.reply_text(f"Zu viele Versuche. Warte {e.seconds} Sek.")
            USER_STATE[user_id] = "MAIN_MENU"
        except Exception as e:
            await update.message.reply_text(f"Fehler: {e}")
        return
    if state == "WAITING_SOURCE_GROUP":
        source_group = update.message.text.strip()
        if not source_group:
            await update.message.reply_text("Gib einen gültigen Link/Benutzernamen der Gruppe ein.")
            return
        context.user_data['source_group'] = source_group
        USER_STATE[user_id] = "WAITING_SPAM_INTERVAL"
        await update.message.reply_text("Gib das Versandintervall (Sekunden), z. B. 60, ein:")
        return
    if state == "WAITING_SPAM_INTERVAL":
        try:
            val = float(update.message.text.strip())
            if val <= 0:
                raise ValueError("Das Intervall muss > 0 sein.")
            context.user_data['spam_interval'] = val
            USER_STATE[user_id] = "WAITING_ROTATION_INTERVAL"
            await update.message.reply_text("Gib nun das Kontowechselintervall (Sekunden), z. B. 300, ein:")
        except ValueError:
            await update.message.reply_text("Bitte gib eine positive Zahl ein. Versuche es erneut.")
        return
    if state == "WAITING_ROTATION_INTERVAL":
        try:
            val = float(update.message.text.strip())
            if val <= 0:
                raise ValueError("Das Intervall muss > 0 sein.")
            context.user_data['rotation_interval'] = val
            await update.message.reply_text("Einstellungen übernommen! Starte den Versand...")
            USER_STATE[user_id] = "SPAM_READY"
            task = asyncio.create_task(run_tagger(update, context))
            USER_TAGGER_TASKS[user_id] = task
        except ValueError:
            await update.message.reply_text("Bitte gib eine positive Zahl ein. Versuche es erneut.")
        return
    await update.message.reply_text("Unbekannter Befehl. Bitte verwende das Menü.")

########################################
# Bestätigung des Codes
########################################
async def confirm_code(update: Update, context: ContextTypes.DEFAULT_TYPE, acc_number: int):
    user_id = update.effective_user.id
    code = context.user_data.get(f'code_{acc_number}', '')
    if not code:
        await update.effective_message.reply_text("Code ist leer. Bitte erneut eingeben.")
        return
    acc_data = context.user_data['accounts'][acc_number]
    client = acc_data['client']
    if not client:
        await update.effective_message.reply_text("Client nicht initialisiert. Starte von vorn.")
        return
    phone_number = acc_data['phone']
    try:
        await client.sign_in(phone_number, code)
    except SessionPasswordNeededError:
        USER_STATE[user_id] = f"WAITING_PASSWORD_{acc_number}"
        await update.effective_message.reply_text("Zwei-Faktor-Authentifizierung ist aktiviert. Gib dein Passwort ein:")
        return
    except FloodWaitError as e:
        await update.effective_message.reply_text(f"Zu viele Versuche. Warte {e.seconds} Sek.")
        USER_STATE[user_id] = "MAIN_MENU"
        return
    except errors.PhoneCodeInvalidError:
        await update.effective_message.reply_text("Falscher Code. Bitte erneut eingeben.")
        context.user_data[f'code_{acc_number}'] = ""
        await update.effective_message.reply_text(
            f"Konto Nr. {acc_number}. Gib den Code ein:",
            reply_markup=digit_keyboard()
        )
        USER_STATE[user_id] = f"WAITING_CODE_{acc_number}"
        return
    except Exception as e:
        await update.effective_message.reply_text(f"Fehler bei der Code-Eingabe: {e}")
        return
    acc_data['is_authorized'] = True
    USER_STATE[user_id] = "MAIN_MENU"
    await update.effective_message.reply_text(f"Konto Nr. {acc_number} erfolgreich autorisiert!",
                                              reply_markup=main_menu_keyboard())

########################################
# Erstellen/Verbinden des Telethon-Clients
########################################
async def create_telethon_client(update: Update, context: ContextTypes.DEFAULT_TYPE, acc_number: int):
    acc_data = context.user_data['accounts'][acc_number]
    api_id = acc_data['api_id']
    api_hash = acc_data['api_hash']
    phone_number = acc_data['phone']
    if not api_id or not api_hash or not phone_number:
        await update.message.reply_text("Unvollständige API-Daten. Starte von vorn.")
        return
    session_name = f"session_user_{update.effective_user.id}_acc_{acc_number}"
    # Der Parameter flood_sleep_threshold=0 deaktiviert das automatische Warten,
    # und wir überschreiben _handle_flood_wait, um sofort eine Ausnahme zu werfen.
    if not acc_data['client']:
        client = TelegramClient(session_name, api_id, api_hash, flood_sleep_threshold=0)
        await client.connect()
        client._handle_flood_wait = no_flood_wait
        acc_data['client'] = client
    else:
        client = acc_data['client']
        if not client.is_connected():
            await client.connect()
            client._handle_flood_wait = no_flood_wait
    try:
        if not await client.is_user_authorized():
            await client.send_code_request(phone_number)
            context.user_data[f'code_{acc_number}'] = ""
            USER_STATE[update.effective_user.id] = f"WAITING_CODE_{acc_number}"
            await update.message.reply_text(
                f"Konto Nr. {acc_number}. Gib den Telegram-Code ein:",
                reply_markup=digit_keyboard()
            )
        else:
            acc_data['is_authorized'] = True
            USER_STATE[update.effective_user.id] = "MAIN_MENU"
            await update.message.reply_text(
                f"Konto Nr. {acc_number} ist bereits autorisiert!",
                reply_markup=main_menu_keyboard()
            )
    except FloodWaitError as e:
        await update.message.reply_text(f"FloodWaitError: Warte {e.seconds} Sek.")
        USER_STATE[update.effective_user.id] = "MAIN_MENU"
    except Exception as e:
        await update.message.reply_text(f"Fehler: {e}")
        USER_STATE[update.effective_user.id] = "MAIN_MENU"

########################################
# MAIN
########################################
if __name__ == "__main__":
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    application.run_polling()
