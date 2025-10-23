import os
import re
import io
import sys
import json
import errno
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from sqlite3 import connect
from telebot import TeleBot, types


def ensure_dir(p):
    try:
        os.makedirs(p, exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def today_log_dir():
    d = datetime.now().strftime("%Y-%m-%d")
    p = os.path.join("logs", d)
    ensure_dir(p)
    return p


class DailyLogger:
    def __init__(self, name="bot"):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.current_dir = None
        self._reconfigure_handlers()

    def _reconfigure_handlers(self):
        log_dir = today_log_dir()
        if self.current_dir == log_dir and self.logger.handlers:
            return
        for h in list(self.logger.handlers):
            self.logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass
        self.current_dir = log_dir
        fmt = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        debug_handler = RotatingFileHandler(os.path.join(log_dir, "debug.log"), maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(fmt)
        info_handler = RotatingFileHandler(os.path.join(log_dir, "info.log"), maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(fmt)
        error_handler = RotatingFileHandler(os.path.join(log_dir, "error.log"), maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(fmt)
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(fmt)
        self.logger.addHandler(debug_handler)
        self.logger.addHandler(info_handler)
        self.logger.addHandler(error_handler)
        self.logger.addHandler(console)

    def _pack_user(self, message, extras=None):
        data = {
            "user_id": getattr(message.from_user, "id", None),
            "username": getattr(message.from_user, "username", None),
            "name": " ".join(filter(None, [message.from_user.first_name or "", message.from_user.last_name or ""])).strip() or None
        }
        if extras:
            data.update(extras)
        return json.dumps(data, ensure_ascii=False)

    def debug(self, message, ctx=None):
        self._reconfigure_handlers()
        if ctx is not None:
            self.logger.debug(f"{message} | ctx={self._pack_user(ctx)}")
        else:
            self.logger.debug(message)

    def info(self, message, ctx=None):
        self._reconfigure_handlers()
        if ctx is not None:
            self.logger.info(f"{message} | ctx={self._pack_user(ctx)}")
        else:
            self.logger.info(message)

    def warning(self, message, ctx=None):
        self._reconfigure_handlers()
        if ctx is not None:
            self.logger.warning(f"{message} | ctx={self._pack_user(ctx)}")
        else:
            self.logger.warning(message)

    def error(self, message, ctx=None):
        self._reconfigure_handlers()
        if ctx is not None:
            self.logger.error(f"{message} | ctx={self._pack_user(ctx)}")
        else:
            self.logger.error(message)


class DataBase:
    def __init__(self):
        self.connection = connect('database.db', check_same_thread=False)
        self.cursor = self.connection.cursor()
        self._create()

    def _create(self):
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS Records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            service TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL
        )
        ''')
        self.connection.commit()

    def add_record(self, telegram_id, name, phone, service, date, time):
        self.cursor.execute('''
        INSERT INTO Records (telegram_id, name, phone, service, date, time)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (telegram_id, name, phone, service, date, time))
        self.connection.commit()

    def get_records_for_date(self, date):
        self.cursor.execute('SELECT time, service FROM Records WHERE date = ? ORDER BY time ASC', (date,))
        return self.cursor.fetchall()

    def get_records_detailed_for_date(self, date):
        self.cursor.execute('SELECT id, time, name, phone, service FROM Records WHERE date = ? ORDER BY time ASC', (date,))
        return self.cursor.fetchall()

    def delete_record(self, record_id):
        self.cursor.execute('DELETE FROM Records WHERE id = ?', (record_id,))
        self.connection.commit()

    def get_years(self):
        self.cursor.execute('SELECT DISTINCT substr(date, 7, 4) AS y FROM Records ORDER BY y')
        return [r[0] for r in self.cursor.fetchall() if r[0]]

    def get_months_for_year(self, year):
        self.cursor.execute('SELECT DISTINCT substr(date, 4, 2) AS m FROM Records WHERE substr(date,7,4)=? ORDER BY m', (year,))
        return [r[0] for r in self.cursor.fetchall() if r[0]]

    def get_days_for_year_month(self, year, month):
        self.cursor.execute('SELECT DISTINCT substr(date, 1, 2) AS d FROM Records WHERE substr(date,7,4)=? AND substr(date,4,2)=? ORDER BY d', (year, month))
        return [r[0] for r in self.cursor.fetchall() if r[0]]


class Bot:
    def __init__(self):
        self.log = DailyLogger("nail-bot")
        self.bot = TeleBot(self.api_key(), parse_mode='HTML')
        self.db = DataBase()
        self.admins = [5955591242]
        self.services = {
            "Маникюр + гель-лак": 1.5,
            "Маникюр + укрепление": 2.0,
            "Наращивание (короткие)": 2.0,
            "Наращивание (длинные)": 3.0,
            "Френч (как доп. к услуге)": 0.5
        }
        self.user_state = {}
        self.register_handlers()
        self.log.info("Bot initialized")

    def api_key(self):
        with open("api_key", "r", encoding="utf-8") as f:
            return f.read().strip()

    def _is_valid_name(self, text):
        if not text:
            return False
        text = text.strip()
        return bool(re.fullmatch(r"[A-Za-zА-Яа-яЁё\-\s]{2,50}", text))

    def _is_valid_phone(self, text):
        digits = re.sub(r"[^\d+]", "", text or "")
        return bool(re.fullmatch(r"(\+38|38)\d{10}", digits))

    def _parse_date(self, text):
        try:
            d = datetime.strptime(text.strip(), "%d.%m.%Y").date()
            if d < datetime.today().date():
                return None
            return d
        except Exception:
            return None

    def get_duration(self, service):
        return self.services.get(service, 1.5)

    def get_available_slots(self, date_str, duration_hours):
        work_start = datetime.strptime("12:00", "%H:%M")
        work_end = datetime.strptime("19:00", "%H:%M")
        busy_ranges = []
        for t, srv in self.db.get_records_for_date(date_str):
            s = datetime.strptime(t, "%H:%M")
            d = timedelta(hours=self.get_duration(srv))
            busy_ranges.append((s, s + d))
        slots = []
        cur = work_start
        need = timedelta(hours=duration_hours)
        while cur + need <= work_end:
            ok = True
            for s, e in busy_ranges:
                if not (cur + need <= s or cur >= e):
                    ok = False
                    break
            if ok:
                slots.append(cur.strftime("%H:%M"))
            cur += timedelta(minutes=30)
        return slots

    def format_table(self, records, date_str):
        headers = ["ID", "Время", "Имя", "Телефон", "Услуга"]
        widths = [6, 7, 16, 14, 32]
        def cut(s, w):
            s = str(s or "")
            return s if len(s) <= w else s[:w-1] + "…"
        line = "+".join(["-"*(w+2) for w in widths])
        parts = []
        parts.append(f"Записи на {date_str}")
        parts.append(line)
        parts.append("| " + " | ".join([h.ljust(w) for h, w in zip(headers, widths)]) + " |")
        parts.append(line)
        if not records:
            parts.append("| " + " | ".join([cut("-", w).ljust(w) for w in widths]) + " |")
        else:
            for rid, tm, name, phone, srv in records:
                row = [cut(rid, widths[0]), cut(tm, widths[1]), cut(name, widths[2]), cut(phone, widths[3]), cut(srv, widths[4])]
                parts.append("| " + " | ".join([str(c).ljust(w) for c, w in zip(row, widths)]) + " |")
        parts.append(line)
        table = "<pre>" + "\n".join(parts) + "</pre>"
        return table

    def send_price(self, message):
        txt = (
            "<b>ПРАЙС-ЛИСТ</b>\n\n"
            "▫️ Маникюр (без покрытия) — <b>800</b>\n"
            "▫️ Маникюр + гель-лак — <b>1300</b>\n"
            "▫️ Укрепление — <b>1600</b>\n"
            "▫️ Наращивание ногтей — <b>от 1900</b>\n"
            "▫️ Коррекция нарощенных — <b>от 1700</b>\n"
            "▫️ Ремонт — <b>50</b>\n"
            "▫️ Френч / втирка — <b>200</b>\n"
            "▫️ Дизайн — <b>от 50</b>\n"
            "▫️ Снятие материала (без покрытия) — <b>300</b>\n\n"
            "🌷 Записывайтесь — и ваши ноготки будут идеальны! 💫"
        )
        self.bot.send_message(message.chat.id, txt)
        self.log.info("Sent price", message)

    def send_examples(self, message):
        self.log.info("Request examples", message)
        try:
            p = "photos"
            if not os.path.exists(p):
                self.bot.send_message(message.chat.id, "Папка <b>photos</b> не найдена.")
                self.log.warning("Photos folder missing", message)
                return
            files = [f for f in os.listdir(p) if f.lower().endswith((".jpg", ".jpeg", ".png"))]
            if not files:
                self.bot.send_message(message.chat.id, "Пока нет примеров работ.")
                self.log.info("No examples found", message)
                return
            self.bot.send_message(message.chat.id, "Примеры моих работ:")
            for fname in files:
                try:
                    with open(os.path.join(p, fname), "rb") as ph:
                        self.bot.send_photo(message.chat.id, ph)
                        self.log.debug(f"Sent example {fname}", message)
                except Exception as e:
                    self.log.error(f"Photo send error: {e}", message)
        except Exception as e:
            self.log.error(f"Examples error: {e}", message)
            self.bot.send_message(message.chat.id, "Ошибка при отправке примеров.")

    def register_handlers(self):
        @self.bot.message_handler(commands=['start'])
        def on_start(message):
            is_admin = message.chat.id in self.admins
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add("💅 Записаться", "📸 Примеры")
            kb.add("💰 Прайс-лист")
            if is_admin:
                kb.add("👑 Админка")
            hello = (
                "<b>Добро пожаловать!</b>\n\n"
                "Я помогу вам записаться на маникюр 🌸\n"
                "Работаю ежедневно с <b>12:00</b> до <b>19:00</b>.\n\n"
                "Выберите действие👇"
            )
            self.bot.send_message(message.chat.id, hello, reply_markup=kb)
            self.log.info("Start", message)

        @self.bot.message_handler(func=lambda m: m.text in ["💅 Записаться", "📸 Примеры", "💰 Прайс-лист", "👑 Админка"])
        def route_main(message):
            if message.text == "💅 Записаться":
                self.start_signup(message)
            elif message.text == "📸 Примеры":
                self.send_examples(message)
            elif message.text == "💰 Прайс-лист":
                self.send_price(message)
            elif message.text == "👑 Админка":
                if message.chat.id in self.admins:
                    self.admin_panel(message)
                else:
                    self.bot.send_message(message.chat.id, "Нет доступа.")
                    self.log.warning("Admin denied", message)

        @self.bot.message_handler(commands=['sign_up'])
        def cmd_signup(message):
            self.start_signup(message)

        @self.bot.message_handler(commands=['admin'])
        def cmd_admin(message):
            if message.chat.id in self.admins:
                self.admin_panel(message)
            else:
                self.bot.send_message(message.chat.id, "Нет доступа.")
                self.log.warning("Admin denied", message)

        @self.bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("del_"))
        def cb_delete(call):
            try:
                if call.message.chat.id not in self.admins:
                    self.bot.answer_callback_query(call.id, "Нет прав", show_alert=True)
                    self.log.warning("Delete denied", call.message)
                    return
                rid = int(call.data.split("_")[1])
                self.db.delete_record(rid)
                self.bot.answer_callback_query(call.id, "Удалено")
                self.bot.send_message(call.message.chat.id, "Запись удалена.")
                self.log.info(f"Record deleted id={rid}", call.message)
            except Exception as e:
                self.log.error(f"Delete error: {e}", call.message)
                self.bot.answer_callback_query(call.id, "Ошибка", show_alert=True)

        @self.bot.message_handler(func=lambda m: True)
        def fallback(message):
            self.bot.send_message(message.chat.id, "Напишите /start")
            self.log.debug("Fallback message", message)

    def start_signup(self, message):
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for s in self.services.keys():
            kb.add(s)
        self.bot.send_message(message.chat.id, "Выберите услугу 💅:", reply_markup=kb)
        self.bot.register_next_step_handler(message, self.get_name)
        self.log.info("Signup started", message)

    def get_name(self, message):
        service = message.text
        if service not in self.services:
            self.bot.send_message(message.chat.id, "Пожалуйста, выберите услугу с клавиатуры.")
            self.log.warning("Invalid service", message)
            return self.start_signup(message)
        self.user_state[message.chat.id] = {"service": service}
        self.bot.send_message(message.chat.id, "Введите ваше имя:", reply_markup=types.ReplyKeyboardRemove())
        self.bot.register_next_step_handler(message, self.get_phone)
        self.log.info(f"Service selected: {service}", message)

    def get_phone(self, message):
        name = (message.text or "").strip()
        if not self._is_valid_name(name):
            self.bot.send_message(message.chat.id, "Некорректное имя. Пример: Анна-Мария")
            self.log.warning("Invalid name", message)
            return self.bot.register_next_step_handler(message, self.get_phone)
        self.user_state[message.chat.id]["name"] = name
        self.bot.send_message(message.chat.id, "Введите номер телефона. Пример: +38 999 123-45-67")
        self.bot.register_next_step_handler(message, self.get_date)
        self.log.info(f"Name entered: {name}", message)

    def get_date(self, message):
        phone_raw = (message.text or "").strip()
        if not self._is_valid_phone(phone_raw):
            self.bot.send_message(message.chat.id, "Некорректный номер. Пример: +38 999 1234567")
            self.log.warning("Invalid phone", message)
            return self.bot.register_next_step_handler(message, self.get_date)
        phone = re.sub(r"[^\d+]", "", phone_raw)
        self.user_state[message.chat.id]["phone"] = phone
        self.bot.send_message(message.chat.id, "Введите дату в формате дд.мм.гггг")
        self.bot.register_next_step_handler(message, self.get_time)
        self.log.info(f"Phone entered: {phone}", message)

    def get_time(self, message):
        d = self._parse_date(message.text or "")
        if not d:
            self.bot.send_message(message.chat.id, "Неверная дата или в прошлом. Формат: дд.мм.гггг")
            self.log.warning("Invalid date", message)
            return self.bot.register_next_step_handler(message, self.get_time)
        chat_id = message.chat.id
        service = self.user_state[chat_id]["service"]
        dur = self.get_duration(service)
        date_str = d.strftime("%d.%m.%Y")
        slots = self.get_available_slots(date_str, dur)
        if not slots:
            self.bot.send_message(message.chat.id, "На этот день нет свободных слотов. Введите другую дату.")
            self.log.info("No slots for date", message)
            return self.bot.register_next_step_handler(message, self.get_time)
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for s in slots:
            kb.add(s)
        self.user_state[chat_id]["date"] = d
        self.bot.send_message(message.chat.id, "Выберите время:", reply_markup=kb)
        self.bot.register_next_step_handler(message, self.confirm_record)
        self.log.info(f"Date selected: {date_str}; slots={len(slots)}", message)

    def confirm_record(self, message):
        t = (message.text or "").strip()
        if not re.fullmatch(r"\d{2}:\d{2}", t):
            self.bot.send_message(message.chat.id, "Некорректное время. Выберите на клавиатуре.")
            self.log.warning("Invalid time", message)
            return self.bot.register_next_step_handler(message, self.confirm_record)
        chat_id = message.chat.id
        data = self.user_state.get(chat_id, {})
        if not data:
            self.bot.send_message(chat_id, "Сессия записи сброшена. Наберите /sign_up")
            self.log.warning("Confirm without state", message)
            return
        try:
            self.db.add_record(
                telegram_id=chat_id,
                name=data["name"],
                phone=data["phone"],
                service=data["service"],
                date=data["date"].strftime("%d.%m.%Y"),
                time=t
            )
            self.bot.send_message(
                chat_id,
                (
                    "<b>Запись подтверждена!</b>\n\n"
                    f"💅 Услуга: <b>{data['service']}</b>\n"
                    f"👩 Имя: <b>{data['name']}</b>\n"
                    f"📞 Телефон: <b>{data['phone']}</b>\n"
                    f"📅 Дата: <b>{data['date'].strftime('%d.%m.%Y')}</b>\n"
                    f"⏰ Время: <b>{t}</b>"
                ),
                reply_markup=types.ReplyKeyboardRemove()
            )
            self.log.info(f"Record created {data['service']} {data['date'].strftime('%d.%m.%Y')} {t}", message)
            for admin_id in self.admins:
                try:
                    self.bot.send_message(
                        admin_id,
                        (
                            "<b>Новая запись</b>\n\n"
                            f"💅 {data['service']}\n"
                            f"👩 {data['name']} | 📞 {data['phone']}\n"
                            f"📅 {data['date'].strftime('%d.%m.%Y')} в {t}"
                        )
                    )
                    self.log.info(f"Admin notified {admin_id}", message)
                except Exception as e:
                    self.log.error(f"Notify admin error {admin_id}: {e}", message)
        except Exception as e:
            self.bot.send_message(chat_id, "Ошибка сохранения. Попробуйте ещё раз.")
            self.log.error(f"DB save error: {e}", message)
        finally:
            if chat_id in self.user_state:
                del self.user_state[chat_id]

    def admin_panel(self, message):
        if message.chat.id not in self.admins:
            self.bot.send_message(message.chat.id, "Нет доступа.")
            self.log.warning("Admin panel denied", message)
            return
        years = self.db.get_years()
        if not years:
            self.bot.send_message(message.chat.id, "Записей нет.")
            self.log.info("Admin panel empty years", message)
            return
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for y in years:
            kb.add(y)
        self.bot.send_message(message.chat.id, "Админка: выберите год", reply_markup=kb)
        self.bot.register_next_step_handler(message, self.admin_choose_month)
        self.log.info(f"Admin panel years: {years}", message)

    def admin_choose_month(self, message):
        if message.chat.id not in self.admins:
            return
        year = (message.text or "").strip()
        months = self.db.get_months_for_year(year)
        if not months:
            self.bot.send_message(message.chat.id, "На этот год записей нет.")
            self.log.info("Admin months empty", message)
            return
        self.user_state[message.chat.id] = {"year": year}
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for m in months:
            kb.add(m)
        self.bot.send_message(message.chat.id, f"Год {year}. Выберите месяц", reply_markup=kb)
        self.bot.register_next_step_handler(message, self.admin_choose_day)
        self.log.info(f"Admin months for {year}: {months}", message)

    def admin_choose_day(self, message):
        if message.chat.id not in self.admins:
            return
        month = (message.text or "").strip()
        year = self.user_state.get(message.chat.id, {}).get("year")
        days = self.db.get_days_for_year_month(year, month)
        if not days:
            self.bot.send_message(message.chat.id, "На этот месяц записей нет.")
            self.log.info("Admin days empty", message)
            return
        self.user_state[message.chat.id]["month"] = month
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for d in days:
            kb.add(d)
        self.bot.send_message(message.chat.id, f"{month}.{year}. Выберите день", reply_markup=kb)
        self.bot.register_next_step_handler(message, self.show_records_for_day)
        self.log.info(f"Admin days for {month}.{year}: {days}", message)

    def show_records_for_day(self, message):
        if message.chat.id not in self.admins:
            return
        data = self.user_state.get(message.chat.id, {})
        year = data.get("year")
        month = data.get("month")
        day = (message.text or "").strip()
        date_str = f"{day.zfill(2)}.{month}.{year}"
        recs = self.db.get_records_detailed_for_date(date_str)
        table = self.format_table(recs, date_str)
        self.bot.send_message(message.chat.id, table, reply_markup=types.ReplyKeyboardRemove())
        self.log.info(f"Admin view {date_str} count={len(recs)}", message)
        if recs:
            kb = types.InlineKeyboardMarkup()
            for r in recs:
                rid, tm, name, _, _ = r
                kb.add(types.InlineKeyboardButton(text=f"Удалить {tm} — {name}", callback_data=f"del_{rid}"))
            self.bot.send_message(message.chat.id, "Удаление записей:", reply_markup=kb)

    def run(self):
        self.log.info("Bot started")
        self.bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)
        self.log.info("Bot stopped")


if __name__ == "__main__":
    Bot().run()
