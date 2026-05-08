"""
Celery tasks — fixed syntax (no multiple * unpacks), lazy imports inside tasks.
"""
import eventlet
eventlet.monkey_patch()

import os, random, math, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, date, timedelta, timezone
from celery import Celery
from celery.schedules import crontab

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ── Build celery app ──────────────────────────────────────────────────────────
# Import only the Flask app object (not models) to get the name + config.
from app import app as flask_app

celery = Celery(flask_app.name, broker=REDIS_URL, backend=REDIS_URL)
celery.conf.update(
    task_serializer="json", result_serializer="json",
    accept_content=["json"], timezone="Africa/Nairobi", enable_utc=True,
    beat_schedule={
        "price-simulation-every-30s": {
            "task": "tasks.simulate_market_volatility", "schedule": 30.0,
        },
        "arbitrage-scan-every-60s": {
            "task": "tasks.detect_and_alert_arbitrage", "schedule": 60.0,
        },
        "weather-rotate-every-5m": {
            "task": "tasks.rotate_weather", "schedule": 300.0,
        },
        "morning-market-blast-7am": {
            "task": "tasks.morning_market_blast",
            "schedule": crontab(hour=7, minute=0),
        },
        "expire-bids-every-5m": {
            "task": "tasks.expire_old_bids", "schedule": 300.0,
        },
    },
)


# ── Lazy import helper — called inside each task body to avoid circular import ─
def _imports():
    from app import (
        app, db,
        Market, Commodity, PriceEntry, WeatherLog, MarketDay,
        Bid, Notification, User, TransportSacco, ArbitrageAlert,
        haversine, smart_base_price, latest_price, latest_weather,
        c_slug, _new_price_entry,
    )
    return (app, db,
            Market, Commodity, PriceEntry, WeatherLog, MarketDay,
            Bid, Notification, User, TransportSacco, ArbitrageAlert,
            haversine, smart_base_price, latest_price, latest_weather,
            c_slug, _new_price_entry)


# ── Notification helpers ───────────────────────────────────────────────────────

def _send_sms(phone, message):
    try:
        import africastalking
        africastalking.initialize(os.getenv("AT_USERNAME", "sandbox"), os.getenv("AT_API_KEY", ""))
        africastalking.SMS.send(message, [phone])
    except Exception as e:
        print(f"[SMS] {e}")


def _send_email(to, subject, body):
    host     = os.getenv("SMTP_HOST", "mailhog")
    port     = int(os.getenv("SMTP_PORT", 1025))
    from_addr = os.getenv("FROM_EMAIL", "noreply@agritrade.ke")
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = to
        msg.attach(MIMEText(f"<p>{body}</p>", "html"))
        with smtplib.SMTP(host, port) as s:
            s.sendmail(from_addr, [to], msg.as_string())
    except Exception as e:
        print(f"[EMAIL] {e}")


def _ws_emit(event, data):
    try:
        import redis as redis_lib, json
        r = redis_lib.from_url(REDIS_URL)
        r.publish("flask-socketio", json.dumps({"event": event, "data": data, "namespace": "/"}))
    except Exception as e:
        print(f"[WS] {e}")


# ── Weather options ────────────────────────────────────────────────────────────

CLIMATE_WEATHER = {
    "highland": [
        {"w": "Sunny",        "icon": "☀️",  "tmp": 22, "rain": False, "prem": 0.00},
        {"w": "Partly Cloudy","icon": "⛅",  "tmp": 19, "rain": False, "prem": 0.00},
        {"w": "Light Rain",   "icon": "🌦️", "tmp": 17, "rain": True,  "prem": 0.12},
        {"w": "Cold & Foggy", "icon": "🌫️", "tmp": 14, "rain": False, "prem": 0.05},
    ],
    "coastal": [
        {"w": "Hot & Sunny",  "icon": "🌞",  "tmp": 31, "rain": False, "prem": 0.00},
        {"w": "Heavy Rain",   "icon": "🌧️", "tmp": 26, "rain": True,  "prem": 0.25},
        {"w": "Humid",        "icon": "💧",  "tmp": 29, "rain": False, "prem": 0.05},
        {"w": "Sea Breeze",   "icon": "🌊",  "tmp": 27, "rain": False, "prem": 0.00},
    ],
    "lakeside": [
        {"w": "Cloudy", "icon": "☁️",  "tmp": 24, "rain": False, "prem": 0.03},
        {"w": "Rainy",  "icon": "🌧️", "tmp": 21, "rain": True,  "prem": 0.18},
        {"w": "Sunny",  "icon": "☀️",  "tmp": 27, "rain": False, "prem": 0.00},
        {"w": "Misty",  "icon": "🌫️", "tmp": 19, "rain": False, "prem": 0.08},
    ],
    "rift": [
        {"w": "Sunny & Dry", "icon": "☀️",  "tmp": 26, "rain": False, "prem": 0.00},
        {"w": "Drought",     "icon": "🔥",  "tmp": 34, "rain": False, "prem": 0.08},
        {"w": "Windy",       "icon": "💨",  "tmp": 22, "rain": False, "prem": 0.06},
        {"w": "Clear",       "icon": "🌤️", "tmp": 24, "rain": False, "prem": 0.00},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — Price simulation
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.simulate_market_volatility")
def simulate_market_volatility():
    (app, db, Market, Commodity, PriceEntry, WeatherLog, MarketDay,
     Bid, Notification, User, TransportSacco, ArbitrageAlert,
     haversine, smart_base_price, latest_price, latest_weather,
     c_slug, _new_price_entry) = _imports()

    with app.app_context():
        markets     = Market.query.all()
        commodities = Commodity.query.all()
        pairs = random.sample([(m, c) for m in markets for c in commodities],
                              k=min(6, len(markets) * len(commodities)))
        updates = []
        for market, commodity in pairs:
            old   = latest_price(market.id, commodity.id) or smart_base_price(commodity, market)
            wx    = latest_weather(market.id)
            prem  = wx["prem"] if wx else 0.0
            base  = smart_base_price(commodity, market)
            vol   = commodity.volatility * (1 + prem * 0.4)
            drift = (base - old) * 0.025
            noise = (random.random() - 0.48) * 2 * vol * old
            new   = max(base * 0.55, round(old + noise + drift, 2))
            # Use helper so both price and price_per_unit are written
            db.session.add(_new_price_entry(market.id, commodity.id, new))
            pct = round((new - old) / old * 100, 2) if old else 0
            updates.append({"market_id": market.id, "commodity_id": c_slug(commodity),
                             "price": new, "change": round(new - old, 2), "pct": pct})
        db.session.commit()
        for u in updates:
            _ws_emit("price_update", u)
    return f"Updated {len(updates)} prices"


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — Weather rotation
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.rotate_weather")
def rotate_weather():
    (app, db, Market, Commodity, PriceEntry, WeatherLog, *rest) = _imports()

    with app.app_context():
        markets = Market.query.all()
        targets = random.sample(markets, k=min(3, len(markets)))
        for m in targets:
            wx = random.choice(CLIMATE_WEATHER.get(m.climate or "highland", CLIMATE_WEATHER["highland"]))
            db.session.add(WeatherLog(
                market_id=m.id, condition=wx["w"], icon=wx["icon"],
                temperature=wx["tmp"], is_raining=wx["rain"],
                transport_premium=wx["prem"],
            ))
            _ws_emit("weather_update", {"market_id": m.id, "weather": wx})
        db.session.commit()
    return f"Rotated weather for {len(targets)} markets"


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — Arbitrage detection
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.detect_and_alert_arbitrage")
def detect_and_alert_arbitrage():
    (app, db, Market, Commodity, PriceEntry, WeatherLog, MarketDay,
     Bid, Notification, User, TransportSacco, ArbitrageAlert,
     haversine, smart_base_price, latest_price, latest_weather,
     c_slug, _new_price_entry) = _imports()

    with app.app_context():
        markets     = Market.query.all()
        commodities = Commodity.query.all()
        saccos      = TransportSacco.query.all()
        default_rate = saccos[0].rate_per_km_mt if saccos else 2.0
        sent = 0

        for commodity in commodities:
            pm = {m.id: {"market": m, "price": p}
                  for m in markets if (p := latest_price(m.id, commodity.id))}
            if len(pm) < 2:
                continue
            lo = min(pm.values(), key=lambda x: x["price"])
            hi = max(pm.values(), key=lambda x: x["price"])
            spread = (hi["price"] - lo["price"]) / lo["price"] * 100
            if spread < 15:
                continue
            dist = haversine(lo["market"].lat, lo["market"].lon,
                             hi["market"].lat, hi["market"].lon)
            wx   = latest_weather(lo["market"].id)
            prem = wx["prem"] if wx else 0
            tc   = dist * default_rate * (1 + prem) / 100
            net  = hi["price"] - lo["price"] - tc
            if net <= 0:
                continue

            msg = (f"🚨 FAIDA: {commodity.emoji} {commodity.name} "
                   f"{lo['market'].name} KES {lo['price']:.0f} → "
                   f"{hi['market'].name} KES {hi['price']:.0f} "
                   f"(+{spread:.1f}% · net KES {net:.0f}/kg)")

            _ws_emit("arbitrage_alert", {"message": msg, "spread": round(spread, 1), "net": round(net, 2)})

            for user in User.query.filter(User.tier.in_(["premium", "enterprise"])).all():
                subs = [s.strip() for s in (user.subscribed_commodities or "").split(",") if s.strip()]
                slug = c_slug(commodity)
                if subs and slug not in subs and commodity.name.lower() not in [s.lower() for s in subs]:
                    continue
                db.session.add(Notification(user_id=user.id, type="arb", msg=msg))
                if user.phone and spread > 20:
                    _send_sms(user.phone, msg[:160])
                if user.email:
                    _send_email(user.email, f"AgriTrade Faida: {commodity.name} +{spread:.0f}%", msg)
                sent += 1

        db.session.commit()
    return f"Scanned arbitrage, sent {sent} alerts"


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — Morning blast
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.morning_market_blast")
def morning_market_blast():
    (app, db, Market, Commodity, PriceEntry, WeatherLog, MarketDay,
     Bid, Notification, User, TransportSacco, ArbitrageAlert,
     haversine, smart_base_price, latest_price, latest_weather,
     c_slug, _new_price_entry) = _imports()

    with app.app_context():
        today = datetime.now().date()
        days  = MarketDay.query.filter_by(scheduled_date=today).all()
        if not days:
            return "No market days today"
        summaries = []
        for md in days:
            c = md.commodity
            p = latest_price(md.market_id, c.id) if c else None
            price_str = f"KES {p:.0f}/{c.unit}" if (p and c) else "—"
            summaries.append(f"📅 {md.market.name}: {c.emoji if c else '🌿'} {c.name if c else 'Various'} @ {price_str}")
        msg = "Good morning! Today's AgriTrade markets:\n" + "\n".join(summaries[:5])
        for user in User.query.all():
            db.session.add(Notification(user_id=user.id, type="market", msg=msg[:500]))
            if user.phone:
                _send_sms(user.phone, msg[:160])
        db.session.commit()
        _ws_emit("sms_alert", {"message": msg[:300]})
    return f"Morning blast: {len(days)} market days"


# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 — Expire old bids
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.expire_old_bids")
def expire_old_bids():
    (app, db, Market, Commodity, PriceEntry, WeatherLog, MarketDay,
     Bid, Notification, User, TransportSacco, ArbitrageAlert,
     haversine, smart_base_price, latest_price, latest_weather,
     c_slug, _new_price_entry) = _imports()

    with app.app_context():
        now     = datetime.now(timezone.utc)
        expired = Bid.query.filter(Bid.status == "open", Bid.expires_at < now).all()
        for b in expired:
            b.status = "expired"
        db.session.commit()
    return f"Expired {len(expired)} bids"
