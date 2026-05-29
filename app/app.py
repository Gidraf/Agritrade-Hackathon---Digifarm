import eventlet
eventlet.monkey_patch()

import os, math, random, json
from datetime import datetime, date, timedelta, timezone
from functools import wraps

import requests as req_lib
from flask import Flask, request, jsonify, g
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_socketio import SocketIO
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from sqlalchemy import text

app = Flask(__name__)
CORS(app, origins="*", supports_credentials=True)

DB_URL        = os.getenv("DATABASE_URL", "sqlite:///agri.db")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379/0")
JWT_SECRET    = os.getenv("JWT_SECRET", "dev_secret_change_in_prod")
JWT_EXPIRE    = int(os.getenv("JWT_EXPIRE_HOURS", 72))
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

app.config.update(
    SQLALCHEMY_DATABASE_URI=DB_URL,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    broker_url=REDIS_URL,
    result_backend=REDIS_URL,
)

db        = SQLAlchemy(app)
socketio  = SocketIO(app, message_queue=REDIS_URL, cors_allowed_origins="*", async_mode="eventlet")

# ─── AI TOKEN SYSTEM CONFIG ───────────────────────────────────────────────────

FREE_DAILY_QUOTA = {"basic": 3, "premium": 10, "enterprise": 50}

TOKEN_PACKS = [
    {"id": 1, "name": "Starter",   "tokens": 20,  "price_kes": 500,   "desc": "Best for light use"},
    {"id": 2, "name": "Pro",       "tokens": 50,  "price_kes": 1_000, "desc": "Most popular"},
    {"id": 3, "name": "Power",     "tokens": 100, "price_kes": 1_800, "desc": "Best value — KES 18/query"},
]
DEV_TOKEN_PACKS = [
    {"id": 4, "name": "Dev Starter",   "tokens": 100,  "price_kes": 2_500},
    {"id": 5, "name": "Dev Pro",       "tokens": 500,  "price_kes": 10_000},
    {"id": 6, "name": "Dev Unlimited", "tokens": 1_000,"price_kes": 18_000},
]


# ─── MIGRATION ────────────────────────────────────────────────────────────────

def run_migrations():
    stmts = [
        "ALTER TABLE market ADD COLUMN IF NOT EXISTS climate    VARCHAR(50) DEFAULT 'highland'",
        "ALTER TABLE market ADD COLUMN IF NOT EXISTS created_at TIMESTAMP   DEFAULT now()",
        "UPDATE market SET climate = 'highland' WHERE climate IS NULL",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS hub_lat        FLOAT",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS hub_lon        FLOAT",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS hub_price      FLOAT DEFAULT 50.0",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS max_multiplier FLOAT DEFAULT 2.0",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS emoji          VARCHAR(10) DEFAULT '🌿'",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS base_price     FLOAT DEFAULT 50.0",
        "ALTER TABLE price_entry ADD COLUMN IF NOT EXISTS price FLOAT",
        "ALTER TABLE price_entry ADD COLUMN IF NOT EXISTS grade VARCHAR(5) DEFAULT 'A'",
        "ALTER TABLE price_entry ADD COLUMN IF NOT EXISTS is_agent_verified BOOLEAN DEFAULT FALSE",
        "ALTER TABLE price_entry ALTER COLUMN price_per_unit DROP NOT NULL",
        "UPDATE price_entry SET price = price_per_unit WHERE price IS NULL AND price_per_unit IS NOT NULL",
        "ALTER TABLE market_day ADD COLUMN IF NOT EXISTS primary_commodity_id INTEGER",
        "ALTER TABLE market_day ADD COLUMN IF NOT EXISTS expected_volume_mt   FLOAT",
        "ALTER TABLE market_day ADD COLUMN IF NOT EXISTS logistics_note       VARCHAR(200)",
    ]
    with db.engine.connect() as conn:
        for sql in stmts:
            try:
                conn.execute(text(sql))
            except Exception as e:
                print(f"[migration] skip ({type(e).__name__}): {sql[:70]}…")
        conn.commit()
    print("[migration] complete ✓")


# ─── MODELS ───────────────────────────────────────────────────────────────────

class Market(db.Model):
    id         = db.Column(db.Integer, primary_key=True)
    name       = db.Column(db.String(100), nullable=False)
    region     = db.Column(db.String(100))
    lat        = db.Column(db.Float, nullable=False)
    lon        = db.Column(db.Float, nullable=False)
    climate    = db.Column(db.String(50), default="highland")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Commodity(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(100), nullable=False, unique=True)
    unit           = db.Column(db.String(20),  default="KG")
    emoji          = db.Column(db.String(10),  default="🌿")
    hub_lat        = db.Column(db.Float,        default=0.0)
    hub_lon        = db.Column(db.Float,        default=0.0)
    hub_price      = db.Column(db.Float,        default=50.0)
    base_price     = db.Column(db.Float,        default=50.0)
    volatility     = db.Column(db.Float,        default=0.08)
    max_multiplier = db.Column(db.Float,        default=2.0)


class PriceEntry(db.Model):
    id                = db.Column(db.Integer, primary_key=True)
    market_id         = db.Column(db.Integer, db.ForeignKey("market.id"),    nullable=False)
    commodity_id      = db.Column(db.Integer, db.ForeignKey("commodity.id"), nullable=False)
    price             = db.Column(db.Float)
    price_per_unit    = db.Column(db.Float)
    grade             = db.Column(db.String(5),  default="A")
    weather_tag       = db.Column(db.String(200))
    is_agent_verified = db.Column(db.Boolean,    default=False)
    recorded_at       = db.Column(db.DateTime,   default=lambda: datetime.now(timezone.utc))
    market            = db.relationship("Market",    backref="prices")
    commodity         = db.relationship("Commodity", backref="prices")


class TransportSacco(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(200), nullable=False)
    rate_per_km_mt = db.Column(db.Float, default=2.0)


class TransportRoute(db.Model):
    id                 = db.Column(db.Integer, primary_key=True)
    name               = db.Column(db.String(200), nullable=False)
    origin_market_id   = db.Column(db.Integer, db.ForeignKey("market.id"))
    dest_market_id     = db.Column(db.Integer, db.ForeignKey("market.id"))
    rate_per_km_per_mt = db.Column(db.Float, default=2.0)
    provider           = db.Column(db.String(200))


class MarketDay(db.Model):
    id                   = db.Column(db.Integer, primary_key=True)
    market_id            = db.Column(db.Integer, db.ForeignKey("market.id"), nullable=False)
    scheduled_date       = db.Column(db.Date, nullable=False)
    primary_commodity_id = db.Column(db.Integer, db.ForeignKey("commodity.id"))
    primary_commodity    = db.Column(db.String(100))
    expected_volume_mt   = db.Column(db.Float)
    logistics_note       = db.Column(db.String(200))
    is_major             = db.Column(db.Boolean, default=True)
    market               = db.relationship("Market")
    commodity            = db.relationship("Commodity", foreign_keys=[primary_commodity_id])


class Farmer(db.Model):
    id                     = db.Column(db.Integer, primary_key=True)
    name                   = db.Column(db.String(200))
    phone                  = db.Column(db.String(20))
    email                  = db.Column(db.String(200))
    lat                    = db.Column(db.Float)
    lon                    = db.Column(db.Float)
    alert_radius_km        = db.Column(db.Float, default=50.0)
    subscribed_commodities = db.Column(db.Text)


class ArbitrageAlert(db.Model):
    id          = db.Column(db.Integer, primary_key=True)
    commodity   = db.Column(db.String(100))
    buy_market  = db.Column(db.String(200))
    sell_market = db.Column(db.String(200))
    buy_price   = db.Column(db.Float)
    sell_price  = db.Column(db.Float)
    spread_pct  = db.Column(db.Float)
    net_margin  = db.Column(db.Float)
    created_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    sms_sent    = db.Column(db.Boolean, default=False)
    email_sent  = db.Column(db.Boolean, default=False)


class User(db.Model):
    id                     = db.Column(db.Integer, primary_key=True)
    name                   = db.Column(db.String(200), nullable=False)
    email                  = db.Column(db.String(200), unique=True, nullable=False)
    password_hash          = db.Column(db.String(256), nullable=False)
    role                   = db.Column(db.String(50),  default="Farmer")
    tier                   = db.Column(db.String(50),  default="basic")
    phone                  = db.Column(db.String(30))
    location               = db.Column(db.String(200))
    subscribed_commodities = db.Column(db.Text, default="")
    alert_radius_km        = db.Column(db.Float, default=100.0)
    created_at             = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def set_password(self, pw):   self.password_hash = generate_password_hash(pw)
    def check_password(self, pw): return check_password_hash(self.password_hash, pw)

    def make_token(self):
        return jwt.encode(
            {"sub": str(self.id),
             "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE)},
            JWT_SECRET, algorithm="HS256")

    def to_dict(self):
        usage = AiUsage.query.filter_by(user_id=self.id).first()
        return {
            "id": self.id, "name": self.name, "email": self.email,
            "role": self.role, "tier": self.tier,
            "phone": self.phone, "location": self.location,
            "subscribed_commodities": [
                s.strip() for s in (self.subscribed_commodities or "").split(",") if s.strip()
            ],
            "alert_radius_km": self.alert_radius_km,
            "ai_tokens": usage.tokens_balance if usage else 0,
        }


class ApiKey(db.Model):
    id          = db.Column(db.Integer, primary_key=True)
    user_id     = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    name        = db.Column(db.String(200), nullable=False)
    key_hash    = db.Column(db.String(256), nullable=False)
    key_preview = db.Column(db.String(30),  nullable=False)
    call_count  = db.Column(db.Integer, default=0)
    ai_call_count = db.Column(db.Integer, default=0)
    created_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    revoked     = db.Column(db.Boolean, default=False)
    user        = db.relationship("User", backref="api_keys")


class Bid(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    buyer_id       = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    commodity_id   = db.Column(db.Integer, db.ForeignKey("commodity.id"), nullable=False)
    bid_price      = db.Column(db.Float, nullable=False)
    qty            = db.Column(db.Float, nullable=False)
    unit           = db.Column(db.String(20), default="KG")
    expires_at     = db.Column(db.DateTime)
    status         = db.Column(db.String(30), default="open")
    notes          = db.Column(db.Text)
    accepted_by_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    created_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    buyer          = db.relationship("User", foreign_keys=[buyer_id])
    accepted_by    = db.relationship("User", foreign_keys=[accepted_by_id])


class Notification(db.Model):
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    type       = db.Column(db.String(50))
    msg        = db.Column(db.Text)
    read       = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class WeatherLog(db.Model):
    id                = db.Column(db.Integer, primary_key=True)
    market_id         = db.Column(db.Integer, db.ForeignKey("market.id"), nullable=False)
    condition         = db.Column(db.String(100))
    icon              = db.Column(db.String(10))
    temperature       = db.Column(db.Float)
    is_raining        = db.Column(db.Boolean, default=False)
    transport_premium = db.Column(db.Float, default=0.0)
    recorded_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    market            = db.relationship("Market")


# ─── AI MODELS ────────────────────────────────────────────────────────────────

class AiUsage(db.Model):
    """Tracks per-user AI token balance and daily free quota."""
    id               = db.Column(db.Integer, primary_key=True)
    user_id          = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, unique=True)
    tokens_balance   = db.Column(db.Integer, default=0)
    daily_free_used  = db.Column(db.Integer, default=0)
    daily_reset_date = db.Column(db.Date,    default=lambda: date.today())
    total_ai_calls   = db.Column(db.Integer, default=0)
    total_spent_kes  = db.Column(db.Float,   default=0.0)
    user             = db.relationship("User", backref=db.backref("ai_usage", uselist=False))


class AiNegotiation(db.Model):
    """Stores Ajiriwa negotiation state for a bid."""
    id               = db.Column(db.Integer, primary_key=True)
    bid_id           = db.Column(db.Integer, db.ForeignKey("bid.id"), unique=True, nullable=False)
    status           = db.Column(db.String(50), default="analyzing")
    # status: analyzing | suggested | countered | buyer_countered | resolved | rejected
    events_json      = db.Column(db.Text, default="[]")
    market_avg_price = db.Column(db.Float)
    ai_fair_price    = db.Column(db.Float)
    ai_counter_price = db.Column(db.Float)
    ai_reasoning     = db.Column(db.Text)
    discount_pct     = db.Column(db.Float)
    confidence       = db.Column(db.Float, default=0.85)
    resolved_price   = db.Column(db.Float)
    created_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    bid              = db.relationship("Bid", backref=db.backref("negotiation", uselist=False))


# ─── AUTH HELPERS ─────────────────────────────────────────────────────────────

def decode_token(token):
    try:
        return int(jwt.decode(token, JWT_SECRET, algorithms=["HS256"])["sub"])
    except Exception:
        return None

def _token_from_request():
    h = request.headers.get("Authorization", "")
    return h[7:].strip() if h.lower().startswith("bearer ") else ""

def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        uid = decode_token(_token_from_request())
        if not uid:
            return jsonify({"detail": "Invalid or missing token"}), 401
        g.user = User.query.get(uid)
        if not g.user:
            return jsonify({"detail": "User not found"}), 401
        return f(*args, **kwargs)
    return wrapper

def require_tier(*tiers):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            uid = decode_token(_token_from_request())
            if not uid:
                return jsonify({"detail": "Missing token"}), 401
            g.user = User.query.get(uid)
            if not g.user:
                return jsonify({"detail": "User not found"}), 401
            if g.user.tier not in tiers:
                return jsonify({"detail": f"Requires {' or '.join(tiers)} tier"}), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator

def require_role(*roles):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            uid = decode_token(_token_from_request())
            if not uid:
                return jsonify({"detail": "Missing token"}), 401
            g.user = User.query.get(uid)
            if not g.user:
                return jsonify({"detail": "User not found"}), 401
            if g.user.role not in roles:
                return jsonify({"detail": f"Requires role: {', '.join(roles)}"}), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator

def require_api_key(f):
    """Authenticate developer API calls via X-Api-Key header."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        raw_key = request.headers.get("X-Api-Key", "")
        if not raw_key or not raw_key.startswith("ak_live_"):
            return jsonify({"error": "Invalid API key format. Use X-Api-Key: ak_live_..."}), 401
        raw = raw_key.replace("ak_live_", "")
        ak = None
        for k in ApiKey.query.filter_by(revoked=False).all():
            if check_password_hash(k.key_hash, raw):
                ak = k
                break
        if not ak:
            return jsonify({"error": "API key not found or revoked"}), 401
        ak.call_count += 1
        db.session.commit()
        g.api_key = ak
        g.user = User.query.get(ak.user_id)
        return f(*args, **kwargs)
    return wrapper


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    d = lambda x: x * math.pi / 180
    a = (math.sin(d(lat2 - lat1) / 2) ** 2
         + math.cos(d(lat1)) * math.cos(d(lat2)) * math.sin(d(lon2 - lon1) / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def smart_base_price(commodity, market):
    """
    Price = hub_price × distance_factor × climate_premium × hub_discount
    distance_factor = 1 + dist_km × 0.035 / 100   (3.5% per 100 km from hub)
    coastal_premium = 1.18                          (18% for coastal markets)
    hub_discount    = 0.88 if dist < 30 km          (wholesale near-hub discount)
    """
    hl   = commodity.hub_lat  or 0.0
    hln  = commodity.hub_lon  or 0.0
    hp   = commodity.hub_price or commodity.base_price or 50.0
    dist          = haversine(hl, hln, market.lat, market.lon)
    dist_factor   = 1 + (dist * 0.035 / 100)
    coastal_prem  = 1.18 if (market.climate or "highland") == "coastal" else 1.0
    hub_disc      = 0.88 if dist < 30 else 1.0
    return min(hp * dist_factor * coastal_prem * hub_disc,
               hp * (commodity.max_multiplier or 2.0))


def transport_cost_per_kg(dist_km: float, rate_per_km_mt: float, weather_prem: float = 0.0) -> float:
    """
    Canonical transport cost formula used throughout the app.
    Rate is KES per km per MT (1 MT = 1000 kg).
    tc = dist × rate × (1 + weather_premium) / 100
    At rate=2.0, 100 km → KES 2.00/kg
    At rate=2.0, 480 km → KES 9.60/kg  (NBI-MSA realistic range)
    """
    return dist_km * rate_per_km_mt * (1 + weather_prem) / 100


def _pval(entry):
    if entry is None: return None
    return entry.price if entry.price is not None else entry.price_per_unit

def latest_price(market_id, commodity_id):
    e = (PriceEntry.query.filter_by(market_id=market_id, commodity_id=commodity_id)
         .order_by(PriceEntry.recorded_at.desc()).first())
    return _pval(e)

def latest_weather(market_id):
    log = (WeatherLog.query.filter_by(market_id=market_id)
           .order_by(WeatherLog.recorded_at.desc()).first())
    if not log: return None
    return {"w": log.condition, "icon": log.icon, "tmp": log.temperature,
            "rain": log.is_raining, "prem": log.transport_premium}

def c_slug(c):
    return c.name.lower().replace(" ", "_")

def _lookup_commodity(slug_or_id):
    if str(slug_or_id).isdigit():
        return Commodity.query.get(int(slug_or_id))
    return Commodity.query.filter(
        db.func.lower(Commodity.name) == str(slug_or_id).replace("_", " ").lower()
    ).first()

def _new_price_entry(market_id, commodity_id, price, grade="A", verified=False, recorded_at=None):
    kwargs = dict(market_id=market_id, commodity_id=commodity_id,
                  price=price, price_per_unit=price, grade=grade, is_agent_verified=verified)
    if recorded_at: kwargs["recorded_at"] = recorded_at
    return PriceEntry(**kwargs)

def bid_to_dict(b):
    com = Commodity.query.get(b.commodity_id)
    exp = ""
    if b.expires_at:
        ea = b.expires_at.replace(tzinfo=timezone.utc) if b.expires_at.tzinfo is None else b.expires_at
        h  = max(0, int((ea - datetime.now(timezone.utc)).total_seconds() / 3600))
        exp = f"{h}h"
    neg = b.negotiation
    return {
        "id": b.id, "buyer": b.buyer.name, "buyer_id": b.buyer_id,
        "commodity": c_slug(com) if com else str(b.commodity_id),
        "unit": b.unit, "bid_price": b.bid_price, "qty": b.qty,
        "expires": exp, "status": b.status, "notes": b.notes,
        "created_at": b.created_at.isoformat(),
        "accepted_by": b.accepted_by.name if b.accepted_by else None,
        "negotiation": {
            "status": neg.status,
            "ai_fair_price": neg.ai_fair_price,
            "ai_counter_price": neg.ai_counter_price,
            "discount_pct": neg.discount_pct,
            "reasoning": neg.ai_reasoning,
            "confidence": neg.confidence,
            "events": json.loads(neg.events_json or "[]"),
        } if neg else None,
    }

def _rel_time(dt):
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    s = int((datetime.now(timezone.utc) - dt).total_seconds())
    if s < 60:    return "just now"
    if s < 3600:  return f"{s // 60}m ago"
    if s < 86400: return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"

def _notify_user(user_id, msg, notif_type="system"):
    n = Notification(user_id=user_id, type=notif_type, msg=msg)
    db.session.add(n)
    db.session.commit()
    socketio.emit("notification", {"msg": msg, "type": notif_type},
                  room=f"user_{user_id}", namespace="/")

def _notify_subscribed_users(msg, commodity_id, roles=None):
    q = User.query
    if roles: q = q.filter(User.role.in_(roles))
    com  = Commodity.query.get(commodity_id)
    slug = c_slug(com) if com else ""
    for user in q.all():
        subs = [s.strip() for s in (user.subscribed_commodities or "").split(",")]
        if subs and slug and slug not in subs: continue
        _notify_user(user.id, msg, notif_type="bid")


# ─── AI HELPERS ───────────────────────────────────────────────────────────────

def _get_or_create_usage(user_id):
    usage = AiUsage.query.filter_by(user_id=user_id).first()
    if not usage:
        usage = AiUsage(user_id=user_id)
        db.session.add(usage)
        db.session.commit()
    if usage.daily_reset_date != date.today():
        usage.daily_free_used  = 0
        usage.daily_reset_date = date.today()
        db.session.commit()
    return usage


def _check_and_consume_token(user):
    """
    Returns (ok: bool, quota_info: dict).
    Priority: free daily quota → purchased token balance → error.
    """
    usage       = _get_or_create_usage(user.id)
    free_limit  = FREE_DAILY_QUOTA.get(user.tier, 3)

    if usage.daily_free_used < free_limit:
        usage.daily_free_used += 1
        usage.total_ai_calls  += 1
        db.session.commit()
        return True, {
            "source": "free_daily",
            "free_remaining": free_limit - usage.daily_free_used,
            "tokens_balance": usage.tokens_balance,
            "free_limit": free_limit,
        }

    if usage.tokens_balance > 0:
        usage.tokens_balance -= 1
        usage.total_ai_calls += 1
        db.session.commit()
        return True, {
            "source": "token",
            "free_remaining": 0,
            "tokens_balance": usage.tokens_balance,
            "free_limit": free_limit,
        }

    return False, {
        "source": "exhausted",
        "free_remaining": 0,
        "tokens_balance": 0,
        "free_limit": free_limit,
        "packs": TOKEN_PACKS,
    }


def call_claude(system_prompt: str, user_message: str, max_tokens: int = 900) -> str | None:
    """
    Call Anthropic claude-sonnet-4-20250514. Returns text or None if unavailable.
    None signals caller to use generated demo data instead.
    """
    if not ANTHROPIC_KEY:
        return None
    try:
        resp = req_lib.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": max_tokens,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_message}],
            },
            timeout=45,
        )
        data = resp.json()
        if resp.status_code != 200:
            print(f"[claude] HTTP {resp.status_code}: {data}")
            return None
        return data.get("content", [{}])[0].get("text", "")
    except Exception as e:
        print(f"[claude] error: {e}")
        return None


def _faida_demo(c, o, t, buy_price, sell_price, spread, net_per_kg, dist, tc, wx_o):
    """Deterministic demo data when no Anthropic key is configured."""
    ok      = net_per_kg > 2
    score   = min(9.5, max(2.0, spread / 5))
    rec     = "EXECUTE" if score >= 7 else ("REVIEW" if score >= 5 else "WAIT")
    conf    = int(min(92, max(45, score * 10)))
    vol     = max(500, min(10_000, int(50_000 / max(1, buy_price))))
    return {
        "route_score": round(score, 1),
        "recommendation": rec,
        "confidence_pct": conf,
        "summary": (
            f"Route scores {score:.1f}/10 with {spread:.1f}% spread. "
            f"{'Weather conditions add transport premium — factor into timing.' if wx_o.get('rain') else 'Conditions are favourable for execution.'}"
        ),
        "execution_steps": [
            {"step": 1, "action": f"Confirm {c.name} quality with Grade A supplier at {o.name}",
             "timing": "Today 6–8 AM", "kpi": f"Buy ≤ KES {buy_price * 1.02:.0f}/{c.unit}"},
            {"step": 2, "action": f"Book SACCO transport ({dist:.0f} km route)",
             "timing": "Same day", "kpi": f"Rate ≤ KES {tc / dist * 100:.1f}/km/MT"},
            {"step": 3, "action": f"Coordinate delivery window at {t.name}",
             "timing": "Next market day", "kpi": f"Sell ≥ KES {sell_price * 0.97:.0f}/{c.unit}"},
        ],
        "risk_factors": [
            {"risk": "Price may narrow before execution", "severity": "MED",
             "mitigation": "Execute within 48 hours or re-verify spread"},
            *([ {"risk": "Rain adds transport costs", "severity": "HIGH",
                 "mitigation": f"Budget +{round(wx_o.get('prem', 0)*100)}% on logistics"}]
               if wx_o.get("rain") else []),
            {"risk": "Post-harvest loss reduces net", "severity": "LOW",
             "mitigation": "Use moisture-controlled bags; target <5% loss"},
        ],
        "optimal_volume_kg": vol,
        "optimal_timing": "Early morning departure to beat midday traffic",
        "projected_margin_pct": round((net_per_kg / sell_price) * 100, 1) if sell_price else 0,
        "calculator_config": {"kg": vol, "loss_pct": 5, "pkg_rate": 2, "notes": f"{rec} — {c.name} spread at {spread:.1f}%"},
        "market_intel": (
            f"{c.name} prices at {t.name} typically peak during school term starts and harvest gaps. "
            f"Current {spread:.1f}% spread is {'above' if spread > 20 else 'near'} the seasonal average."
        ),
    }


def _ajiriwa_demo(bid, com, market_avg, fair_price, counter_price, discount_pct):
    position = "AT_MARKET" if abs(discount_pct) < 3 else ("ABOVE_MARKET" if discount_pct < 0 else "BELOW_MARKET")
    return {
        "fair_price": round(fair_price, 2),
        "counter_price": round(counter_price, 2),
        "market_avg": round(market_avg, 2),
        "discount_pct": round(discount_pct, 1),
        "market_position": position,
        "confidence": 0.83,
        "reasoning": (
            f"Current spot price at hub markets is KES {market_avg:.2f}/{com.unit}. "
            f"The bid of KES {bid.bid_price:.2f} is {abs(discount_pct):.1f}% "
            f"{'below' if discount_pct > 0 else 'above'} fair value. "
            f"A counter of KES {counter_price:.2f} reflects transport cost recovery and "
            f"Grade A quality premium while remaining competitive for the buyer."
        ),
        "negotiation_advice": (
            f"Your position is strong — {bid.qty:,.0f} {com.unit} is a significant volume. "
            f"Counter at KES {counter_price:.2f} and hold for 24 hours. "
            f"If buyer does not respond, the open market price at hub is KES {market_avg:.2f}."
        ),
        "events": [
            {"time": "Just now", "type": "scan",      "text": f"Scanned {Market.query.count()} live market prices for {com.name}"},
            {"time": "Just now", "type": "analysis",  "text": f"Fair value calculated: KES {fair_price:.2f}/{com.unit} (hub avg + transport + quality prem)"},
            {"time": "Just now", "type": "verdict",   "text": f"Bid is {abs(discount_pct):.1f}% {'below' if discount_pct > 0 else 'above'} fair value — {position.replace('_', ' ').title()}", "highlight": True},
            {"time": "Just now", "type": "counter",   "text": f"Counter-offer generated: KES {counter_price:.2f}/{com.unit}", "highlight": True},
        ],
    }


# ─── AUTH ─────────────────────────────────────────────────────────────────────

@app.route("/api/auth/register", methods=["POST"])
def register():
    d = request.json or {}
    if not all(k in d for k in ("name", "email", "password", "role")):
        return jsonify({"detail": "Missing required fields"}), 400
    if User.query.filter_by(email=d["email"]).first():
        return jsonify({"detail": "Email already registered"}), 409
    u = User(name=d["name"], email=d["email"],
             role=d.get("role", "Farmer"), tier=d.get("tier", "basic"),
             phone=d.get("phone"), location=d.get("location"),
             subscribed_commodities=",".join(d.get("subscribed_commodities") or []))
    u.set_password(d["password"])
    db.session.add(u)
    db.session.commit()
    # seed free AI quota
    _get_or_create_usage(u.id)
    return jsonify({"access_token": u.make_token(), "token_type": "bearer", "user": u.to_dict()}), 201


@app.route("/api/auth/login", methods=["POST"])
def login():
    d = request.json or {}
    u = User.query.filter_by(email=d.get("email", "")).first()
    if not u or not u.check_password(d.get("password", "")):
        return jsonify({"detail": "Invalid credentials"}), 401
    _get_or_create_usage(u.id)
    return jsonify({"access_token": u.make_token(), "token_type": "bearer", "user": u.to_dict()})


@app.route("/api/auth/me")
@require_auth
def me():
    return jsonify(g.user.to_dict())


@app.route("/api/auth/debug-token")
def debug_token():
    raw = _token_from_request()
    if not raw: return jsonify({"error": "Send Authorization: Bearer <token>"}), 400
    try:
        payload = jwt.decode(raw, JWT_SECRET, algorithms=["HS256"])
        user    = User.query.get(payload.get("sub"))
        return jsonify({"valid": True, "sub": payload.get("sub"),
                        "user_found": user is not None,
                        "user_email": user.email if user else None})
    except jwt.ExpiredSignatureError:
        return jsonify({"valid": False, "reason": "Expired"})
    except Exception as e:
        return jsonify({"valid": False, "reason": str(e)})


# ─── MARKETS & COMMODITIES ────────────────────────────────────────────────────

@app.route("/api/markets")
def get_markets():
    return jsonify([{"id": m.id, "name": m.name, "region": m.region,
                     "lat": m.lat, "lon": m.lon,
                     "climate": m.climate or "highland"} for m in Market.query.all()])

@app.route("/api/commodities")
def get_commodities():
    return jsonify([{
        "id": c_slug(c), "db_id": c.id,
        "name": c.name, "unit": c.unit, "emoji": c.emoji,
        "hub_lat": c.hub_lat or 0, "hub_lon": c.hub_lon or 0,
        "hub_price": c.hub_price or c.base_price or 50,
        "volatility": c.volatility, "max_multiplier": c.max_multiplier,
    } for c in Commodity.query.all()])


# ─── LIVE PRICES ──────────────────────────────────────────────────────────────

@app.route("/api/prices/live")
def get_live_prices():
    markets, commodities = Market.query.all(), Commodity.query.all()
    result = {}
    for m in markets:
        result[m.id] = {}
        for c in commodities:
            p = latest_price(m.id, c.id)
            if p is None: p = smart_base_price(c, m)
            prev_e = (PriceEntry.query.filter_by(market_id=m.id, commodity_id=c.id)
                      .order_by(PriceEntry.recorded_at.desc()).offset(1).first())
            prev   = _pval(prev_e) or p
            pct    = ((p - prev) / prev * 100) if prev else 0
            result[m.id][c_slug(c)] = {
                "price": round(p, 2), "prev": round(prev, 2),
                "change": round(p - prev, 2), "pct": round(pct, 2),
            }
    return jsonify(result)


@app.route("/api/prices/history")
def get_price_history():
    c      = _lookup_commodity(request.args.get("commodity", ""))
    mkt_id = request.args.get("market", type=int)
    days   = int(request.args.get("days", 60))
    if not c: return jsonify([])
    since  = datetime.now(timezone.utc) - timedelta(days=days)
    entries = (PriceEntry.query.filter_by(commodity_id=c.id, market_id=mkt_id)
               .filter(PriceEntry.recorded_at >= since)
               .order_by(PriceEntry.recorded_at.asc()).all())
    return jsonify([{
        "date":  e.recorded_at.strftime("%b %-d"),
        "price": round(_pval(e) or 0, 2),
        "high":  round((_pval(e) or 0) * 1.02, 2),
        "low":   round((_pval(e) or 0) * 0.98, 2),
    } for e in entries])


# ─── WEATHER ──────────────────────────────────────────────────────────────────

@app.route("/api/weather/live")
def get_live_weather():
    return jsonify({m.id: wx for m in Market.query.all()
                    if (wx := latest_weather(m.id)) is not None})


# ─── ARBITRAGE ────────────────────────────────────────────────────────────────

@app.route("/api/arbitrage/opportunities")
@require_tier("premium", "enterprise")
def get_arbitrage_opps():
    markets, commodities = Market.query.all(), Commodity.query.all()
    saccos       = TransportSacco.query.all()
    default_rate = saccos[0].rate_per_km_mt if saccos else 2.0
    result = []
    for c in commodities:
        pm = {m.id: {"market": m, "price": p}
              for m in markets if (p := latest_price(m.id, c.id))}
        if len(pm) < 2: continue
        lo     = min(pm.values(), key=lambda x: x["price"])
        hi     = max(pm.values(), key=lambda x: x["price"])
        spread = (hi["price"] - lo["price"]) / lo["price"] * 100
        if spread < 8: continue
        dist   = haversine(lo["market"].lat, lo["market"].lon,
                           hi["market"].lat, hi["market"].lon)
        wx_o   = latest_weather(lo["market"].id) or {"prem": 0, "rain": False, "icon": "☀️", "w": "Clear", "tmp": 24}
        wx_d   = latest_weather(hi["market"].id) or {"prem": 0, "rain": False, "icon": "☀️", "w": "Clear", "tmp": 24}
        tc     = transport_cost_per_kg(dist, default_rate, wx_o.get("prem", 0))
        result.append({
            "com":  c.name, "comId": c_slug(c), "emoji": c.emoji, "unit": c.unit,
            "buyMkt":  {"id": lo["market"].id, "name": lo["market"].name,
                        "region": lo["market"].region, "lat": lo["market"].lat,
                        "lon": lo["market"].lon, "climate": lo["market"].climate or "highland"},
            "buyP":    round(lo["price"], 2),
            "sellMkt": {"id": hi["market"].id, "name": hi["market"].name,
                        "region": hi["market"].region, "lat": hi["market"].lat,
                        "lon": hi["market"].lon, "climate": hi["market"].climate or "highland"},
            "sellP":   round(hi["price"], 2),
            "spread":  round(spread, 1), "dist": round(dist, 1),
            "tc":      round(tc, 2),
            "net":     round(hi["price"] - lo["price"] - tc, 2),
            "wxOrigin": wx_o, "wxDest": wx_d,
            "action":  "Execute" if spread > 22 else "Review",
        })
    return jsonify(sorted(result, key=lambda x: x["spread"], reverse=True)[:8])


@app.route("/api/arbitrage/calculate", methods=["POST"])
@require_tier("premium", "enterprise")
def calc_arbitrage():
    d  = request.json or {}
    c  = _lookup_commodity(d.get("commodity_id", ""))
    o  = Market.query.get(d.get("origin_market_id"))
    t  = Market.query.get(d.get("target_market_id"))
    if not c or not o or not t:
        return jsonify({"detail": "Invalid inputs"}), 400
    lpu    = latest_price(o.id, c.id) or smart_base_price(c, o)
    dpu    = latest_price(t.id, c.id) or smart_base_price(c, t)
    vol_mt = float(d.get("volume_mt", 1))
    vol_kg = vol_mt * 1000
    dist   = haversine(o.lat, o.lon, t.lat, t.lon)
    wx_o   = latest_weather(o.id) or {"prem": 0}
    saccos = TransportSacco.query.order_by(TransportSacco.rate_per_km_mt).all()
    routes = []
    for i, s in enumerate(saccos[:4]):
        # slight distance variance per route (ring road / bypass)
        da    = round(dist * (1 + i * 0.05), 1)
        tc_kg = transport_cost_per_kg(da, s.rate_per_km_mt, wx_o.get("prem", 0))
        routes.append({
            "rank": i + 1, "name": s.name, "dist_km": da,
            "est_time": f"{int(da/75)}h {random.randint(10, 55)}m",
            "rate": f"KES {s.rate_per_km_mt:.2f}/km/MT",
            "tc_per_kg": round(tc_kg, 2),
            "status": "recommended" if i == 0 else ("traffic" if i == 2 else ""),
        })
    br     = saccos[0].rate_per_km_mt if saccos else 2.0
    tc     = transport_cost_per_kg(dist, br, wx_o.get("prem", 0))
    net_kg = dpu - lpu - tc
    spread = (dpu - lpu) / lpu * 100 if lpu else 0
    return jsonify({
        "spread":  round(spread, 1),
        "net":     round(net_kg * vol_kg, 0),
        "lpu":     round(lpu, 2),
        "dpu":     round(dpu, 2),
        "tc_kg":   round(tc, 2),
        "dist":    round(dist, 1),
        "vol_kg":  vol_kg,
        "routes":  routes,
        "ok":      net_kg * vol_kg > 0,
    })


# ─── PREDICTIONS ──────────────────────────────────────────────────────────────

@app.route("/api/predict/forecast")
@require_tier("premium", "enterprise")
def predict_forecast():
    c      = _lookup_commodity(request.args.get("commodity", ""))
    mkt_id = request.args.get("market", type=int)
    if not c: return jsonify([])
    since  = datetime.now(timezone.utc) - timedelta(days=60)
    entries = (PriceEntry.query.filter_by(commodity_id=c.id, market_id=mkt_id)
               .filter(PriceEntry.recorded_at >= since)
               .order_by(PriceEntry.recorded_at.asc()).all())
    prices = [_pval(e) for e in entries if _pval(e)]
    if len(prices) < 5: return jsonify([])
    n   = len(prices)
    mx  = (n - 1) / 2
    my  = sum(prices) / n
    num = sum((i - mx) * (p - my) for i, p in enumerate(prices))
    den = sum((i - mx) ** 2 for i in range(n))
    sl  = num / den if den else 0
    ic  = my - sl * mx
    std = math.sqrt(sum((p - (sl * i + ic)) ** 2 for i, p in enumerate(prices)) / n)
    return jsonify([{
        "date":      (datetime.now(timezone.utc) + timedelta(days=i + 1)).strftime("%b %-d"),
        "predicted": round(max(0, sl * (n + i) + ic), 2),
        "upper":     round(max(0, sl * (n + i) + ic) + 1.6 * std, 2),
        "lower":     round(max(0, max(0, sl * (n + i) + ic) - 1.6 * std), 2),
    } for i in range(14)])


# ─── CALENDAR ─────────────────────────────────────────────────────────────────

@app.route("/api/calendar")
def get_calendar():
    mo, yr = (request.args.get("month", type=int, default=datetime.now().month),
              request.args.get("year",  type=int, default=datetime.now().year))
    start = date(yr, mo, 1)
    end   = date(yr, 12, 31) if mo == 12 else date(yr, mo + 1, 1) - timedelta(days=1)
    days  = MarketDay.query.filter(MarketDay.scheduled_date.between(start, end)).all()
    result = []
    for md in days:
        c  = md.commodity
        bp = smart_base_price(c, md.market) if c else 0
        result.append({
            "day": md.scheduled_date.day, "mkt": md.market.name, "mktId": md.market_id,
            "commodity": c.name if c else (md.primary_commodity or "Various"),
            "emoji": c.emoji if c else "🌿", "unit": c.unit if c else "KG",
            "color": f"hsl({hash(md.market.name) % 360},60%,55%)",
            "price": f"KES {int(bp*0.9)}-{int(bp*1.15)}/{c.unit if c else 'KG'}",
            "volume": f"{md.expected_volume_mt or random.randint(5, 50)} MT",
            "logistics": md.logistics_note or "Normal demand",
        })
    return jsonify(result)


# ─── BIDS ─────────────────────────────────────────────────────────────────────

@app.route("/api/bids")
def get_bids():
    return jsonify([bid_to_dict(b) for b in Bid.query.order_by(Bid.created_at.desc()).all()])

@app.route("/api/bids", methods=["POST"])
@require_role("Buyer")
def place_bid():
    d = request.json or {}
    c = _lookup_commodity(d.get("commodity", ""))
    if not c:
        return jsonify({"detail": f"Unknown commodity: {d.get('commodity')}"}), 400
    hours = int(str(d.get("expires", "2h")).replace("h", "") or 2)
    bid   = Bid(buyer_id=g.user.id, commodity_id=c.id,
                bid_price=float(d["bid_price"]), qty=float(d["qty"]),
                unit=d.get("unit", c.unit),
                expires_at=datetime.now(timezone.utc) + timedelta(hours=hours),
                notes=d.get("notes"), status="open")
    db.session.add(bid)
    db.session.commit()
    data = bid_to_dict(bid)
    socketio.emit("new_bid", data, namespace="/")
    _notify_subscribed_users(
        f"💰 {g.user.name} bid: {c.name} KES {d['bid_price']}/{c.unit} · {d['qty']} {c.unit}",
        commodity_id=c.id, roles=["Farmer", "Broker"])
    return jsonify(data), 201

@app.route("/api/bids/<int:bid_id>/accept", methods=["POST"])
@require_role("Farmer", "Broker")
def accept_bid(bid_id):
    bid = Bid.query.get_or_404(bid_id)
    if bid.status != "open":
        return jsonify({"detail": "Bid is not open"}), 409
    bid.status         = "accepted"
    bid.accepted_by_id = g.user.id
    db.session.commit()
    data = bid_to_dict(bid)
    socketio.emit("bid_accepted", data, namespace="/")
    c = Commodity.query.get(bid.commodity_id)
    _notify_user(bid.buyer_id,
                 f"✅ {g.user.name} accepted your bid for {c.name if c else ''} @ KES {bid.bid_price}/{bid.unit}")
    return jsonify(data)


# ─── NOTIFICATIONS ────────────────────────────────────────────────────────────

@app.route("/api/notifications")
@require_auth
def get_notifications():
    notifs = (Notification.query.filter_by(user_id=g.user.id)
              .order_by(Notification.created_at.desc()).limit(30).all())
    return jsonify([{"id": n.id, "type": n.type, "msg": n.msg,
                     "read": n.read, "time": _rel_time(n.created_at)} for n in notifs])

@app.route("/api/notifications/<int:nid>/read", methods=["POST"])
@require_auth
def mark_read(nid):
    n = Notification.query.filter_by(id=nid, user_id=g.user.id).first()
    if n: n.read = True; db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/notifications/read-all", methods=["POST"])
@require_auth
def mark_all_read():
    Notification.query.filter_by(user_id=g.user.id).update({"read": True})
    db.session.commit()
    return jsonify({"ok": True})


# ─── AGENT ────────────────────────────────────────────────────────────────────

@app.route("/api/agent/prices", methods=["POST"])
def agent_submit_price():
    d = request.json or {}
    c = _lookup_commodity(d.get("commodity_id", ""))
    m = Market.query.get(d.get("market_id"))
    if not c or not m:
        return jsonify({"detail": "Invalid market or commodity"}), 400
    expected = smart_base_price(c, m)
    entered  = float(d.get("price", 0))
    if abs(entered - expected) / expected > 0.6:
        return jsonify({"detail": f"KES {entered:.0f} is unrealistic for {m.name} (expected ~KES {expected:.0f})"}), 422
    e = _new_price_entry(m.id, c.id, round(entered, 2), grade=d.get("grade", "A"), verified=True)
    db.session.add(e)
    db.session.commit()
    socketio.emit("price_update", {"market_id": m.id, "commodity_id": c_slug(c),
                                   "price": e.price, "pct": 0, "change": 0}, namespace="/")
    return jsonify({"id": e.id}), 201

@app.route("/api/agent/logistics", methods=["POST"])
@require_role("Agent")
def agent_update_logistics():
    d = request.json or {}
    s = TransportSacco.query.filter_by(name=d.get("sacco_name")).first()
    if not s: s = TransportSacco(name=d["sacco_name"]); db.session.add(s)
    s.rate_per_km_mt = float(d.get("rate_per_km_mt", s.rate_per_km_mt))
    db.session.commit()
    return jsonify({"id": s.id}), 201


# ─── SUBSCRIPTIONS ────────────────────────────────────────────────────────────

@app.route("/api/user/subscriptions", methods=["PUT"])
@require_auth
def update_subscriptions():
    d = request.json or {}
    g.user.subscribed_commodities = ",".join(d.get("commodities") or [])
    g.user.alert_radius_km        = float(d.get("radius_km", g.user.alert_radius_km))
    db.session.commit()
    return jsonify({"ok": True})


# ─── DEVELOPER PORTAL ─────────────────────────────────────────────────────────

@app.route("/api/dev/keys")
@require_tier("enterprise")
def get_api_keys():
    keys = ApiKey.query.filter_by(user_id=g.user.id, revoked=False).all()
    return jsonify([{"id": k.id, "name": k.name, "key_preview": k.key_preview,
                     "call_count": k.call_count, "ai_call_count": k.ai_call_count,
                     "created_at": k.created_at.isoformat()} for k in keys])

@app.route("/api/dev/keys", methods=["POST"])
@require_tier("enterprise")
def create_api_key():
    import secrets
    raw = secrets.token_urlsafe(32)
    ak  = ApiKey(user_id=g.user.id, name=request.json.get("name", "Unnamed"),
                 key_hash=generate_password_hash(raw), key_preview=raw[:12] + "•" * 12)
    db.session.add(ak)
    db.session.commit()
    return jsonify({"id": ak.id, "name": ak.name, "key_preview": ak.key_preview,
                    "full_key": f"ak_live_{raw}", "call_count": 0, "ai_call_count": 0,
                    "created_at": ak.created_at.isoformat()}), 201

@app.route("/api/dev/keys/<int:key_id>", methods=["DELETE"])
@require_tier("enterprise")
def revoke_api_key(key_id):
    k = ApiKey.query.filter_by(id=key_id, user_id=g.user.id).first_or_404()
    k.revoked = True
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/dev/usage")
@require_tier("enterprise")
def get_dev_usage():
    calls    = sum(k.call_count for k in ApiKey.query.filter_by(user_id=g.user.id).all())
    ai_calls = sum(k.ai_call_count for k in ApiKey.query.filter_by(user_id=g.user.id).all())
    usage    = _get_or_create_usage(g.user.id)
    return jsonify({"today": calls, "ai_today": ai_calls,
                    "rate_limit": "10,000/hr", "ai_rate_limit": "500/hr",
                    "uptime": "99.97%", "latency": "42ms",
                    "tokens_balance": usage.tokens_balance})


# ─── AI QUOTA & TOKEN PURCHASE ────────────────────────────────────────────────

@app.route("/api/ai/quota")
@require_auth
def get_ai_quota():
    usage      = _get_or_create_usage(g.user.id)
    free_limit = FREE_DAILY_QUOTA.get(g.user.tier, 3)
    return jsonify({
        "tokens_balance":  usage.tokens_balance,
        "daily_free_used": usage.daily_free_used,
        "daily_free_limit": free_limit,
        "free_remaining":  max(0, free_limit - usage.daily_free_used),
        "total_ai_calls":  usage.total_ai_calls,
        "total_spent_kes": usage.total_spent_kes,
        "packs": TOKEN_PACKS,
    })


@app.route("/api/ai/purchase", methods=["POST"])
@require_auth
def purchase_tokens():
    """
    Simulates M-Pesa STK push token purchase.
    In production: integrate Safaricom Daraja API here.
    """
    d       = request.json or {}
    pack_id = d.get("pack_id")
    phone   = d.get("phone", g.user.phone or "")
    pack    = next((p for p in TOKEN_PACKS + DEV_TOKEN_PACKS if p["id"] == pack_id), None)
    if not pack:
        return jsonify({"detail": "Unknown pack"}), 400

    # ── Simulate M-Pesa ─────────────────────────────────────────────
    # In prod: POST to Daraja /mpesa/stkpush/v1/processrequest
    # and poll /mpesa/stkpushquery/v1/query for confirmation.
    # Here we immediately credit the tokens.
    # ─────────────────────────────────────────────────────────────────
    usage = _get_or_create_usage(g.user.id)
    usage.tokens_balance  += pack["tokens"]
    usage.total_spent_kes += pack["price_kes"]
    db.session.commit()

    _notify_user(g.user.id,
                 f"✅ {pack['tokens']} AI tokens added · KES {pack['price_kes']} charged to {phone or 'your M-Pesa'}",
                 notif_type="ai_purchase")
    return jsonify({
        "ok": True,
        "tokens_added": pack["tokens"],
        "tokens_balance": usage.tokens_balance,
        "mpesa_ref": f"QJM{random.randint(1000000, 9999999)}",
        "message": f"M-Pesa payment of KES {pack['price_kes']:,} confirmed. {pack['tokens']} tokens added.",
    })


# ─── AI — FAIDA DISPATCHER ────────────────────────────────────────────────────

@app.route("/api/ai/faida/dispatch", methods=["POST"])
@require_auth
def faida_dispatch():
    """
    Full-spectrum AI route analysis.
    Deducts 1 token (free daily or purchased).
    Returns structured JSON with execution plan, risk assessment, and
    pre-filled calculator config for direct injection into TradeCalculator.
    """
    ok, quota = _check_and_consume_token(g.user)
    if not ok:
        return jsonify({"error": "quota_exceeded", "quota": quota}), 429

    d = request.json or {}
    c = _lookup_commodity(d.get("commodity_id", ""))
    o = Market.query.get(d.get("origin_market_id"))
    t = Market.query.get(d.get("target_market_id"))
    if not c or not o or not t:
        return jsonify({"detail": "Invalid inputs"}), 400

    buy_price  = latest_price(o.id, c.id) or smart_base_price(c, o)
    sell_price = latest_price(t.id, c.id) or smart_base_price(c, t)
    wx_o       = latest_weather(o.id) or {"prem": 0, "rain": False, "icon": "☀️", "w": "Clear", "tmp": 24}
    wx_t       = latest_weather(t.id) or {"prem": 0, "rain": False, "icon": "☀️", "w": "Clear", "tmp": 24}
    saccos     = TransportSacco.query.order_by(TransportSacco.rate_per_km_mt).all()
    best_rate  = saccos[0].rate_per_km_mt if saccos else 2.0
    dist       = haversine(o.lat, o.lon, t.lat, t.lon)
    tc         = transport_cost_per_kg(dist, best_rate, wx_o.get("prem", 0))
    spread     = (sell_price - buy_price) / buy_price * 100 if buy_price else 0
    net_per_kg = sell_price - buy_price - tc

    # Historical prices (last 7 days at origin)
    since   = datetime.now(timezone.utc) - timedelta(days=7)
    recent  = (PriceEntry.query.filter_by(commodity_id=c.id, market_id=o.id)
               .filter(PriceEntry.recorded_at >= since)
               .order_by(PriceEntry.recorded_at.desc()).limit(7).all())
    hist    = ", ".join([f"KES {_pval(e):.0f}" for e in recent[:5]]) if recent else "No history"

    # Upcoming market day
    nmd = MarketDay.query.filter(
        MarketDay.market_id == t.id,
        MarketDay.scheduled_date >= date.today(),
        MarketDay.scheduled_date <= date.today() + timedelta(days=14),
    ).first()

    ai_text = call_claude(
        system_prompt="""You are AgriTrade Faida Dispatcher — East Africa's most precise agricultural trade intelligence system.
Analyze commodity arbitrage routes with rigorous attention to real-world Kenya logistics, weather risk, and market microstructure.
Respond ONLY with valid JSON. Zero prose outside the JSON structure. No markdown code fences.""",
        user_message=f"""Analyze this arbitrage route:

COMMODITY: {c.name} ({c.emoji}) · Unit: {c.unit}
ORIGIN:      {o.name}, {o.region} — Buy KES {buy_price:.2f}/{c.unit}
DESTINATION: {t.name}, {t.region} — Sell KES {sell_price:.2f}/{c.unit}
DISTANCE:    {dist:.1f} km
TRANSPORT:   KES {tc:.2f}/kg (best SACCO rate: KES {best_rate}/km/MT)
RAW SPREAD:  {spread:.1f}%   NET/KG: KES {net_per_kg:.2f}

WEATHER ORIGIN:      {wx_o.get('w','Clear')} {wx_o.get('icon','☀️')} {wx_o.get('tmp',24)}°C{' ⚠️ RAINING +' + str(round(wx_o.get('prem',0)*100)) + '% tc' if wx_o.get('rain') else ''}
WEATHER DESTINATION: {wx_t.get('w','Clear')} {wx_t.get('icon','☀️')} {wx_t.get('tmp',24)}°C
RECENT ORIGIN PRICES (last 7 days): {hist}
NEXT MARKET DAY @ DEST: {'Day ' + str(nmd.scheduled_date.day) + ' of month' if nmd else 'None in 14 days'}

Return ONLY this JSON (no other text):
{{
  "route_score": <1.0-10.0>,
  "recommendation": "<EXECUTE|REVIEW|WAIT>",
  "confidence_pct": <0-100>,
  "summary": "<2 sentences — executive decision brief>",
  "execution_steps": [
    {{"step": 1, "action": "<specific action>", "timing": "<when exactly>", "kpi": "<measurable target>"}},
    {{"step": 2, "action": "<specific action>", "timing": "<when exactly>", "kpi": "<measurable target>"}},
    {{"step": 3, "action": "<specific action>", "timing": "<when exactly>", "kpi": "<measurable target>"}}
  ],
  "risk_factors": [
    {{"risk": "<specific risk>", "severity": "<HIGH|MED|LOW>", "mitigation": "<concrete action>"}}
  ],
  "optimal_volume_kg": <integer>,
  "optimal_timing": "<specific timing advice>",
  "projected_margin_pct": <float>,
  "price_trend": "<RISING|STABLE|FALLING>",
  "calculator_config": {{
    "kg": <optimal volume>,
    "loss_pct": <recommended %>,
    "pkg_rate": <KES per kg>,
    "notes": "<one insight>"
  }},
  "market_intel": "<2-3 sentences of Kenya-specific market intelligence>"
}}""",
        max_tokens=1000,
    )

    if ai_text is None:
        result = _faida_demo(c, o, t, buy_price, sell_price, spread, net_per_kg, dist, tc, wx_o)
    else:
        try:
            result = json.loads(ai_text)
        except json.JSONDecodeError:
            result = _faida_demo(c, o, t, buy_price, sell_price, spread, net_per_kg, dist, tc, wx_o)

    result["quota"]     = quota
    result["buy_price"] = round(buy_price, 2)
    result["sell_price"]= round(sell_price, 2)
    result["tc_kg"]     = round(tc, 2)
    result["dist_km"]   = round(dist, 1)

    socketio.emit("ai_dispatch_ready", {
        "user_id": g.user.id,
        "route": f"{o.name} → {t.name}",
        "commodity": c.name,
        "recommendation": result.get("recommendation", "REVIEW"),
        "route_score": result.get("route_score", 0),
    }, namespace="/")

    return jsonify(result)


# ─── AI — AJIRIWA NEGOTIATION ENGINE ─────────────────────────────────────────

@app.route("/api/ai/ajiriwa/analyze", methods=["POST"])
@require_auth
def ajiriwa_analyze():
    """
    Ajiriwa engine: analyze a bid against fair market value and suggest a counter-offer.
    Triggered by Farmer/Broker clicking "Analyze with Ajiriwa" on a bid.
    Costs 1 AI token.
    """
    ok, quota = _check_and_consume_token(g.user)
    if not ok:
        return jsonify({"error": "quota_exceeded", "quota": quota}), 429

    bid_id = (request.json or {}).get("bid_id")
    bid    = Bid.query.get_or_404(bid_id)
    com    = Commodity.query.get(bid.commodity_id)
    if not com:
        return jsonify({"detail": "Commodity not found"}), 400

    # Calculate fair price: average across all markets + transport + quality premium
    all_prices = [latest_price(m.id, com.id) for m in Market.query.all()]
    all_prices = [p for p in all_prices if p]
    market_avg = sum(all_prices) / len(all_prices) if all_prices else smart_base_price(com, Market.query.first())
    hub_price  = com.hub_price or com.base_price or 50.0
    # Fair = weighted avg (60% market avg, 40% hub) + 5% quality premium
    fair_price   = (market_avg * 0.6 + hub_price * 0.4) * 1.05
    discount_pct = (fair_price - bid.bid_price) / fair_price * 100
    # Counter: meet 65% of the way to fair value
    counter_price = bid.bid_price + (fair_price - bid.bid_price) * 0.65

    ai_text = call_claude(
        system_prompt="""You are Ajiriwa — AgriTrade's autonomous negotiation intelligence.
You protect smallholder farmers and brokers from below-market bids.
Analyse bids with precision using real Kenya market data. Always respond in valid JSON only.""",
        user_message=f"""Analyze this marketplace bid:

COMMODITY:    {com.name} ({com.emoji}) · Unit: {com.unit}
BUYER BID:    KES {bid.bid_price:.2f}/{com.unit} for {bid.qty:,.0f} {com.unit}
BUYER:        {bid.buyer.name} · Expires: {bid.expires_at.strftime('%Y-%m-%d %H:%M') if bid.expires_at else 'N/A'}
BID NOTES:    {bid.notes or 'None'}
MARKET AVG:   KES {market_avg:.2f}/{com.unit} (average across {len(all_prices)} markets)
HUB PRICE:    KES {hub_price:.2f}/{com.unit}
FAIR VALUE:   KES {fair_price:.2f}/{com.unit} (market avg + quality premium)
DISCOUNT:     {discount_pct:.1f}% below fair value
COUNTER CALC: KES {counter_price:.2f}/{com.unit} (65% toward fair value)

Return ONLY this JSON:
{{
  "fair_price": {round(fair_price, 2)},
  "counter_price": <your recommended counter KES float>,
  "market_avg": {round(market_avg, 2)},
  "discount_pct": {round(discount_pct, 1)},
  "market_position": "<ABOVE_MARKET|AT_MARKET|BELOW_MARKET>",
  "confidence": <0.0-1.0>,
  "reasoning": "<2-3 sentences explaining the analysis>",
  "negotiation_advice": "<specific tactical advice for the seller — max 2 sentences>",
  "urgency": "<HIGH|MEDIUM|LOW>",
  "events": [
    {{"time": "Just now", "type": "scan",    "text": "<price scan summary>"}},
    {{"time": "Just now", "type": "analysis","text": "<fair value analysis>"}},
    {{"time": "Just now", "type": "verdict", "text": "<verdict vs market>", "highlight": true}},
    {{"time": "Just now", "type": "counter", "text": "<counter-offer rationale>", "highlight": true}}
  ]
}}""",
        max_tokens=700,
    )

    if ai_text is None:
        data = _ajiriwa_demo(bid, com, market_avg, fair_price, counter_price, discount_pct)
    else:
        try:
            data = json.loads(ai_text)
        except json.JSONDecodeError:
            data = _ajiriwa_demo(bid, com, market_avg, fair_price, counter_price, discount_pct)

    # Persist negotiation state
    neg = bid.negotiation
    if not neg:
        neg = AiNegotiation(bid_id=bid.id)
        db.session.add(neg)

    neg.status           = "suggested"
    neg.market_avg_price = round(market_avg, 2)
    neg.ai_fair_price    = data.get("fair_price", round(fair_price, 2))
    neg.ai_counter_price = data.get("counter_price", round(counter_price, 2))
    neg.ai_reasoning     = data.get("reasoning", "")
    neg.discount_pct     = data.get("discount_pct", round(discount_pct, 1))
    neg.confidence       = data.get("confidence", 0.83)
    neg.events_json      = json.dumps(data.get("events", []))
    neg.updated_at       = datetime.now(timezone.utc)
    db.session.commit()

    data["quota"]    = quota
    data["bid_id"]   = bid.id
    data["neg_id"]   = neg.id
    data["bid_price"]= bid.bid_price

    # Emit to buyer so they see the negotiation is active
    socketio.emit("ajiriwa_analyzed", {
        "bid_id": bid.id,
        "commodity": com.name,
        "counter_price": neg.ai_counter_price,
        "status": "suggested",
    }, namespace="/")
    _notify_user(bid.buyer_id,
                 f"🤝 Ajiriwa counter on your {com.name} bid: KES {neg.ai_counter_price:.0f}/{com.unit}")

    return jsonify(data)


@app.route("/api/ai/ajiriwa/action", methods=["POST"])
@require_auth
def ajiriwa_action():
    """
    Seller/Buyer takes action on a negotiation:
    action: accept_bid | accept_counter | reject | buyer_counter
    """
    d      = request.json or {}
    bid    = Bid.query.get_or_404(d.get("bid_id"))
    neg    = bid.negotiation
    if not neg:
        return jsonify({"detail": "No active negotiation"}), 404
    action = d.get("action", "")
    com    = Commodity.query.get(bid.commodity_id)
    events = json.loads(neg.events_json or "[]")
    now    = datetime.now(timezone.utc).strftime("%H:%M")

    if action == "accept_bid":
        bid.status          = "accepted"
        bid.accepted_by_id  = g.user.id
        neg.status          = "resolved"
        neg.resolved_price  = bid.bid_price
        events.append({"time": now, "type": "resolved",
                        "text": f"✅ Accepted at KES {bid.bid_price:.2f}/{com.unit if com else ''}", "highlight": True})
        _notify_user(bid.buyer_id, f"✅ {g.user.name} accepted your {com.name if com else ''} bid @ KES {bid.bid_price}/{bid.unit}")

    elif action == "accept_counter":
        bid.status          = "accepted"
        bid.accepted_by_id  = g.user.id
        neg.status          = "resolved"
        neg.resolved_price  = neg.ai_counter_price
        bid.bid_price       = neg.ai_counter_price
        events.append({"time": now, "type": "resolved",
                        "text": f"✅ Counter accepted: KES {neg.ai_counter_price:.2f}/{com.unit if com else ''}", "highlight": True})
        _notify_user(bid.buyer_id, f"✅ Counter-offer of KES {neg.ai_counter_price:.0f}/{bid.unit} accepted by {g.user.name}")

    elif action == "reject":
        neg.status = "rejected"
        events.append({"time": now, "type": "rejected",
                        "text": "❌ Negotiation rejected", "highlight": False})
        _notify_user(bid.buyer_id, f"❌ Your {com.name if com else ''} bid was declined by {g.user.name}")

    elif action == "buyer_counter":
        new_price = float(d.get("price", bid.bid_price))
        neg.status = "buyer_countered"
        events.append({"time": now, "type": "buyer_counter",
                        "text": f"💬 Buyer counter-offer: KES {new_price:.2f}/{com.unit if com else ''}",
                        "highlight": True})
        _notify_user(bid.buyer_id,
                     f"💬 Counter received on your {com.name if com else ''} bid: KES {new_price:.0f}/{bid.unit}")

    else:
        return jsonify({"detail": f"Unknown action: {action}"}), 400

    neg.events_json = json.dumps(events)
    neg.updated_at  = datetime.now(timezone.utc)
    db.session.commit()
    socketio.emit("negotiation_update", bid_to_dict(bid), namespace="/")
    return jsonify(bid_to_dict(bid))


@app.route("/api/ai/ajiriwa/<int:bid_id>")
@require_auth
def get_negotiation(bid_id):
    bid = Bid.query.get_or_404(bid_id)
    neg = bid.negotiation
    if not neg: return jsonify(None)
    return jsonify({
        "status":          neg.status,
        "market_avg":      neg.market_avg_price,
        "ai_fair_price":   neg.ai_fair_price,
        "ai_counter_price":neg.ai_counter_price,
        "discount_pct":    neg.discount_pct,
        "reasoning":       neg.ai_reasoning,
        "confidence":      neg.confidence,
        "resolved_price":  neg.resolved_price,
        "events":          json.loads(neg.events_json or "[]"),
    })


# ─── DEVELOPER API (v1) ───────────────────────────────────────────────────────

@app.route("/api/v1/prices/live")
@require_api_key
def dev_live_prices():
    """Developer API: live prices. Same as internal but API-key gated."""
    return get_live_prices()


@app.route("/api/v1/arbitrage/opportunities")
@require_api_key
def dev_arbitrage():
    """Developer API: current arbitrage opportunities."""
    if g.user.tier not in ("premium", "enterprise"):
        return jsonify({"error": "Enterprise tier required for arbitrage via API"}), 403
    return get_arbitrage_opps()


@app.route("/api/v1/ai/market-brief", methods=["POST"])
@require_api_key
def dev_ai_brief():
    """
    Developer AI API: generate a market intelligence brief for a commodity.
    Costs 1 AI token from the developer's account.
    Expose this in your own apps.
    """
    ok, quota = _check_and_consume_token(g.user)
    if not ok:
        return jsonify({"error": "ai_quota_exceeded",
                        "detail": "Purchase more tokens at /api/ai/purchase",
                        "quota": quota}), 429

    d         = request.json or {}
    com_id    = d.get("commodity_id", "")
    mkt_ids   = d.get("market_ids", [])  # optional filter
    com       = _lookup_commodity(com_id)
    if not com:
        return jsonify({"error": f"Unknown commodity: {com_id}"}), 400

    markets   = Market.query.filter(Market.id.in_(mkt_ids)).all() if mkt_ids else Market.query.all()
    price_lines = []
    for m in markets[:10]:
        p = latest_price(m.id, com.id)
        if p: price_lines.append(f"  - {m.name} ({m.region}): KES {p:.2f}/{com.unit}")

    ai_text = call_claude(
        system_prompt="You are AgriTrade market analyst. Provide concise, data-driven market briefs for the East African agricultural market. Be specific about prices, trends, and actionable advice. Respond in plain text (not JSON).",
        user_message=f"""Write a 3-paragraph market intelligence brief for {com.name} ({com.emoji}):

Live market prices:
{chr(10).join(price_lines) if price_lines else '  No live data available'}

Focus on:
1. Current price landscape and spread analysis
2. Key supply/demand drivers this week
3. Trading recommendation for farmers and buyers

Be specific with KES amounts. Max 200 words.""",
        max_tokens=400,
    )

    if ai_text is None:
        ai_text = (f"{com.name} market is active across {len(markets)} monitored markets. "
                   f"Prices range from KES {min(p for p in [latest_price(m.id, com.id) for m in markets] if p) if any(latest_price(m.id, com.id) for m in markets) else 'N/A'} "
                   f"to KES {max(p for p in [latest_price(m.id, com.id) for m in markets] if p) if any(latest_price(m.id, com.id) for m in markets) else 'N/A'}/{com.unit}. "
                   f"Set ANTHROPIC_API_KEY for full AI analysis.")

    g.api_key.ai_call_count += 1
    db.session.commit()

    return jsonify({
        "commodity": com.name,
        "emoji": com.emoji,
        "unit": com.unit,
        "brief": ai_text,
        "markets_analyzed": len(price_lines),
        "quota": quota,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/v1/ai/route-analysis", methods=["POST"])
@require_api_key
def dev_ai_route():
    """
    Developer AI API: full route analysis (same as Faida Dispatcher).
    Costs 1 AI token.
    """
    if g.user.tier not in ("premium", "enterprise"):
        return jsonify({"error": "Premium or Enterprise tier required"}), 403
    # delegate to the same logic
    ok, quota = _check_and_consume_token(g.user)
    if not ok:
        return jsonify({"error": "ai_quota_exceeded", "quota": quota}), 429
    g.api_key.ai_call_count += 1
    db.session.commit()
    # re-use the faida dispatch handler's core (without token re-deduction)
    d = request.json or {}
    c = _lookup_commodity(d.get("commodity_id", ""))
    o = Market.query.get(d.get("origin_market_id"))
    t = Market.query.get(d.get("target_market_id"))
    if not c or not o or not t:
        return jsonify({"error": "Invalid inputs. Provide commodity_id, origin_market_id, target_market_id."}), 400
    buy_price  = latest_price(o.id, c.id) or smart_base_price(c, o)
    sell_price = latest_price(t.id, c.id) or smart_base_price(c, t)
    wx_o       = latest_weather(o.id) or {"prem": 0, "rain": False, "icon": "☀️", "w": "Clear", "tmp": 24}
    saccos     = TransportSacco.query.order_by(TransportSacco.rate_per_km_mt).all()
    best_rate  = saccos[0].rate_per_km_mt if saccos else 2.0
    dist       = haversine(o.lat, o.lon, t.lat, t.lon)
    tc         = transport_cost_per_kg(dist, best_rate, wx_o.get("prem", 0))
    spread     = (sell_price - buy_price) / buy_price * 100 if buy_price else 0
    net_per_kg = sell_price - buy_price - tc
    result     = _faida_demo(c, o, t, buy_price, sell_price, spread, net_per_kg, dist, tc, wx_o)
    result["quota"] = quota
    result["buy_price"] = round(buy_price, 2)
    result["sell_price"]= round(sell_price, 2)
    return jsonify(result)


# ─── HEALTH ───────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok",
                    "ai_enabled": bool(ANTHROPIC_KEY),
                    "timestamp": datetime.now(timezone.utc).isoformat()})


# ─── WEBSOCKET ────────────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect(auth):
    uid = decode_token((auth or {}).get("token", ""))
    if uid:
        from flask_socketio import join_room
        join_room(f"user_{uid}")


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        run_migrations()
    socketio.run(app, debug=True, host="0.0.0.0", port=5000)