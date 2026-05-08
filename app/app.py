import eventlet
eventlet.monkey_patch()

import os, math, random
from datetime import datetime, date, timedelta, timezone
from functools import wraps

from flask import Flask, request, jsonify, g
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_socketio import SocketIO
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from sqlalchemy import text

app = Flask(__name__)
CORS(app, origins="*", supports_credentials=True)

DB_URL     = os.getenv("DATABASE_URL", "sqlite:///agri.db")
REDIS_URL  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
JWT_SECRET = os.getenv("JWT_SECRET", "dev_secret_change_in_prod")
JWT_EXPIRE = int(os.getenv("JWT_EXPIRE_HOURS", 72))

app.config.update(
    SQLALCHEMY_DATABASE_URI=DB_URL,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    CELERY_BROKER_URL=REDIS_URL,
    broker_url=REDIS_URL,
    result_backend=REDIS_URL,
)

db = SQLAlchemy(app)
socketio = SocketIO(app, message_queue=REDIS_URL, cors_allowed_origins="*", async_mode="eventlet")


# ─── MIGRATION ────────────────────────────────────────────────────────────────

def run_migrations():
    """Safe to run multiple times. Adds missing columns, drops bad constraints."""
    stmts = [
        # market
        "ALTER TABLE market ADD COLUMN IF NOT EXISTS climate    VARCHAR(50) DEFAULT 'highland'",
        "ALTER TABLE market ADD COLUMN IF NOT EXISTS created_at TIMESTAMP   DEFAULT now()",
        # Update any null climates
        "UPDATE market SET climate = 'highland' WHERE climate IS NULL",

        # commodity
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS hub_lat        FLOAT",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS hub_lon        FLOAT",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS hub_price      FLOAT DEFAULT 50.0",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS max_multiplier FLOAT DEFAULT 2.0",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS emoji          VARCHAR(10) DEFAULT '🌿'",
        "ALTER TABLE commodity ADD COLUMN IF NOT EXISTS base_price     FLOAT DEFAULT 50.0",

        # price_entry — add new canonical column
        "ALTER TABLE price_entry ADD COLUMN IF NOT EXISTS price FLOAT",
        "ALTER TABLE price_entry ADD COLUMN IF NOT EXISTS grade VARCHAR(5) DEFAULT 'A'",
        "ALTER TABLE price_entry ADD COLUMN IF NOT EXISTS is_agent_verified BOOLEAN DEFAULT FALSE",

        # KEY FIX: drop NOT NULL constraint on legacy price_per_unit so new inserts work
        "ALTER TABLE price_entry ALTER COLUMN price_per_unit DROP NOT NULL",

        # Backfill price from price_per_unit for old rows
        "UPDATE price_entry SET price = price_per_unit WHERE price IS NULL AND price_per_unit IS NOT NULL",

        # market_day
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
    base_price     = db.Column(db.Float,        default=50.0)  # legacy compat
    volatility     = db.Column(db.Float,        default=0.08)
    max_multiplier = db.Column(db.Float,        default=2.0)


class PriceEntry(db.Model):
    id                = db.Column(db.Integer, primary_key=True)
    market_id         = db.Column(db.Integer, db.ForeignKey("market.id"),    nullable=False)
    commodity_id      = db.Column(db.Integer, db.ForeignKey("commodity.id"), nullable=False)
    price             = db.Column(db.Float)   # canonical — always written from now on
    price_per_unit    = db.Column(db.Float)   # legacy column — nullable after migration
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
    """Legacy model — kept so old rows don't error."""
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
    """Legacy — kept to avoid FK errors on old rows."""
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

    def set_password(self, pw):
        self.password_hash = generate_password_hash(pw)

    def check_password(self, pw):
        return check_password_hash(self.password_hash, pw)

    def make_token(self):
        payload = {
            "sub": str(self.id),          # ← convert to string
            "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE)
        }
        return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

    def to_dict(self):
        return {
            "id": self.id, "name": self.name, "email": self.email,
            "role": self.role, "tier": self.tier,
            "phone": self.phone, "location": self.location,
            "subscribed_commodities": [
                s.strip() for s in (self.subscribed_commodities or "").split(",") if s.strip()
            ],
            "alert_radius_km": self.alert_radius_km,
        }


class ApiKey(db.Model):
    id          = db.Column(db.Integer, primary_key=True)
    user_id     = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    name        = db.Column(db.String(200), nullable=False)
    key_hash    = db.Column(db.String(256), nullable=False)
    key_preview = db.Column(db.String(30),  nullable=False)
    call_count  = db.Column(db.Integer, default=0)
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


# ─── AUTH HELPERS ─────────────────────────────────────────────────────────────

# def decode_token(token):
#     try:
#         return jwt.decode(token, JWT_SECRET, algorithms=["HS256"]).get("sub")
#     except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
#         return None


# def require_auth(f):
#     @wraps(f)
#     def wrapper(*args, **kwargs):
#         token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
#         if not token:
#             return jsonify({"detail": "Missing token"}), 401
#         uid = decode_token(token)
#         if not uid:
#             return jsonify({"detail": "Invalid or expired token"}), 401
#         g.user = User.query.get(uid)
#         if not g.user:
#             return jsonify({"detail": "User not found"}), 401
#         return f(*args, **kwargs)
#     return wrapper


# def require_tier(*tiers):
#     def decorator(f):
#         @wraps(f)
#         @require_auth
#         def wrapper(*args, **kwargs):
#             if g.user.tier not in tiers:
#                 return jsonify({"detail": f"Requires tier: {', '.join(tiers)}"}), 403
#             return f(*args, **kwargs)
#         return wrapper
#     return decorator


# def require_role(*roles):
#     def decorator(f):
#         @wraps(f)
#         @require_auth
#         def wrapper(*args, **kwargs):
#             if g.user.role not in roles:
#                 return jsonify({"detail": f"Requires role: {', '.join(roles)}"}), 403
#             return f(*args, **kwargs)
#         return wrapper
#     return decorator


def decode_token(token):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return int(payload["sub"])    # ← back to int for DB queries
    except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError, KeyError, ValueError):
        return None
    except Exception as e:
        print(f"[auth] {type(e).__name__}: {e}")
        return None
 
def _token_from_request():
    h = request.headers.get("Authorization", "")
    if h.lower().startswith("bearer "):
        return h[7:].strip()
    return ""
 
 
def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = _token_from_request()
        if not token:
            return jsonify({"detail": "Missing token"}), 401
        uid = decode_token(token)
        if not uid:
            return jsonify({"detail": "Invalid or expired token"}), 401
        g.user = User.query.get(uid)
        if not g.user:
            return jsonify({"detail": "User not found"}), 401
        return f(*args, **kwargs)
    return wrapper
 
 
# NOTE: require_tier and require_role do NOT use @require_auth internally.
# Stacking @require_auth inside these caused silent 401s.
def require_tier(*tiers):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            token = _token_from_request()
            if not token:
                return jsonify({"detail": "Missing token"}), 401
            uid = decode_token(token)
            if not uid:
                return jsonify({"detail": "Invalid or expired token"}), 401
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
            token = _token_from_request()
            if not token:
                return jsonify({"detail": "Missing token"}), 401
            uid = decode_token(token)
            if not uid:
                return jsonify({"detail": "Invalid or expired token"}), 401
            g.user = User.query.get(uid)
            if not g.user:
                return jsonify({"detail": "User not found"}), 401
            if g.user.role not in roles:
                return jsonify({"detail": f"Requires role: {', '.join(roles)}"}), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator
 
 
# ─── ADD THIS ENDPOINT anywhere before the entrypoint ────────────────────────
@app.route("/api/auth/debug-token")
def debug_token():
    """No auth required. Tells you exactly why a token fails."""
    raw = _token_from_request()
    if not raw:
        return jsonify({"error": "Send Authorization: Bearer <token>"}), 400
    try:
        payload = jwt.decode(raw, JWT_SECRET, algorithms=["HS256"])
        user = User.query.get(payload.get("sub"))
        return jsonify({
            "valid": True,
            "sub": payload.get("sub"),
            "user_found": user is not None,
            "user_email": user.email if user else None,
            "user_role": user.role if user else None,
            "user_tier": user.tier if user else None,
            "secret_prefix": JWT_SECRET[:10] + "…",
        })
    except jwt.ExpiredSignatureError:
        return jsonify({"valid": False, "reason": "Expired — re-login"})
    except jwt.InvalidSignatureError:
        return jsonify({
            "valid": False,
            "reason": "Wrong secret",
            "fix": "Re-login after docker compose restart",
            "secret_prefix": JWT_SECRET[:10] + "…",
        })
    except Exception as e:
        return jsonify({"valid": False, "reason": str(e)})
# ─── HELPERS ──────────────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    d = lambda x: x * math.pi / 180
    a = (math.sin(d(lat2 - lat1) / 2) ** 2
         + math.cos(d(lat1)) * math.cos(d(lat2)) * math.sin(d(lon2 - lon1) / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def smart_base_price(commodity, market):
    hl  = commodity.hub_lat  or 0.0
    hln = commodity.hub_lon  or 0.0
    hp  = commodity.hub_price or commodity.base_price or 50.0
    dist         = haversine(hl, hln, market.lat, market.lon)
    dist_factor  = 1 + (dist * 0.035 / 100)
    coastal_prem = 1.18 if (market.climate or "highland") == "coastal" else 1.0
    hub_disc     = 0.88 if dist < 30 else 1.0
    raw = hp * dist_factor * coastal_prem * hub_disc
    return min(raw, hp * (commodity.max_multiplier or 2.0))


def _pval(entry):
    """Read price from either canonical or legacy column."""
    if entry is None:
        return None
    return entry.price if entry.price is not None else entry.price_per_unit


def latest_price(market_id, commodity_id):
    e = (PriceEntry.query
         .filter_by(market_id=market_id, commodity_id=commodity_id)
         .order_by(PriceEntry.recorded_at.desc())
         .first())
    return _pval(e)


def latest_weather(market_id):
    log = (WeatherLog.query
           .filter_by(market_id=market_id)
           .order_by(WeatherLog.recorded_at.desc())
           .first())
    if not log:
        return None
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
    """Always write both price (new) and price_per_unit (legacy) to avoid NOT NULL issues."""
    kwargs = dict(market_id=market_id, commodity_id=commodity_id,
                  price=price, price_per_unit=price,
                  grade=grade, is_agent_verified=verified)
    if recorded_at:
        kwargs["recorded_at"] = recorded_at
    return PriceEntry(**kwargs)


def bid_to_dict(b):
    com = Commodity.query.get(b.commodity_id)
    exp = ""
    if b.expires_at:
        ea = b.expires_at.replace(tzinfo=timezone.utc) if b.expires_at.tzinfo is None else b.expires_at
        h  = max(0, int((ea - datetime.now(timezone.utc)).total_seconds() / 3600))
        exp = f"{h}h"
    return {
        "id": b.id, "buyer": b.buyer.name, "buyer_id": b.buyer_id,
        "commodity": c_slug(com) if com else str(b.commodity_id),
        "unit": b.unit, "bid_price": b.bid_price, "qty": b.qty,
        "expires": exp, "status": b.status, "notes": b.notes,
        "created_at": b.created_at.isoformat(),
        "accepted_by": b.accepted_by.name if b.accepted_by else None,
    }


def _rel_time(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
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
    if roles:
        q = q.filter(User.role.in_(roles))
    com = Commodity.query.get(commodity_id)
    slug = c_slug(com) if com else ""
    for user in q.all():
        subs = [s.strip() for s in (user.subscribed_commodities or "").split(",")]
        if subs and slug and slug not in subs:
            continue
        _notify_user(user.id, msg, notif_type="bid")


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
    return jsonify({"access_token": u.make_token(), "token_type": "bearer", "user": u.to_dict()}), 201


@app.route("/api/auth/login", methods=["POST"])
def login():
    d = request.json or {}
    u = User.query.filter_by(email=d.get("email", "")).first()
    if not u or not u.check_password(d.get("password", "")):
        return jsonify({"detail": "Invalid credentials"}), 401
    return jsonify({"access_token": u.make_token(), "token_type": "bearer", "user": u.to_dict()})


@app.route("/api/auth/me")
@require_auth
def me():
    return jsonify(g.user.to_dict())


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
            if p is None:
                p = smart_base_price(c, m)
            prev_e = (PriceEntry.query
                      .filter_by(market_id=m.id, commodity_id=c.id)
                      .order_by(PriceEntry.recorded_at.desc())
                      .offset(1).first())
            prev = _pval(prev_e) or p
            pct  = ((p - prev) / prev * 100) if prev else 0
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
    if not c:
        return jsonify([])
    since = datetime.now(timezone.utc) - timedelta(days=days)
    entries = (PriceEntry.query
               .filter_by(commodity_id=c.id, market_id=mkt_id)
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
    saccos = TransportSacco.query.all()
    default_rate = saccos[0].rate_per_km_mt if saccos else 2.0
    result = []
    for c in commodities:
        pm = {m.id: {"market": m, "price": p}
              for m in markets if (p := latest_price(m.id, c.id))}
        if len(pm) < 2:
            continue
        lo = min(pm.values(), key=lambda x: x["price"])
        hi = max(pm.values(), key=lambda x: x["price"])
        spread = (hi["price"] - lo["price"]) / lo["price"] * 100
        if spread < 8:
            continue
        dist = haversine(lo["market"].lat, lo["market"].lon,
                         hi["market"].lat, hi["market"].lon)
        wx_o = latest_weather(lo["market"].id) or {"prem": 0, "rain": False, "icon": "☀️", "w": "Clear", "tmp": 24}
        wx_d = latest_weather(hi["market"].id) or {"prem": 0, "rain": False, "icon": "☀️", "w": "Clear", "tmp": 24}
        tc   = dist * default_rate * (1 + wx_o["prem"]) / 100
        result.append({
            "com": c.name, "comId": c_slug(c), "emoji": c.emoji, "unit": c.unit,
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
    d = request.json or {}
    c = _lookup_commodity(d.get("commodity_id", ""))
    o = Market.query.get(d.get("origin_market_id"))
    t = Market.query.get(d.get("target_market_id"))
    if not c or not o or not t:
        return jsonify({"detail": "Invalid inputs"}), 400
    lpu  = latest_price(o.id, c.id) or smart_base_price(c, o)
    dpu  = latest_price(t.id, c.id) or smart_base_price(c, t)
    vol  = float(d.get("volume_mt", 1))
    dist = haversine(o.lat, o.lon, t.lat, t.lon)
    wx_o = latest_weather(o.id) or {"prem": 0}
    saccos = TransportSacco.query.order_by(TransportSacco.rate_per_km_mt).all()
    routes = []
    for i, s in enumerate(saccos[:3]):
        da = round(dist * (1 + i * 0.07), 1)
        r  = s.rate_per_km_mt * (1 + wx_o["prem"])
        routes.append({
            "rank": i + 1, "name": s.name, "dist_km": da,
            "est_time": f"{int(da/78)}h {random.randint(10, 55)}m",
            "rate": f"KES {da * r:.0f}/MT",
            "status": "recommended" if i == 0 else ("traffic" if i == 2 else ""),
        })
    br  = saccos[0].rate_per_km_mt if saccos else 2.0
    tc  = dist * br * (1 + wx_o["prem"]) / 100
    net = dpu - lpu - tc
    return jsonify({
        "spread": round((dpu - lpu) / lpu * 100 if lpu else 0, 1),
        "net": round(net * vol, 0), "lpu": round(lpu, 2), "dpu": round(dpu, 2),
        "dist": round(dist, 1), "routes": routes, "ok": net * vol > 0,
    })


# ─── PREDICTIONS ──────────────────────────────────────────────────────────────

@app.route("/api/predict/forecast")
@require_tier("premium", "enterprise")
def predict_forecast():
    c      = _lookup_commodity(request.args.get("commodity", ""))
    mkt_id = request.args.get("market", type=int)
    if not c:
        return jsonify([])
    since = datetime.now(timezone.utc) - timedelta(days=60)
    entries = (PriceEntry.query.filter_by(commodity_id=c.id, market_id=mkt_id)
               .filter(PriceEntry.recorded_at >= since)
               .order_by(PriceEntry.recorded_at.asc()).all())
    prices = [_pval(e) for e in entries if _pval(e)]
    if len(prices) < 5:
        return jsonify([])
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
    bid.status = "accepted"
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
    if n:
        n.read = True
        db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/notifications/read-all", methods=["POST"])
@require_auth
def mark_all_read():
    Notification.query.filter_by(user_id=g.user.id).update({"read": True})
    db.session.commit()
    return jsonify({"ok": True})


# ─── AGENT ────────────────────────────────────────────────────────────────────

@app.route("/api/agent/prices", methods=["POST"])
# @require_role("Agent")
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
    if not s:
        s = TransportSacco(name=d["sacco_name"])
        db.session.add(s)
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


# ─── DEVELOPER ────────────────────────────────────────────────────────────────

@app.route("/api/dev/keys")
@require_tier("enterprise")
def get_api_keys():
    keys = ApiKey.query.filter_by(user_id=g.user.id, revoked=False).all()
    return jsonify([{"id": k.id, "name": k.name, "key_preview": k.key_preview,
                     "call_count": k.call_count,
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
                    "full_key": f"ak_live_{raw}", "call_count": 0,
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
    calls = sum(k.call_count for k in ApiKey.query.filter_by(user_id=g.user.id).all())
    return jsonify({"today": calls, "rate_limit": "10,000/hr", "uptime": "99.97%", "latency": "42ms"})


# ─── HEALTH ───────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})


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
