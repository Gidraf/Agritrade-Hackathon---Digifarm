"""
seed.py — idempotent. Run: python seed.py
Writes both `price` AND `price_per_unit` on every PriceEntry to satisfy the
old NOT NULL constraint that may still exist before migrations run.
"""
import os, sys, math, random
from datetime import datetime, date, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from app import (
    app, db, run_migrations,
    User, Market, Commodity, PriceEntry, WeatherLog,
    TransportSacco, MarketDay, Bid, Notification,
    smart_base_price, _new_price_entry,
)

# ── Reference data ────────────────────────────────────────────────────────────

MARKETS_DATA = [
    {"name":"Nairobi Central", "region":"Nairobi",    "lat":-1.286,"lon":36.817,"climate":"highland"},
    {"name":"Mombasa Port",    "region":"Coast",       "lat":-4.043,"lon":39.668,"climate":"coastal"},
    {"name":"Kisumu Main",     "region":"Nyanza",      "lat":-0.102,"lon":34.762,"climate":"lakeside"},
    {"name":"Eldoret Hub",     "region":"Rift Valley", "lat": 0.520,"lon":35.270,"climate":"rift"},
    {"name":"Nakuru Town",     "region":"Rift Valley", "lat":-0.303,"lon":36.080,"climate":"rift"},
    {"name":"Bamburi Market",  "region":"Coast",       "lat":-3.984,"lon":39.726,"climate":"coastal"},
    {"name":"Wangige Market",  "region":"Nairobi",     "lat":-1.186,"lon":36.720,"climate":"highland"},
    {"name":"Thika Town",      "region":"Central",     "lat":-1.033,"lon":37.069,"climate":"highland"},
    {"name":"Kongowea Market", "region":"Coast",       "lat":-4.024,"lon":39.680,"climate":"coastal"},
    {"name":"Kitale Market",   "region":"Trans Nzoia", "lat": 1.017,"lon":35.001,"climate":"rift"},
]

COMMODITIES_DATA = [
    {"name":"Tomatoes",   "unit":"KG","emoji":"🍅","hub_lat":-0.303,"hub_lon":36.080,"hub_price":45, "volatility":0.14,"max_multiplier":2.8},
    {"name":"Maize",      "unit":"KG","emoji":"🌽","hub_lat": 1.017,"hub_lon":35.001,"hub_price":32, "volatility":0.07,"max_multiplier":2.2},
    {"name":"Milk",       "unit":"L", "emoji":"🥛","hub_lat":-0.303,"hub_lon":36.080,"hub_price":38, "volatility":0.05,"max_multiplier":1.8},
    {"name":"Wheat",      "unit":"KG","emoji":"🌾","hub_lat": 0.520,"hub_lon":35.270,"hub_price":42, "volatility":0.05,"max_multiplier":1.9},
    {"name":"Coffee",     "unit":"KG","emoji":"☕","hub_lat":-0.482,"hub_lon":37.126,"hub_price":620,"volatility":0.10,"max_multiplier":2.0},
    {"name":"Avocado",    "unit":"KG","emoji":"🥑","hub_lat":-1.033,"hub_lon":37.069,"hub_price":80, "volatility":0.16,"max_multiplier":2.5},
    {"name":"Potatoes",   "unit":"KG","emoji":"🥔","hub_lat":-0.303,"hub_lon":36.080,"hub_price":28, "volatility":0.09,"max_multiplier":2.3},
    {"name":"Beans",      "unit":"KG","emoji":"🫘","hub_lat":-0.303,"hub_lon":36.080,"hub_price":65, "volatility":0.07,"max_multiplier":2.0},
    {"name":"Tilapia",    "unit":"KG","emoji":"🐟","hub_lat":-0.102,"hub_lon":34.762,"hub_price":280,"volatility":0.08,"max_multiplier":2.4},
    {"name":"Sukuma Wiki","unit":"KG","emoji":"🥬","hub_lat":-1.286,"hub_lon":36.817,"hub_price":15, "volatility":0.18,"max_multiplier":3.0},
]

SACCOS_DATA = [
    {"name":"A104 Direct (Rift Valley Trans)", "rate_per_km_mt":2.1},
    {"name":"B3 via Naivasha (Western Exp.)",  "rate_per_km_mt":1.8},
    {"name":"C67 Bypass (Local SACCO)",         "rate_per_km_mt":1.9},
    {"name":"Easy Coach Freight",               "rate_per_km_mt":2.2},
    {"name":"Matatu / Boda Last Mile",          "rate_per_km_mt":3.5},
]

DEMO_USERS = [
    {"name":"Alice Kamau",   "email":"farmer@demo.ke",  "password":"demo1234","role":"Farmer",    "tier":"premium",    "phone":"+254711000001","location":"Nakuru County","commodities":"tomatoes,maize,milk"},
    {"name":"James Ochieng", "email":"buyer@demo.ke",   "password":"demo1234","role":"Buyer",     "tier":"enterprise", "phone":"+254711000002","location":"Nairobi",     "commodities":"maize,milk,wheat"},
    {"name":"Mary Njeri",    "email":"broker@demo.ke",  "password":"demo1234","role":"Broker",    "tier":"premium",    "phone":"+254711000003","location":"Kiambu County","commodities":"tomatoes,avocado,coffee"},
    {"name":"Kevin Mutai",   "email":"agent@demo.ke",   "password":"demo1234","role":"Agent",     "tier":"basic",      "phone":"+254711000004","location":"Trans Nzoia", "commodities":"maize,beans"},
    {"name":"Dev Corp",      "email":"dev@demo.ke",      "password":"demo1234","role":"Developer", "tier":"enterprise", "phone":"+254711000005","location":"Nairobi",     "commodities":""},
]

WEATHER_INIT = {
    "highland": {"w":"Partly Cloudy","icon":"⛅", "tmp":19,"rain":False,"prem":0.00},
    "coastal":  {"w":"Humid",         "icon":"💧", "tmp":29,"rain":False,"prem":0.05},
    "lakeside": {"w":"Cloudy",        "icon":"☁️", "tmp":24,"rain":False,"prem":0.03},
    "rift":     {"w":"Sunny & Dry",   "icon":"☀️", "tmp":26,"rain":False,"prem":0.00},
}

LOGISTICS_OPTS = ["Normal demand","High capacity required","Pre-booked only"]


def seed():
    with app.app_context():
        print("🔧 Creating tables + running migrations…")
        db.create_all()
        run_migrations()   # adds climate, drops price_per_unit NOT NULL, etc.

        # ── Markets ───────────────────────────────────────────────────────
        if Market.query.count() == 0:
            print("🗺  Seeding markets…")
            for m in MARKETS_DATA:
                db.session.add(Market(**m))
            db.session.commit()
        else:
            # Ensure climate column is filled on existing rows
            for mkt in Market.query.all():
                if not mkt.climate:
                    mkt.climate = "highland"
            db.session.commit()
        markets = Market.query.order_by(Market.id).all()
        print(f"   ✓ {len(markets)} markets")

        # ── Commodities ───────────────────────────────────────────────────
        if Commodity.query.count() == 0:
            print("🌽 Seeding commodities…")
            for c in COMMODITIES_DATA:
                db.session.add(Commodity(**c, base_price=c["hub_price"]))
            db.session.commit()
        else:
            # Backfill hub columns for any old rows missing them
            for existing in Commodity.query.all():
                ref = next((r for r in COMMODITIES_DATA if r["name"] == existing.name), None)
                if ref:
                    existing.hub_lat        = ref["hub_lat"]
                    existing.hub_lon        = ref["hub_lon"]
                    existing.hub_price      = ref["hub_price"]
                    existing.volatility     = ref["volatility"]
                    existing.max_multiplier = ref["max_multiplier"]
                    existing.emoji          = ref["emoji"]
                    existing.base_price     = ref["hub_price"]
            db.session.commit()
        commodities = Commodity.query.order_by(Commodity.id).all()
        print(f"   ✓ {len(commodities)} commodities")

        # ── SACCOs ────────────────────────────────────────────────────────
        if TransportSacco.query.count() == 0:
            print("🚛 Seeding SACCOs…")
            for s in SACCOS_DATA:
                db.session.add(TransportSacco(**s))
            db.session.commit()
        print(f"   ✓ {TransportSacco.query.count()} SACCOs")

        # ── Demo users ────────────────────────────────────────────────────
        print("👤 Seeding demo users…")
        user_by_role = {}
        for ud in DEMO_USERS:
            u = User.query.filter_by(email=ud["email"]).first()
            if not u:
                u = User(name=ud["name"], email=ud["email"], role=ud["role"],
                         tier=ud["tier"], phone=ud["phone"], location=ud["location"],
                         subscribed_commodities=ud["commodities"])
                u.set_password(ud["password"])
                db.session.add(u)
                db.session.flush()
                print(f"   ✓ created {ud['email']}")
            else:
                print(f"   ↩ {ud['email']} exists")
            user_by_role[ud["role"]] = u
        db.session.commit()

        # ── Weather ───────────────────────────────────────────────────────
        if WeatherLog.query.count() == 0:
            print("🌤  Seeding initial weather…")
            for m in markets:
                wx = WEATHER_INIT.get(m.climate or "highland", WEATHER_INIT["highland"])
                db.session.add(WeatherLog(
                    market_id=m.id, condition=wx["w"], icon=wx["icon"],
                    temperature=wx["tmp"], is_raining=wx["rain"],
                    transport_premium=wx["prem"],
                ))
            db.session.commit()

        # ── 60-day price history ──────────────────────────────────────────
        if PriceEntry.query.count() == 0:
            print("📈 Seeding 60-day price history…")
            count = 0
            for c in commodities:
                for m in markets:
                    base  = smart_base_price(c, m)
                    price = base
                    for day_back in range(60, 0, -1):
                        dt    = datetime.now(timezone.utc) - timedelta(days=day_back)
                        drift = (base - price) * 0.02
                        noise = (random.random() - 0.47) * 2 * c.volatility * price
                        price = max(base * 0.5, round(price + noise + drift, 2))
                        # Use _new_price_entry so BOTH price AND price_per_unit are written
                        db.session.add(_new_price_entry(
                            m.id, c.id, price,
                            grade=random.choice(["A","A","A","B"]),
                            verified=random.random() > 0.7,
                            recorded_at=dt,
                        ))
                        count += 1
                    if count % 1000 == 0:
                        db.session.flush()
            db.session.commit()
            print(f"   ✓ {count} price entries")
        else:
            print(f"   ↩ {PriceEntry.query.count()} price entries already exist")

        # ── Market calendar ───────────────────────────────────────────────
        if MarketDay.query.count() == 0:
            print("📅 Seeding market calendar…")
            count = 0
            for offset in range(30):
                d = date.today() + timedelta(days=offset)
                if d.weekday() not in (0, 2, 4):
                    continue
                for m in random.sample(markets, k=min(4, len(markets))):
                    if random.random() < 0.4:
                        continue
                    c = random.choice(commodities[:6])
                    db.session.add(MarketDay(
                        market_id=m.id, scheduled_date=d,
                        primary_commodity_id=c.id, primary_commodity=c.name,
                        expected_volume_mt=round(random.uniform(5, 60), 1),
                        logistics_note=random.choice(LOGISTICS_OPTS),
                    ))
                    count += 1
            db.session.commit()
            print(f"   ✓ {count} market days")

        # ── Sample bids ───────────────────────────────────────────────────
        if Bid.query.count() == 0:
            buyer   = user_by_role.get("Buyer")
            com_map = {c.name.lower(): c for c in commodities}
            if buyer:
                print("💰 Seeding sample bids…")
                for bd in [
                    {"name":"milk",    "price":42.0,"qty":10000,"notes":"Grade A, Nairobi delivery","h":48},
                    {"name":"maize",   "price":38.0,"qty":50000,"notes":"Dry Grade A only",         "h":72},
                    {"name":"avocado", "price":95.0,"qty": 5000,"notes":"Hass variety preferred",   "h":24},
                    {"name":"tomatoes","price":55.0,"qty": 8000,"notes":"Class 1 size",             "h":12},
                ]:
                    c = com_map.get(bd["name"])
                    if c:
                        db.session.add(Bid(
                            buyer_id=buyer.id, commodity_id=c.id,
                            bid_price=bd["price"], qty=bd["qty"], unit=c.unit,
                            notes=bd["notes"], status="open",
                            expires_at=datetime.now(timezone.utc) + timedelta(hours=bd["h"]),
                        ))
                db.session.commit()
                print(f"   ✓ {Bid.query.count()} bids")

        # ── Welcome notifications ─────────────────────────────────────────
        if Notification.query.count() == 0:
            print("🔔 Seeding welcome notifications…")
            for u in User.query.all():
                for t, msg in [
                    ("arb",    "🌽 Maize arbitrage: Kitale → Mombasa +38% margin detected"),
                    ("market", "📅 Wangige Market open today — Tomatoes expected KES 78/kg"),
                    ("bid",    "💰 Brookside Dairy bid: Milk KES 42/L · 10,000L · 48h"),
                ]:
                    db.session.add(Notification(
                        user_id=u.id, type=t, msg=msg,
                        created_at=datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 6)),
                    ))
            db.session.commit()

        print("\n✅ Seed complete!")
        print("\n  Email                  Password   Role        Tier")
        print("  ─────────────────────────────────────────────────────")
        for ud in DEMO_USERS:
            print(f"  {ud['email']:<24}{ud['password']:<11}{ud['role']:<12}{ud['tier']}")


if __name__ == "__main__":
    seed()
