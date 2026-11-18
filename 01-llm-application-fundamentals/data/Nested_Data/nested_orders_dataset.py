"""
Nested e-commerce orders -- PySpark / Spark SQL practice dataset
=================================================================
* 26 records (25 distinct order_ids -- the data is intentionally messy)
* `orders` is a plain Python list of dicts (same data as orders.json)
* Small builder helpers (AD, PF, item, PAY, SH, ...) only construct dicts;
  they keep the file short. Print `orders[0]` to see the raw shape.

Usage:
    from nested_orders_dataset import get_df, SCHEMA_DDL
    df = get_df(spark)
"""
import copy
import json

# ---------------------------------------------------------------------------
# SPARK SCHEMA (DDL string; works with createDataFrame and spark.read.schema)
# ---------------------------------------------------------------------------
SCHEMA_DDL = """
order_id STRING,
order_date STRING,
order_status STRING,
customer STRUCT<
  customer_id: STRING,
  profile: STRUCT<
    name: STRING,
    age: INT,
    preferences: STRUCT<
      favorite_categories: ARRAY<STRING>,
      preferred_brands: ARRAY<STRING>,
      communication_channels: ARRAY<STRING>,
      notification_settings: MAP<STRING, ARRAY<STRING>>
    >,
    contact: STRUCT<
      email: STRING,
      phone: STRING,
      addresses: ARRAY<STRUCT<
        type: STRING,
        city: STRING,
        state: STRING,
        geo: STRUCT<latitude: DOUBLE, longitude: DOUBLE>,
        tags: ARRAY<STRING>
      >>
    >
  >,
  loyalty: STRUCT<
    tier: STRING,
    points: BIGINT,
    benefits: ARRAY<STRUCT<
      benefit_id: STRING,
      name: STRING,
      rules: STRUCT<min_spend: DOUBLE, categories: ARRAY<STRING>>
    >>
  >
>,
items ARRAY<STRUCT<
  item_id: STRING,
  product: STRUCT<
    product_id: STRING,
    name: STRING,
    category: STRING,
    brand: STRING,
    attributes: MAP<STRING, STRING>,
    supplier: STRUCT<
      name: STRING,
      certifications: ARRAY<STRUCT<code: STRING, expires: STRING>>
    >
  >,
  quantity: INT,
  pricing: STRUCT<unit_price: DOUBLE, discount: DOUBLE, tax: DOUBLE>,
  variants: ARRAY<STRUCT<
    variant_id: STRING,
    name: STRING,
    attributes: MAP<STRING, STRING>
  >>,
  discounts: ARRAY<STRUCT<
    discount_type: STRING,
    code: STRING,
    amount: DOUBLE,
    conditions: ARRAY<STRUCT<condition_type: STRING, value: STRING>>
  >>
>>,
payments ARRAY<STRUCT<
  payment_id: STRING,
  method: STRING,
  amount: DOUBLE,
  status: STRING,
  metadata: MAP<STRING, STRING>,
  attempts: ARRAY<STRUCT<
    attempt_no: INT,
    result: STRING,
    gateway_response: MAP<STRING, STRING>
  >>
>>,
shipments ARRAY<STRUCT<
  shipment_id: STRING,
  carrier: STRING,
  tracking: STRING,
  packages: ARRAY<STRUCT<
    package_id: STRING,
    weight: DOUBLE,
    dimensions: STRUCT<length: DOUBLE, width: DOUBLE, height: DOUBLE>,
    contents: ARRAY<STRUCT<item_id: STRING, qty: INT>>
  >>,
  events: ARRAY<STRUCT<
    `timestamp`: STRING,
    status: STRING,
    location: STRUCT<city: STRING, country: STRING>
  >>
>>,
fees MAP<STRING, STRUCT<amount: DOUBLE, taxable: BOOLEAN>>,
labels MAP<STRING, ARRAY<STRING>>,
audit_log ARRAY<MAP<STRING, STRING>>,
metadata MAP<STRING, STRING>
"""

# ---------------------------------------------------------------------------
# BUILDER HELPERS (only construct dicts)
# ---------------------------------------------------------------------------
AUTO = object()
T = lambda t: t  # payment amount == order total


def AD(t, city, state, ll=None, tags=None):
    return {"type": t, "city": city, "state": state,
            "geo": None if ll is None else {"latitude": ll[0], "longitude": ll[1]},
            "tags": tags}


def BN(bid, name, min_spend=None, cats=None):
    rules = None if (min_spend is None and cats is None) else {"min_spend": min_spend, "categories": cats}
    return {"benefit_id": bid, "name": name, "rules": rules}


def PF(cats, brands, chans, notif):
    return {"favorite_categories": cats, "preferred_brands": brands,
            "communication_channels": chans, "notification_settings": notif}


def LY(tier, points, benefits):
    return {"tier": tier, "points": points, "benefits": benefits}


def CU(cid, name, age, prefs, email, phone, addrs, loyalty):
    return {"customer_id": cid,
            "profile": {"name": name, "age": age, "preferences": prefs,
                        "contact": {"email": email, "phone": phone, "addresses": addrs}},
            "loyalty": loyalty}


def lp(c, points):
    c = copy.deepcopy(c)
    c["loyalty"]["points"] = points
    return c


CATALOG = {
    "P101": dict(name="UltraBook 14", category="Electronics", brand="NovaTech", price=1199.0,
                 attrs={"color": "Silver", "ram": "16GB", "storage": "512GB"},
                 supplier=("Nova Components Ltd", [("ISO9001", "2027-03-01"), ("RoHS", "2026-12-31")])),
    "P102": dict(name="Noise-Cancel Headphones", category="Electronics", brand="SonicWave", price=199.0,
                 attrs={"color": "Black", "bluetooth": "5.3", "anc": "true"},
                 supplier=("SonicWave Mfg", [("CE", "2028-01-15")])),
    "P103": dict(name="Trail Runner Shoes", category="Footwear", brand="StrideX", price=89.0,
                 attrs={"material": "mesh", "gender": "unisex"},
                 supplier=("StrideX Asia", [])),
    "P104": dict(name="Organic Cotton Tee", category="Apparel", brand="GreenLoom", price=24.0,
                 attrs={"material": "cotton", "fit": "regular"},
                 supplier=("GreenLoom Textiles", [("GOTS", "2026-11-30"), ("OEKO-TEX", "2027-06-30")])),
    "P105": dict(name="Stainless Water Bottle", category="Home & Kitchen", brand="HydroPeak", price=22.5,
                 attrs={}, supplier=None),
    "P106": dict(name="Smart Watch S2", category="Electronics", brand="NovaTech", price=249.0,
                 attrs={"color": "Graphite", "gps": "true", "strap": "silicone"},
                 supplier=("Nova Components Ltd", [("ISO9001", "2027-03-01")])),
    "P107": dict(name="Yoga Mat Pro", category="Sports", brand="FlexFit", price=45.0,
                 attrs={"thickness": "6mm", "material": "TPE"},
                 supplier=("FlexFit Co", [("REACH", "2027-02-28")])),
    "P108": dict(name="Espresso Maker", category="Home & Kitchen", brand="BrewMaster", price=129.0,
                 attrs={"pressure": "15bar", "color": "Red"},
                 supplier=("BrewMaster Italia", [("CE", "2027-09-09"), ("UL", "2026-10-01")])),
    "P109": dict(name="Hardcover Novel Bundle", category="Books", brand="PageTurn", price=24.99,
                 attrs={"language": "EN", "format": "hardcover"}, supplier=None),
    "P110": dict(name="Gaming Mouse G7", category="Electronics", brand="Zephyr", price=59.0,
                 attrs={"dpi": "16000", "wireless": "false"},
                 supplier=("Zephyr Peripherals", [("FCC", "2028-05-05")])),
    "P111": dict(name="Denim Jacket", category="Apparel", brand="Urban&Co", price=79.0,
                 attrs={"material": "denim", "fit": "slim"}, supplier=None),
    "P112": dict(name="Protein Powder 2kg", category="Health", brand="MuscleLab", price=54.0,
                 attrs={"flavor": "Chocolate", "vegan": "false"},
                 supplier=("MuscleLab Foods", [("GMP", "2026-12-01")])),
    "P113": dict(name="4K Monitor 27in", category="Electronics", brand="ViewMax", price=349.0,
                 attrs={"panel": "IPS", "refresh": "144Hz"},
                 supplier=("ViewMax Displays", [("TCO", "2027-04-04")])),
    "P114": dict(name="Scented Candle Set", category="Home & Kitchen", brand="Lumo", price=18.5,
                 attrs={"scent": "Lavender"}, supplier=None),
}


def product(pid, **ov):
    c = {**CATALOG[pid], **ov}
    sup = c["supplier"]
    if sup is not None:
        certs = None if sup[1] is None else [{"code": a, "expires": b} for a, b in sup[1]]
        sup = {"name": sup[0], "certifications": certs}
    return {"product_id": pid, "name": c["name"], "category": c["category"], "brand": c["brand"],
            "attributes": copy.deepcopy(c["attrs"]), "supplier": sup}


def V(vid, name, attrs):
    return {"variant_id": vid, "name": name, "attributes": attrs}


def D(dtype, code, amount, conds=None):
    return {"discount_type": dtype, "code": code, "amount": amount,
            "conditions": None if conds is None else [{"condition_type": a, "value": b} for a, b in conds]}


def item(pid, qty, variants=None, discounts=None, price=None, tax_rate=0.08, disc=AUTO, **ov):
    unit = CATALOG[pid]["price"] if price is None else price
    d = round(sum((x["amount"] for x in (discounts or [])), 0.0), 2) if disc is AUTO else disc
    base = qty * unit - (d or 0.0)
    tax = None if tax_rate is None else round(max(base, 0.0) * tax_rate, 2)
    return {"item_id": None, "product": product(pid, **ov), "quantity": qty,
            "pricing": {"unit_price": unit, "discount": d, "tax": tax},
            "variants": variants, "discounts": discounts}


def PAY(method, amount, status, meta=None, attempts=None):
    return {"payment_id": None, "method": method, "amount": amount, "status": status,
            "metadata": meta, "attempts": attempts}


def ATT(n, result, resp=None):
    return {"attempt_no": n, "result": result, "gateway_response": resp}


def PK(weight, dims, contents=None):
    return {"package_id": None, "weight": weight,
            "dimensions": None if dims is None else {"length": dims[0], "width": dims[1], "height": dims[2]},
            "contents": contents}  # list of (item_index_1_based, qty)


def EV(ts, status, city, country):
    return {"timestamp": ts, "status": status,
            "location": None if (city is None and country is None) else {"city": city, "country": country}}


def SH(carrier, tracking, packages, events):
    return {"shipment_id": None, "carrier": carrier, "tracking": tracking,
            "packages": packages, "events": events}


def F(amount, taxable):
    return {"amount": amount, "taxable": taxable}


def AU(ts, actor, action, **kw):
    return {"ts": ts, "actor": actor, "action": action, **kw}


def total(items, fees=None):
    t = 0.0
    for i in items:
        p = i["pricing"]
        t += i["quantity"] * p["unit_price"] - (p["discount"] or 0.0) + (p["tax"] or 0.0)
    for f in (fees or {}).values():
        t += f["amount"]
    return round(t, 2)


def ORD(n, date, status, customer, items, payments, shipments, metadata,
        fees=None, labels=None, audit=None):
    oid = f"ORD-{n}"
    for k, it in enumerate(items, 1):
        it["item_id"] = f"{oid}-I{k}"
    t = total(items, fees)
    for k, p in enumerate(payments or [], 1):
        p["payment_id"] = f"PAY-{n}-{k}"
        if callable(p["amount"]):
            p["amount"] = round(p["amount"](t), 2)
    for k, s in enumerate(shipments or [], 1):
        s["shipment_id"] = f"SHP-{n}-{k}"
        for j, pk in enumerate(s["packages"] or [], 1):
            pk["package_id"] = f"PKG-{n}-{k}-{j}"
            if pk["contents"] is not None:
                pk["contents"] = [{"item_id": items[i - 1]["item_id"], "qty": q} for i, q in pk["contents"]]
    return {"order_id": oid, "order_date": date, "order_status": status, "customer": customer,
            "items": items, "payments": payments, "shipments": shipments,
            "fees": fees, "labels": labels, "audit_log": audit, "metadata": metadata}


# ---------------------------------------------------------------------------
# CUSTOMERS
# ---------------------------------------------------------------------------
c1 = CU("C-2001", "Aanya Sharma", 34,
        PF(["Electronics", "Books"], ["NovaTech", "PageTurn"], ["email", "sms"],
           {"promo": ["email", "sms"], "order_updates": ["push"]}),
        "aanya.sharma@example.com", "+91-98765-43210",
        [AD("home", "Mumbai", "MH", (19.0760, 72.8777), ["primary", "apartment"]),
         AD("work", "pune", "MH", (18.5204, 73.8567), [])],
        LY("Gold", 5400, [BN("B-FS", "Free Shipping", 0.0, []), BN("B-EA", "Early Access", None, ["Electronics"])]))

c2 = CU("C-2002", "rahul verma", 29,
        PF(["Sports", "Health"], ["FlexFit", "MuscleLab"], ["push"], {}),
        "RAHUL.VERMA@EXAMPLE.COM", None,
        [AD("home", "Delhi", "DL", (28.7041, 77.1025), None)],
        LY("silver", 1200, []))

c3 = CU("C-2003", "Meera Iyer", 41,
        PF(["Home & Kitchen", "Apparel"], ["BrewMaster", "GreenLoom", "Lumo"], ["email"],
           {"promo": [], "order_updates": ["email", "sms"]}),
        "meera.iyer@example.com", "+91-99887-76655",
        [AD("home", "Chennai", "TN", (13.0827, 80.2707), ["primary"]),
         AD("billing", "Chennai", "TN", (13.0827, 80.2707), ["primary"]),
         AD("gift", "Bengaluru", "KA", None, ["gift", "office"])],
        LY("Platinum", 15250, [BN("B-FS", "Free Shipping", 0.0, []), BN("B-CS", "Concierge Support", None, None),
                               BN("B-DC", "Double Points", 250.0, ["Home & Kitchen", "Apparel"])]))

c4 = CU("C-2004", "Karan Malhotra", None, None, "karan.m@example.com", "+91-91234-56789", [], None)

c5 = CU("C-2005", "Sofia Rossi", 27,
        PF(["Footwear", "Sports"], ["StrideX"], [], None),
        "sofia.rossi@example.it", None,
        [AD("home", "Milan", "MI", (45.4642, 9.1900), ["primary"])],
        LY("Bronze", 320, [BN("B-FS", "Free Shipping", 75.0, [])]))

c6 = CU("C-2006", "Liam O'Connor", 52,
        PF(["Electronics", "Home & Kitchen", "Books"], ["ViewMax", "Zephyr", "NovaTech"], ["email", "sms", "push"],
           {"promo": ["push"], "order_updates": ["email", "push", "sms"], "security": ["email"]}),
        "liam.oconnor@example.ie", "+353-85-555-0101",
        [AD("home", "Dublin", "D", (53.3498, -6.2603), ["primary", "house"]),
         AD("work", "Cork", "CO", (51.8985, -8.4756), ["office"])],
        LY("Gold", 7800, [BN("B-FS", "Free Shipping", 0.0, []), BN("B-EA", "Early Access", None, ["Electronics", "Books"])]))

c7 = CU("C-2007", "Priya Nair", 23,
        PF(["Apparel", "Health"], ["GreenLoom", "MuscleLab"], ["sms"], {"promo": ["sms"]}),
        "priya.nair@example.com", "+91-90000-11122",
        [AD("home", "Kochi", "KL", (9.9312, 76.2673), ["primary"])],
        LY("Silver", 980, [BN("B-FS", "Free Shipping", 50.0, ["Apparel"])]))

c8 = CU("C-2008", "Chen Wei", 36,
        PF(["Electronics"], ["Zephyr", "NovaTech"], ["email"], {"order_updates": ["email"]}),
        "chen.wei@example.cn", "+86-138-0000-1111",
        [AD("home", "Shanghai", "SH", (31.2304, 121.4737), ["primary"]),
         AD("home", "Shanghai", "SH", (31.2304, 121.4737), ["primary"])],
        LY("GOLD", 6100, [BN("B-FS", "Free Shipping", 0.0, [])]))

c9 = CU("C-2009", "Fatima Khan", 31,
        PF(["Home & Kitchen", "Books"], ["PageTurn", "Lumo"], ["email", "push"],
           {"promo": ["email"], "order_updates": []}),
        "fatima.khan@example.ae", "+971-50-123-4567",
        [AD("home", "Dubai", "DU", (25.2048, 55.2708), ["primary", "apartment"]),
         AD("work", "Abu Dhabi", "AZ", (24.4539, 54.3773), ["office"])],
        LY("Silver", 1450, []))

c10 = CU("C-2010", "Noah Williams", 45,
         PF(["Sports", "Electronics", "Health"], ["FlexFit", "ViewMax"], ["email", "sms"],
            {"promo": ["email"], "order_updates": ["sms"]}),
         "noah.w@example.com", "+1-415-555-0133",
         [AD("home", "San Francisco", "CA", (37.7749, -122.4194), ["primary"]),
          AD("work", "Oakland", "CA", (37.8044, -122.2712), None),
          AD("vacation", "Lake Tahoe", "CA", (39.0968, -120.0324), ["seasonal"])],
         LY("Platinum", 22100, [BN("B-FS", "Free Shipping", 0.0, []),
                                BN("B-DP", "Double Points", 100.0, ["Sports", "Health"]),
                                BN("B-CS", "Concierge Support", None, None)]))

c11 = CU("C-2011", "Isabella García", 38,
         PF(["Apparel", "Footwear"], ["Urban&Co", "StrideX"], ["push", "email"], {"promo": ["push", "email"]}),
         "isabella.garcia@example.es", None,
         [AD("home", "Madrid", "M", (40.4168, -3.7038), ["primary"])],
         LY("Bronze", 150, []))

c12 = CU("C-2012", "Omar Haddad", None, PF([], [], [], {}), "omar.haddad@example.com", None, None, None)

c13 = CU("C-2013", "Yuki Tanaka", 30,
         PF(["Books", "Electronics"], ["PageTurn", "SonicWave"], ["email"], {"promo": ["email"]}),
         "yuki.tanaka@example.jp", "+81-90-1234-5678",
         [AD("home", "Tokyo", "TK", (35.6762, 139.6503), ["primary"]),
          AD("work", "Yokohama", "KN", (35.4437, 139.6380), [])],
         LY("Silver", 2050, [BN("B-FS", "Free Shipping", 30.0, ["Books"])]))

c14 = CU("C-2014", "Emma Johansson", 26,
         PF(["Home & Kitchen"], ["HydroPeak", "Lumo"], ["email", "sms"], {"promo": [], "order_updates": ["email"]}),
         "emma.j@example.se", "+46-70-123-4567",
         [AD("home", "Stockholm", "AB", (59.3293, 18.0686), ["primary"])],
         LY("Bronze", 410, [BN("B-FS", "Free Shipping", 75.0, None)]))

c15 = CU("C-2015", "Arjun Reddy", 33,
         PF(["Electronics", "Sports"], ["NovaTech", "FlexFit"], ["push"],
            {"promo": ["push"], "order_updates": ["push", "email"]}),
         "arjun.reddy@example.in", "+91-98480-22334",
         [AD("home", "Hyderabad", "TS", (17.3850, 78.4867), ["primary"]),
          AD("parents", "Warangal", "TS", None, ["family"])],
         LY("gold", 5900, [BN("B-FS", "Free Shipping", 0.0, []), BN("B-EA", "Early Access", None, ["Electronics"])]))

c16 = CU("C-2016", "Hannah Müller", 47,
         PF(["Books", "Home & Kitchen", "Health"], ["PageTurn", "BrewMaster", "MuscleLab"], ["email"],
            {"promo": ["email"], "order_updates": ["email"]}),
         "hannah.mueller@example.de", "+49-151-2345-6789",
         [AD("home", "Berlin", "BE", (52.5200, 13.4050), ["primary"]),
          AD("billing", "Munich", "BY", (48.1351, 11.5820), None)],
         LY("Silver", 1700, []))

# reusable variants
V_UB = V("V-101-S16", "Silver / 16GB", {"color": "Silver", "ram": "16GB"})
V_MON = V("V-113-27", "27in", {"size": "27in"})
V_WGR = V("V-106-GR", "Graphite / 44mm", {"color": "Graphite", "size": "44mm"})
V_MBK = V("V-110-BK", "Black", {"color": "Black"})
V_HBK = V("V-102-BK", "Black", {"color": "Black"})
V_CLV = V("V-114-LV", "Lavender", {"scent": "Lavender"})
V_CVN = V("V-114-VN", "Vanilla", {"scent": "Vanilla"})
V_ERD = V("V-108-RD", "Red", {"color": "Red"})
V_S42 = V("V-103-42", "EU 42", {"size": "42", "color": "Orange"})


def _o1007(late):
    return ORD(
        1007, "2026-09-04", "shipped" if late else "Pending", c7,
        [item("P104", 3, [V("V-104-BK-S", "Black / S", {"color": "Black", "size": "S"}),
                          V("V-104-BK-M", "Black / M", {"color": "Black", "size": "M"})],
              [D("BULK", "TEE3", 9.0, [("min_quantity", "3")])]),
         item("P112", 1)],
        [PAY("upi", T, "captured" if late else "pending", {"vpa": "priya@oksbi"}, [])],
        ([SH("Ecom Express", "EE42001", [PK(1.4, (30.0, 25.0, 12.0), [(1, 3), (2, 1)])],
             [EV("2026-09-05T09:00:00Z", "picked_up", "Kochi", "IN")])] if late else []),
        {"source": "app", "device": "android", "ingest_batch": "B-0905-2" if late else "B-0904-1"},
        fees={"shipping": F(2.99, False)}, labels={},
        audit=[AU("2026-09-04T09:00:00Z", "system", "created")]
              + ([AU("2026-09-04T09:05:00Z", "gateway", "paid"), AU("2026-09-05T08:00:00Z", "wms", "shipped")] if late else []))


# ---------------------------------------------------------------------------
# ORDERS
# ---------------------------------------------------------------------------
orders = [
    ORD(1001, "2026-09-01", "DELIVERED", c1,
        [item("P101", 1, [V_UB, V("V-101-S16-US", "US keyboard layout", {"layout": "US"})],
              [D("PERCENT", "FALL10", 119.9, [("min_cart_value", "1000"), ("category", "Electronics")]),
               D("COUPON", "WELCOMEBACK", 50.0, [("customer_tier", "Gold")])]),
         item("P102", 2, [V_HBK], [])],
        [PAY("card", T, "captured", {"network": "visa", "last4": "4242", "3ds": "true"},
             [ATT(1, "approved", {"code": "00", "avs": "Y"})])],
        [SH("BlueDart", "BD778899",
            [PK(2.4, (40.0, 30.0, 8.0), [(1, 1)]), PK(0.9, (25.0, 20.0, 10.0), [(2, 2)])],
            [EV("2026-09-02T08:00:00Z", "picked_up", "Mumbai", "IN"),
             EV("2026-09-03T14:30:00Z", "in_transit", "Nagpur", "IN"),
             EV("2026-09-05T11:10:00Z", "delivered", "Pune", "IN")])],
        {"source": "web", "campaign": "FALL26", "device": "ios"},
        fees={"shipping": F(0.0, False), "gift_wrap": F(4.5, True)},
        labels={"warehouse": ["WH-WEST"], "fraud_flags": []},
        audit=[AU("2026-09-01T10:00:00Z", "system", "created"), AU("2026-09-01T10:01:10Z", "gateway", "paid"),
               AU("2026-09-02T08:00:00Z", "wms", "shipped", carrier="BlueDart"),
               AU("2026-09-05T11:10:00Z", "carrier", "delivered")]),

    ORD(1002, "2026-09-01", "delivered", c2,
        [item("P107", 1, [V("V-107-BL", "Blue / 6mm", {"color": "Blue", "thickness": "6mm"})], None),
         item("P112", 3, [], [D("BULK", "PROTEIN3", 30.0, [("min_quantity", "3")])]),
         item("P105", 1, None, [])],
        [PAY("upi", T, "captured", {"vpa": "rahul@okbank"}, None)],
        [SH("Delhivery", "DL5512", [PK(3.2, (35.0, 25.0, 20.0), [(1, 1), (2, 3), (3, 1)])],
            [EV("2026-09-02T09:00:00Z", "picked_up", "Delhi", "IN"),
             EV("2026-09-04T16:45:00Z", "delivered", "Delhi", "IN")])],
        {"source": "app", "device": "android"},
        fees={"shipping": F(3.99, False)}, labels={},
        audit=[AU("2026-09-01T11:00:00Z", "system", "created"), AU("2026-09-04T16:45:00Z", "carrier", "delivered")]),

    ORD(1003, "2026-09-02", "Shipped", c3,
        [item("P108", 1, [V_ERD, V("V-108-EU", "EU plug", {"plug": "EU"})],
              [D("FIXED", "COFFEE25", 25.0, [("category", "Home & Kitchen")]),
               D("LOYALTY", "PLAT5", 15.0, [("customer_tier", "Platinum"), ("min_points", "10000")]),
               D("PERCENT", "BUNDLE5", 19.5, None)]),
         item("P114", 4, [V_CLV, V_CVN], [D("BULK", "CANDLE4", 8.0, [("min_quantity", "4")])],
              supplier=("Lumo Wax Co", None)),
         item("P104", 2, [V("V-104-WH-M", "White / M", {"color": "White", "size": "M"})], None, category="apparel")],
        [PAY("card", lambda t: round(t - 100.0, 2), "captured", {"network": "mastercard", "last4": "0007"},
             [ATT(1, "declined", {"code": "05"}), ATT(2, "approved", {"code": "00"})]),
         PAY("gift_card", 100.0, "captured", {"card_no": "GC-8891", "issuer": "HomeStore"}, None)],
        [SH("DHL", "DHL90123",
            [PK(1.8, (30.0, 30.0, 25.0), [(1, 1)]), PK(1.1, (22.0, 18.0, 12.0), [(2, 4), (3, 2)])],
            [EV("2026-09-03T10:00:00Z", "picked_up", "Chennai", "IN"),
             EV("2026-09-04T09:20:00Z", "in_transit", "Bengaluru", "IN")])],
        {"source": "web", "campaign": "HOME-FEST", "gift": "true"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-SOUTH"]},
        audit=[AU("2026-09-02T12:00:00Z", "system", "created"), AU("2026-09-03T10:00:00Z", "wms", "shipped")]),

    ORD(1004, "2026-09-02", "Pending", c4,
        [item("P110", 1, None, None, tax_rate=None, attrs=None)],
        [PAY("cod", T, "pending", None, None)],
        [], {}, ),

    ORD(1005, "2026-09-03", "CANCELLED", c5,
        [item("P103", 0, [V_S42], []),
         item("P107", 1, None, None)],
        [PAY("card", T, "refunded", {"network": "visa", "last4": "1881", "refund_id": "RF-77"},
             [ATT(1, "approved", None)])],
        [], {"source": "web", "cancel_reason": "changed_mind"},
        fees={}, labels={"fraud_flags": ["address_mismatch"]},
        audit=[AU("2026-09-03T08:00:00Z", "system", "created"),
               AU("2026-09-03T09:30:00Z", "customer", "cancelled", reason="customer_request")]),

    ORD(1006, "2026-09-03", "SHIPPED", c6,
        [item("P113", 2, [V_MON], [D("PERCENT", "VIEW15", 104.7, [("min_quantity", "2"), ("category", "Electronics")])]),
         item("P110", 1, [V_MBK], None),
         item("P106", 1, [V_WGR, V("V-106-SL", "Silicone strap", {"strap": "silicone"})],
              [D("FIXED", "WATCH30", 30.0, [])]),
         item("P109", 2, None, []),
         item("P101", 1, [V_UB],
              [D("PERCENT", "EDU10", 119.9, [("verification", "student"), ("email_domain", "edu")]),
               D("FIXED", "SHIPFREE", 0.0, [("region", "IE")])])],
        [PAY("card", 1500.0, "captured", {"network": "amex", "last4": "1005"}, [ATT(1, "approved", {"code": "00"})]),
         PAY("paypal", lambda t: round(t - 1500.0, 2), "captured", {"payer_id": "PP-5521", "country": "IE"}, None)],
        [SH("UPS", "1Z999AA10123456784",
            [PK(11.5, (70.0, 45.0, 25.0), [(1, 2)]), PK(2.2, (30.0, 25.0, 15.0), [(2, 1), (3, 1), (4, 2)])],
            [EV("2026-09-04T07:30:00Z", "picked_up", "Dublin", "IE"),
             EV("2026-09-05T13:00:00Z", "in_transit", "Cork", "IE")]),
         SH("DHL", None, [PK(2.4, (40.0, 30.0, 8.0), [(5, 1)])], [])],
        {"source": "web", "device": "desktop", "campaign": None},
        fees={"shipping": F(14.9, False), "insurance": F(9.5, False)},
        labels={"warehouse": ["WH-EU1", "WH-EU2"], "fraud_flags": ["high_value"]},
        audit=[AU("2026-09-03T14:00:00Z", "system", "created"), AU("2026-09-03T14:02:00Z", "gateway", "paid"),
               AU("2026-09-03T15:00:00Z", "fraud", "reviewed", result="pass"),
               AU("2026-09-04T07:30:00Z", "wms", "shipped", carrier="UPS")]),

    _o1007(False),

    ORD(1008, "2026-09-05", "delivered", c8,
        [item("P110", 2, [V_MBK, V("V-110-WH", "White", {"color": "White"})],
              [D("FIXED", "MOUSE10", 10.0, [("min_quantity", "2")]),
               D("FIXED", "MOUSE10", 10.0, [("min_quantity", "2")])])],
        [PAY("alipay", T, "captured", {"account": "cw***@example.cn"}, [ATT(1, "approved", {"code": "OK"})])],
        [SH("SF Express", "SF1234567", [PK(0.6, (18.0, 12.0, 6.0), [(1, 2)])],
            [EV("2026-09-06T07:00:00Z", "picked_up", "Shanghai", "CN"),
             EV("2026-09-07T12:00:00Z", "delivered", "Shanghai", "CN")])],
        {"source": "app", "device": "ios"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-CN1"]}, audit=[]),

    ORD(1009, "2026-09-05", "Shipped", c9,
        [item("P108", 1, [V("V-108-BK", "Black", {"color": "Black"})],
              [D("PERCENT", "RAMADAN10", 12.9, [("campaign", "RAMADAN"), ("category", "Home & Kitchen")])], disc=18.0),
         item("P109", 3, None, None, disc=None),
         item("P114", 2, [V_CLV], [])],
        [PAY("card", T, "captured", {"network": "visa", "last4": "7788"}, [ATT(1, "approved", {"code": "00"})])],
        [SH("Aramex", "ARX55001",
            [PK(2.9, (32.0, 28.0, 22.0), [(1, 1)]), PK(1.5, (25.0, 20.0, 15.0), [(2, 3), (3, 2)])],
            [EV("2026-09-06T05:30:00Z", "picked_up", "Dubai", "AE"),
             EV("2026-09-06T15:00:00Z", "out_for_delivery", "Abu Dhabi", "AE")])],
        {"source": "web", "device": "desktop"},
        fees={"shipping": F(6.0, False), "gift_wrap": F(0.0, True)}, labels={"warehouse": ["WH-GCC"]},
        audit=[AU("2026-09-05T18:00:00Z", "system", "created"), AU("2026-09-06T05:30:00Z", "wms", "shipped")]),

    ORD(1010, "2026-09-06", "DELIVERED", c10,
        [item("P107", 2, [V("V-107-GN", "Green / 6mm", {"color": "Green", "thickness": "6mm"})],
              [D("PERCENT", "FIT10", 9.0, [("category", "Sports")]),
               D("LOYALTY", "PLAT-EXTRA", 5.0, [("customer_tier", "Platinum")])]),
         item("P112", 2, None,
              [D("BULK", "PROTEIN2", 15.0, [("min_quantity", "2"), ("category", "Health"), ("brand", "MuscleLab")])]),
         item("P113", 1, [V_MON], None),
         item("P102", 1, [V("V-102-WH", "White", {"color": "White"}),
                          V("V-102-WH-CASE", "Hard case", {"accessory": "case"})],
              [D("FIXED", "MUSIC20", 20.0, None)])],
        [PAY("card", lambda t: round(t - 200.0, 2), "captured",
             {"network": "visa", "last4": "3030", "wallet": "apple_pay"}, [ATT(1, "approved", {"code": "00", "avs": "Y"})]),
         PAY("gift_card", 150.0, "captured", {"card_no": "GC-1203"}, None),
         PAY("store_credit", 50.0, "captured", {}, None)],
        [SH("FedEx", "FX7001",
            [PK(1.4, (60.0, 20.0, 6.0), [(1, 2)]), PK(2.6, (30.0, 25.0, 20.0), [(2, 2)])],
            [EV("2026-09-07T03:00:00Z", "picked_up", "Oakland", "US"),
             EV("2026-09-08T10:00:00Z", "in_transit", "Sacramento", "US"),
             EV("2026-09-09T18:00:00Z", "delivered", "San Francisco", "US")]),
         SH("USPS", "9400111899223344556677",
            [PK(0.5, (20.0, 15.0, 8.0), [(4, 1)]), PK(8.9, (75.0, 48.0, 18.0), [(3, 1)])],
            [EV("2026-09-07T04:00:00Z", "picked_up", "Oakland", "US"),
             EV("2026-09-10T09:00:00Z", "delivered", "San Francisco", "US")])],
        {"source": "web", "device": "desktop", "campaign": "LABOR-DAY"},
        fees={"shipping": F(0.0, False), "expedite": F(12.0, True)},
        labels={"warehouse": ["WH-US-W", "WH-US-W2"], "fraud_flags": []},
        audit=[AU("2026-09-06T09:00:00Z", "system", "created"), AU("2026-09-06T09:01:00Z", "gateway", "paid"),
               AU("2026-09-07T03:00:00Z", "wms", "shipped"), AU("2026-09-10T09:00:00Z", "carrier", "delivered")]),

    ORD(1011, "2026-09-06", "returned", c11,
        [item("P111", 1, [V("V-111-M", "M / Indigo", {"size": "M", "color": "Indigo"})], None),
         item("P103", 1, [V("V-103-38", "EU 38", {"size": "38", "color": "White"}),
                          V("V-103-39", "EU 39", {"size": "39", "color": "White"})],
              [D("PERCENT", "RUN15", 13.35, [("category", "Footwear")])])],
        [PAY("card", T, "refunded", {"network": "mastercard", "last4": "5599", "refund_id": "RF-90"},
             [ATT(1, "approved", {"code": "00"})])],
        [SH("Correos", "CO88221", [PK(1.3, (35.0, 25.0, 10.0), [(1, 1), (2, 1)])],
            [EV("2026-09-07T08:00:00Z", "picked_up", "Madrid", "ES"),
             EV("2026-09-09T13:00:00Z", "delivered", "Madrid", "ES"),
             EV("2026-09-12T10:00:00Z", "return_initiated", "Madrid", "ES"),
             EV("2026-09-14T16:00:00Z", "returned_to_warehouse", "Valencia", "ES")])],
        {"source": "app", "device": "ios", "return_reason": "wrong_size"},
        fees={"shipping": F(4.5, False)}, labels={"warehouse": ["WH-ES1"]},
        audit=[AU("2026-09-06T20:00:00Z", "system", "created"), AU("2026-09-12T10:00:00Z", "customer", "return_requested")]),

    ORD(1012, "2026-09-07", "Pending", c12,
        [item("P105", 2, [], None),
         item("P114", 1, None, []),
         item("P109", 0, [], None)],
        [PAY("cod", T, "pending", None, None)],
        None, {}, fees=None, labels=None, audit=[]),

    ORD(1013, "2026-09-07", "Delivered", lp(c1, 5600),
        [item("P106", 1, [V("V-106-SV", "Silver / 40mm", {"color": "Silver", "size": "40mm"})],
              [D("PERCENT", "WATCH10", 24.9, [("category", "Electronics"), ("customer_tier", "Gold")])]),
         item("P109", 5, None, [D("BULK", "BOOKS5", 12.0, [("min_quantity", "5")]),
                                D("FIXED", "READMORE", 3.0, [("category", "Books")])])],
        [PAY("card", T, "captured", {"network": "visa", "last4": "4242"}, [ATT(1, "approved", {"code": "00"})])],
        [SH("BlueDart", "BD778901",
            [PK(0.3, (15.0, 12.0, 6.0), [(1, 1)]), PK(3.0, (32.0, 24.0, 18.0), [(2, 5)])],
            [EV("2026-09-08T08:00:00Z", "picked_up", "Mumbai", "IN"),
             EV("2026-09-09T12:00:00Z", "delivered", "Mumbai", "IN")])],
        {"source": "web", "device": "ios"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-WEST"]},
        audit=[AU("2026-09-07T10:00:00Z", "system", "created"), AU("2026-09-09T12:00:00Z", "carrier", "delivered")]),

    ORD(1014, "2026-09-08", "Shipped ", c13,
        [item("P109", 3, [V("V-109-EN", "English / Hardcover", {"language": "EN", "format": "hardcover"})],
              [D("BULK", "BOOKS3", 6.0, [("min_quantity", "3")])]),
         item("P102", 1, [V_HBK], None)],
        [PAY("card", T, "captured", {"network": "jcb", "last4": "6611"}, [ATT(1, "approved", {"code": "00"})])],
        [SH("Yamato", "YM3300", [PK(2.2, (30.0, 22.0, 14.0), [(1, 3), (2, 1)])],
            [EV("2026-09-09T06:00:00Z", "picked_up", "Tokyo", "JP"),
             EV("2026-09-10T08:00:00Z", "in_transit", "Yokohama", "JP")])],
        {"source": "app", "device": "android"},
        fees={"shipping": F(5.0, False)}, labels={"warehouse": ["WH-JP1"]}, audit=None),

    ORD(1015, "2026-09-08", "partially_shipped", c14,
        [item("P105", 2, [V("V-105-BL", "Blue / 750ml", {"color": "Blue", "volume": "750ml"})], None),
         item("P114", 3, [V_CLV, V_CVN], [D("BULK", "CANDLE3", 6.0, [("min_quantity", "3")])]),
         item("P108", 1, None, [D("PERCENT", "BREW10", 12.9, [("category", "Home & Kitchen")])])],
        [PAY("card", T, "captured", {"network": "visa", "last4": "2468"}, [ATT(1, "approved", {"code": "00"})])],
        [SH("PostNord", "PN1100", [PK(1.0, (28.0, 10.0, 10.0), [(1, 2)])],
            [EV("2026-09-09T07:00:00Z", "picked_up", "Stockholm", "SE"),
             EV("2026-09-10T12:00:00Z", "in_transit", "Uppsala", "SE")]),
         SH("PostNord", None, [PK(2.7, (30.0, 28.0, 24.0), [(3, 1)]), PK(None, None, [(2, 3)])], None)],
        {"source": "web", "device": "desktop"},
        fees={"shipping": F(7.5, False)}, labels={"warehouse": ["WH-SE1"], "backorder": ["P114"]}, audit=[]),

    ORD(1016, "2026-09-09", "DELIVERED", c15,
        [item("P101", 1, [V("V-101-G32", "Graphite / 32GB", {"color": "Graphite", "ram": "32GB"})],
              [D("PERCENT", "TECH10", 119.9, [("category", "Electronics"), ("min_cart_value", "1000")]),
               D("CASHBACK", "UPI5", 25.0, [("payment_method", "upi")])]),
         item("P107", 1, None, []),
         item("P112", 2, [V("V-112-CH", "Chocolate 2kg", {"flavor": "Chocolate"})], None)],
        [PAY("upi", T, "captured", {"vpa": "arjun@okaxis"},
             [ATT(1, "failed", {"code": "U16"}), ATT(2, "approved", {"code": "00"})])],
        [SH("Delhivery", "DL7799",
            [PK(2.4, (40.0, 30.0, 8.0), [(1, 1)]), PK(4.1, (35.0, 25.0, 20.0), [(2, 1), (3, 2)])],
            [EV("2026-09-10T09:00:00Z", "picked_up", "Hyderabad", "IN"),
             EV("2026-09-11T15:00:00Z", "delivered", "Hyderabad", "IN")])],
        {"source": "app", "device": "android", "campaign": "UPI-CASHBACK"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-SOUTH"]},
        audit=[AU("2026-09-09T19:00:00Z", "system", "created"), AU("2026-09-11T15:00:00Z", "carrier", "delivered")]),

    ORD(1017, "2026-09-10", "Delivered", lp(c3, 15900),
        [item("P104", 4, [V("V-104-WH-S", "White / S", {"color": "White", "size": "S"}),
                          V("V-104-WH-M", "White / M", {"color": "White", "size": "M"}),
                          V("V-104-WH-L", "White / L", {"color": "White", "size": "L"})],
              [D("BULK", "TEE4", 12.0, [("min_quantity", "4")]),
               D("LOYALTY", "PLAT5", 8.0, [("customer_tier", "Platinum")])]),
         item("P111", 1, [V("V-111-L", "L / Black", {"size": "L", "color": "Black"})], None)],
        [PAY("card", T, "captured", {"network": "mastercard", "last4": "0007"}, [ATT(1, "approved", None)])],
        [SH("DHL", "DHL90150", [PK(1.6, (35.0, 28.0, 12.0), [(1, 4), (2, 1)])],
            [EV("2026-09-11T08:00:00Z", "picked_up", "Chennai", "IN"),
             EV("2026-09-12T10:00:00Z", "delivered", "Chennai", "IN")])],
        {"source": "web", "device": "desktop"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-SOUTH"]}, audit=[]),

    ORD(1018, "2026-09-10", "Shipped", c16,
        [item("P108", 1, [V_ERD], [D("PERCENT", "BREW10", 12.9, [("category", "Home & Kitchen")])]),
         item("P109", 2, None, None),
         item("P112", 1, None, None)],
        [PAY("sepa", T, "captured", {"iban_last4": "3201", "mandate": "M-8812"}, [ATT(1, "approved", {"code": "ACSC"})])],
        [SH("DHL", "DHL-DE-3301", [PK(3.1, (32.0, 28.0, 22.0), [(1, 1), (2, 2), (3, 1)])],
            [EV("2026-09-11T09:00:00Z", "picked_up", "Berlin", "DE"),
             EV("2026-09-12T11:00:00Z", "in_transit", "Leipzig", "DE")])],
        {"source": "web", "device": "desktop"},
        fees={"shipping": F(6.9, False)}, labels={"warehouse": ["WH-DE1"]}, audit=None),

    ORD(1019, "2026-09-11", "DELIVERED", lp(c6, 8600),
        [item("P113", 1, [V_MON], [D("FIXED", "MON50", 50.0, [("category", "Electronics")])]),
         item("P106", 1, [V_WGR], None)],
        [PAY("card", T, "captured", {"network": "amex", "last4": "1005"}, [ATT(1, "approved", {"code": "00"})])],
        [SH("UPS", "1Z999AA10123456999",
            [PK(8.8, (75.0, 48.0, 18.0), [(1, 1)]), PK(0.4, (15.0, 12.0, 6.0), [(2, 1)])],
            [EV("2026-09-12T07:00:00Z", "picked_up", "Dublin", "IE"),
             EV("2026-09-13T12:00:00Z", "delivered", "Dublin", "IE")])],
        {"source": "web", "device": "desktop"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-EU1"], "fraud_flags": []}, audit=[]),

    ORD(1020, "2026-09-12", "cancelled", c2,
        [item("P103", 2, [V_S42, V("V-103-43", "EU 43", {"size": "43", "color": "Orange"})], None)],
        [PAY("upi", T, "failed", {"vpa": "rahul@okbank"}, [ATT(1, "failed", {"code": "U30"})]),
         PAY("upi", T, "failed", {"vpa": "rahul@okbank"},
             [ATT(1, "failed", {"code": "U30"}), ATT(2, "failed", {"code": "U30"})])],
        [], {"source": "app", "device": "android", "cancel_reason": "payment_failed"},
        fees={"shipping": F(3.99, False)}, labels={"fraud_flags": ["repeated_failures"]},
        audit=[AU("2026-09-12T20:00:00Z", "system", "created"), AU("2026-09-12T20:20:00Z", "system", "cancelled",
                                                                    reason="payment_failed")]),

    ORD(1021, "2026-09-12", "SHIPPED", c8,
        [item("P113", 2, [V_MON], [D("PERCENT", "VIEW10", 69.8, [("min_quantity", "2")]),
                                    D("COUPON", "NEWMONTH", 20.0, [("region", "CN"), ("customer_tier", "Gold")])]),
         item("P101", 1, [V_UB], None),
         item("P102", 1, [V_HBK], [])],
        [PAY("alipay", T, "captured", {"account": "cw***@example.cn"}, [ATT(1, "approved", {"code": "OK"})])],
        [SH("SF Express", "SF1234600",
            [PK(11.0, (70.0, 45.0, 25.0), [(1, 2)]), PK(2.4, (40.0, 30.0, 8.0), [(2, 1)]),
             PK(0.9, (25.0, 20.0, 10.0), [(3, 1)])],
            [EV("2026-09-13T06:00:00Z", "picked_up", "Shanghai", "CN"),
             EV("2026-09-14T10:00:00Z", "in_transit", "Nanjing", "CN"),
             EV("2026-09-14T22:00:00Z", "in_transit", "Hefei", "CN")])],
        {"source": "app", "device": "ios"},
        fees={"shipping": F(0.0, False), "insurance": F(6.0, False)},
        labels={"warehouse": ["WH-CN1"], "fraud_flags": []}, audit=[]),

    ORD(1022, "2026-09-13", "pending", lp(c1, 6000),
        [item("P109", 1, None, None)],
        [PAY("card", T, "pending", {"network": "visa", "last4": "4242"}, None)],
        [], {"source": "web"}, fees={}, labels={}, audit=None),

    ORD(1023, "2026-09-14", "Delivered", c9,
        [item("P114", 6, [V_CLV, V_CVN, V("V-114-SW", "Sandalwood", {"scent": "Sandalwood"})],
              [D("BULK", "CANDLE6", 15.0, [("min_quantity", "6")]), D("FIXED", "GCC5", 5.0, [("region", "AE")])]),
         item("P105", 2, None, None),
         item("P109", 1, None, None)],
        [PAY("card", T, "captured", {"network": "visa", "last4": "7788"}, [ATT(1, "approved", {"code": "00"})])],
        [SH("Aramex", "ARX55077",
            [PK(3.4, (30.0, 28.0, 24.0), [(1, 6)]), PK(1.2, (28.0, 10.0, 10.0), [(2, 2), (3, 1)])],
            [EV("2026-09-15T06:00:00Z", "picked_up", "Dubai", "AE"),
             EV("2026-09-15T17:00:00Z", "delivered", "Dubai", "AE")])],
        {"source": "web", "device": "desktop"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-GCC"]}, audit=[]),

    ORD(1024, "2026-09-14", "DELIVERED", lp(c10, 22900),
        [item("P113", 3, [V_MON], [D("PERCENT", "OFFICE12", 125.64, [("min_quantity", "3"), ("customer_tier", "Platinum")])]),
         item("P110", 3, [V_MBK], [D("BULK", "MOUSE3", 15.0, [("min_quantity", "3")])]),
         item("P102", 1, None, None),
         item("P106", 1, [V_WGR], [D("FIXED", "WATCH30", 30.0, [])]),
         item("P101", 1, [V_UB, V("V-101-S16-UK", "UK keyboard layout", {"layout": "UK"})],
              [D("PERCENT", "TECH10", 119.9, [("category", "Electronics"), ("min_cart_value", "1000")])])],
        [PAY("card", lambda t: round(t - 500.0, 2), "captured",
             {"network": "visa", "last4": "3030", "wallet": "apple_pay"}, [ATT(1, "approved", {"code": "00"})]),
         PAY("gift_card", 500.0, "captured", {"card_no": "GC-2211"}, None)],
        [SH("FedEx", "FX7100",
            [PK(25.0, (70.0, 50.0, 40.0), [(1, 3)]), PK(2.0, (25.0, 20.0, 10.0), [(2, 3)])],
            [EV("2026-09-15T03:00:00Z", "picked_up", "Oakland", "US"),
             EV("2026-09-16T10:00:00Z", "in_transit", "Reno", "US"),
             EV("2026-09-17T18:00:00Z", "delivered", "San Francisco", "US")]),
         SH("FedEx", "FX7101",
            [PK(0.9, (25.0, 20.0, 10.0), [(3, 1)]), PK(0.4, (15.0, 12.0, 6.0), [(4, 1)]),
             PK(2.4, (40.0, 30.0, 8.0), [(5, 1)])],
            [EV("2026-09-15T03:30:00Z", "picked_up", "Oakland", "US"),
             EV("2026-09-16T11:00:00Z", "delivered", "San Francisco", "US")])],
        {"source": "web", "device": "desktop", "campaign": "OFFICE-SETUP"},
        fees={"shipping": F(0.0, False), "expedite": F(15.0, True)},
        labels={"warehouse": ["WH-US-W"], "fraud_flags": []},
        audit=[AU("2026-09-14T09:00:00Z", "system", "created"), AU("2026-09-15T03:00:00Z", "wms", "shipped"),
               AU("2026-09-17T18:00:00Z", "carrier", "delivered")]),

    ORD(1025, "2026-09-15", "Shipped", c15,
        [item("P106", 2, [V_WGR, V("V-106-SV", "Silver / 40mm", {"color": "Silver", "size": "40mm"})],
              [D("PERCENT", "WATCH10", 49.8, [("category", "Electronics")])]),
         item("P107", 2, None, [])],
        [PAY("upi", T, "captured", {"vpa": "arjun@okaxis"}, [ATT(1, "approved", {"code": "00"})])],
        [SH("Delhivery", "DL7810",
            [PK(0.8, (20.0, 15.0, 8.0), [(1, 2)]), PK(2.4, (35.0, 25.0, 10.0), [(2, 2)])],
            [EV("2026-09-16T09:00:00Z", "picked_up", "Hyderabad", "IN"),
             EV("2026-09-17T07:00:00Z", "in_transit", "Vijayawada", "IN")])],
        {"source": "app", "device": "android"},
        fees={"shipping": F(0.0, False)}, labels={"warehouse": ["WH-SOUTH"]}, audit=[]),

    # late-arriving update for order 1007 (same order_id, different state)
    _o1007(True),
]


# ---------------------------------------------------------------------------
# HELPERS FOR LOADING
# ---------------------------------------------------------------------------
def get_df(spark):
    """Return the nested DataFrame (structure preserved, nothing stringified)."""
    return spark.createDataFrame(orders, schema=SCHEMA_DDL)


def write_json(path="orders.json", lines=False):
    with open(path, "w", encoding="utf-8") as f:
        if lines:
            for o in orders:
                f.write(json.dumps(o, ensure_ascii=False) + "\n")
        else:
            json.dump(orders, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").appName("nested_orders").getOrCreate()
    df = get_df(spark)
    df.printSchema()
    print("records:", df.count())
    df.createOrReplaceTempView("orders")
