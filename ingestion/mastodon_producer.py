import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from mastodon import Mastodon, MastodonError

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "mastodon_stream")
API_BASE_URL = os.environ.get("MASTODON_API_BASE_URL", "https://mastodon.social")
ACCESS_TOKEN = os.environ.get("MASTODON_ACCESS_TOKEN", "")

HASHTAGS = [h.strip().lstrip("#").lower() for h in os.environ.get("HASHTAGS", "DataScience,AI").split(",") if h.strip()]
KEYWORDS = [k.strip().lower() for k in os.environ.get("KEYWORDS", "").split(",") if k.strip()]

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "15"))  
BACKFILL_PAGES = int(os.environ.get("BACKFILL_PAGES", "0"))   
PAGE_LIMIT = 40  #max autorisé

def log(msg):
    print(f"[mastodon-producer] {msg}", flush=True)

#nettoyage de toot

TAG_RE = re.compile(r"<[^>]+>")
SPACES_RE = re.compile(r"\s+")

def html_to_text(content):
    # transferer le contenu dun toot qui est en html et on garde le text
    if not content:
        return ""
    text = re.sub(r"<br\s*/?>|</p>", " ", content)
    text = TAG_RE.sub("", text)
    text = html.unescape(text)
    return SPACES_RE.sub(" ", text).strip()


def to_iso(dt):
    #Mastodon.py renvoie des datetime on envoie de l'ISO 8601 en UTC dans le JSON
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return str(dt) if dt else None

def parse_toot(status, source):
    account = status.get("account") or {}
    acct = account.get("acct", "")
    instance = acct.split("@", 1)[1] if "@" in acct else API_BASE_URL.replace("https://", "").replace("http://", "")
    return {
        "toot_id": str(status["id"]),
        "created_at": to_iso(status.get("created_at")),
        "user_id": str(account.get("id", "")),
        "username": account.get("username", ""),
        "acct": acct,
        "instance": instance,
        "followers_count": account.get("followers_count", 0),
        "text": html_to_text(status.get("content", "")),
        "language": status.get("language"),
        "hashtags": [t["name"].lower() for t in (status.get("tags") or []) if t.get("name")],
        "favourites_count": status.get("favourites_count", 0),
        "reblogs_count": status.get("reblogs_count", 0),
        "replies_count": status.get("replies_count", 0),
        "url": status.get("url"),
        # Mastodon ne donne pas de geolocolisation
        "location": None,
        "source": source,  #source dou vient le toot
        "ingested_at": to_iso(datetime.now(timezone.utc)),
    }

def matches_keywords(toot):
    text = toot["text"].lower()
    return any(k in text for k in KEYWORDS) or any(k.lstrip("#") in toot["hashtags"] for k in KEYWORDS)


#connexions

def wait_for_kafka_and_topic():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="mastodon-admin")
            break
        except Exception:  #kafka sactualise
            log(f"Kafka pas encore dispo sur {BOOTSTRAP}, on réessaie dans 3 s...")
            time.sleep(3)
    while TOPIC not in admin.list_topics():
        log(f"Le topic '{TOPIC}' n'existe pas encore, crée-le. Nouvel essai dans 5 s...")
        time.sleep(5)
    admin.close()


def connect_mastodon():
    if not ACCESS_TOKEN:
        log("ATTENTION : pas de MASTODON_ACCESS_TOKEN, on tente en anonyme (certaines instances refusent)")
    api = Mastodon(access_token=ACCESS_TOKEN or None, api_base_url=API_BASE_URL, ratelimit_method="wait")
    if ACCESS_TOKEN:
        me = api.account_verify_credentials()
        log(f"Connecté à {API_BASE_URL} en tant que @{me['username']}")
    return api


def check_rate_budget():
    calls_per_round = len(HASHTAGS) + (1 if KEYWORDS else 0)
    per_5min = calls_per_round * (300 / POLL_INTERVAL)
    log(f"{calls_per_round} requêtes par tour, toutes les {POLL_INTERVAL:.0f} s -> ~{per_5min:.0f} req / 5 min (limite : 300)")
    if per_5min > 270:
        log("Trop proche de la limite -> augmente POLL_INTERVAL ou réduis le nombre de hashtags")


def main():
    log(f"Hashtags suivis : {HASHTAGS} | mots-clés timeline publique : {KEYWORDS or 'aucun'}")
    check_rate_budget()
    wait_for_kafka_and_topic()
    api = connect_mastodon()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=50,
    )

    since_ids = {}
    seen = set()  # un toot peut avoir plusieurs hashtags suivis on evite le doublon 
    sent = 0

    def send(toot):
        nonlocal sent
        if toot["toot_id"] in seen:
            return
        seen.add(toot["toot_id"])
        producer.send(TOPIC, key=toot["user_id"] or toot["toot_id"], value=toot)
        sent += 1

    def fetch(source, since_id=None, max_id=None):
        if source.startswith("tag:"):
            return api.timeline_hashtag(source[4:], since_id=since_id, max_id=max_id, limit=PAGE_LIMIT)
        return api.timeline_public(since_id=since_id, max_id=max_id, limit=PAGE_LIMIT)

    sources = [f"tag:{h}" for h in HASHTAGS] + (["public"] if KEYWORDS else [])

    def handle(source, statuses):
        for s in statuses:
            if s.get("reblog"):  #boost = repartage dun autre toot, l ignorerr pour pas compter en double
                continue
            toot = parse_toot(s, source if source != "public" else "public:keyword")
            if source == "public" and not matches_keywords(toot):
                continue
            send(toot)

    # 1) historique
    if BACKFILL_PAGES > 0:
        log(f"Backfill : {BACKFILL_PAGES} page(s) par source...")
        for source in sources:
            max_id = None
            for _ in range(BACKFILL_PAGES):
                try:
                    page = fetch(source, max_id=max_id)
                except MastodonError as e:
                    log(f"Erreur backfill {source} : {e}")
                    break
                if not page:
                    break
                since_ids.setdefault(source, page[0]["id"])
                handle(source, page)
                max_id = page[-1]["id"]
        producer.flush()
        log(f"Backfill terminé : {sent} toots envoyés")

    # 2) temps réel
    log(f"Flux temps réel vers '{TOPIC}' (Ctrl+C / docker compose stop producer pour arrêter)")
    try:
        while True:
            for source in sources:
                try:
                    statuses = fetch(source, since_id=since_ids.get(source))
                except MastodonError as e:
                    log(f"Erreur API sur {source} : {e} (on continue)")
                    continue
                if statuses:
                    since_ids[source] = statuses[0]["id"]  # lAPI renvoie du plus récent au plus ancien

                    handle(source, list(reversed(statuses)))
            producer.flush()
            log(f"{sent} toots envoyés au total")
            if len(seen) > 50_000:
                seen.clear()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        log(f"Arrêt. {sent} toots envoyés.")
        sys.exit(0)


if __name__ == "__main__":
    main()