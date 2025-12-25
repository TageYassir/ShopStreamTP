import random
import json
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch

# Configuration de Faker (multi-langues pour réalisme)
fake = Faker(['fr_FR', 'en_US', 'de_DE', 'es_ES'])

# MODIFIEZ CES PARAMETRES SELON VOTRE CONFIGURATION
DB_CONFIG = {
    'host': 'localhost',
    'database': 'shopstream',
    'user': 'postgres',
    'password': '1234'  # METTEZ VOTRE MOT DE PASSE ICI
}

# Constantes métier
COUNTRIES = ['FRA', 'USA', 'DEU', 'ESP', 'GBR', 'ITA', 'CAN', 'AUS']
PLAN_TYPES = ['freemium', 'premium', 'enterprise']
CATEGORIES = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports', 'Beauty', 'Toys']
ORDER_STATUSES = ['pending', 'paid', 'shipped', 'delivered', 'cancelled']
EVENT_TYPES = ['page_view', 'add_to_cart', 'checkout_start', 'purchase', 'error']
PAYMENT_METHODS = ['stripe', 'paypal', 'credit_card']
SOURCES = ['organic', 'paid_ads', 'referral', 'email_campaign']


def get_connection():
    """Connexion à PostgreSQL"""
    print("Connexion à PostgreSQL...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connexion réussie")
        return conn
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        print("Vérifiez vos paramètres dans DB_CONFIG (host, database, user, password)")
        exit(1)


def generate_users(conn, n=1000):
    """Génère n utilisateurs"""
    print(f"\nGénération de {n} utilisateurs...")
    cursor = conn.cursor()

    users = []
    for i in range(n):
        if i % 100 == 0:
            print(f"   ...  {i}/{n} utilisateurs générés")

        user = (
            fake.email(),
            fake.first_name(),
            fake.last_name(),
            random.choice(COUNTRIES),
            random.choice(PLAN_TYPES) if random.random() > 0.7 else 'freemium',
            fake.date_time_between(start_date='-2y', end_date='now'),
            fake.date_time_between(start_date='-30d', end_date='now') if random.random() > 0.3 else None,
            random.random() > 0.05
        )
        users.append(user)

    query = """
        INSERT INTO users (email, first_name, last_name, country, plan_type, created_at, last_login, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        execute_batch(cursor, query, users, page_size=100)
        conn.commit()
        print(f"{n} utilisateurs créés avec succès")
    except Exception as e:
        print(f"Erreur lors de la création des utilisateurs : {e}")
        conn.rollback()


def generate_products(conn, n=200):
    """Génère n produits"""
    print(f"\nGénération de {n} produits...")
    cursor = conn.cursor()

    products = []
    for i in range(n):
        if i % 50 == 0:
            print(f"   ... {i}/{n} produits générés")

        merchant_id = random.randint(1, 100)
        product = (
            merchant_id,
            fake.catch_phrase(),
            fake.text(max_nb_chars=150),
            random.choice(CATEGORIES),
            round(random.uniform(5.0, 500.0), 2), random.randint(0, 1000),
            fake.date_time_between(start_date='-1y', end_date='now'), datetime.now())
        products.append(product)

    query = """
        INSERT INTO products (merchant_id, name, description, category, price, stock_quantity, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        execute_batch(cursor, query, products, page_size=100)
        conn.commit()
        print(f"{n} produits créés avec succès")
    except Exception as e:
        print(f"Erreur lors de la création des produits : {e}")
        conn.rollback()


def generate_orders(conn, n=5000):
    """Génère n commandes avec leurs lignes"""
    print(f"\nGénération de {n} commandes...")
    cursor = conn.cursor()

    # Récupération des IDs users et products existants
    cursor.execute("SELECT id, country FROM users WHERE is_active = TRUE")
    users = cursor.fetchall()

    cursor.execute("SELECT id, price FROM products")
    products = cursor.fetchall()

    if not users or not products:
        print("Pas d'utilisateurs ou de produits.  Générez-les d'abord.")
        return

    orders = []
    order_items = []

    for i in range(n):
        if i % 500 == 0:
            print(f"   ... {i}/{n} commandes générées")

        user = random.choice(users)
        user_id = user[0]
        country = user[1]

        created_at = fake.date_time_between(start_date='-6m', end_date='now')

        # Nombre d'articles par commande (1 à 5)
        nb_items = random.randint(1, 5)

        total_amount = 0
        order_products = random.sample(products, min(nb_items, len(products)))

        for product in order_products:
            product_id = product[0]
            unit_price = product[1]
            quantity = random.randint(1, 3)
            line_total = unit_price * quantity
            total_amount += line_total

            order_items.append((
                i + 1,
                product_id,
                quantity,
                unit_price,
                line_total
            ))

        status = random.choices(
            ORDER_STATUSES,
            weights=[5, 40, 25, 25, 5]
        )[0]

        orders.append((
            user_id,
            created_at,
            round(total_amount, 2),
            status,
            country,
            random.choice(PAYMENT_METHODS)
        ))

    # Insertion des commandes
    query_orders = """
        INSERT INTO orders (user_id, created_at, total_amount, status, country, payment_method)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    try:
        execute_batch(cursor, query_orders, orders, page_size=100)
        conn.commit()
        print(f"{n} commandes créées")
    except Exception as e:
        print(f"Erreur lors de la création des commandes : {e}")
        conn.rollback()
        return

    # Insertion des lignes de commande
    query_items = """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        execute_batch(cursor, query_items, order_items, page_size=100)
        conn.commit()
        print(f"{len(order_items)} lignes de commande créées")
    except Exception as e:
        print(f"Erreur lors de la création des lignes : {e}")
        conn.rollback()


def generate_events(conn, n=10000):
    """Génère n événements"""
    print(f"\nGénération de {n} événements...")
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM users LIMIT 500")
    user_ids = [row[0] for row in cursor.fetchall()]

    if not user_ids:
        print("Pas d'utilisateurs. Générez-les d'abord.")
        return

    events = []
    for i in range(n):
        if i % 1000 == 0:
            print(f"   ... {i}/{n} événements générés")

        user_id = random.choice(user_ids) if random.random() > 0.1 else None
        event_type = random.choice(EVENT_TYPES)
        event_ts = fake.date_time_between(start_date='-3m', end_date='now')

        if event_type == 'page_view':
            metadata = json.dumps({
                'page_url': fake.uri_path(),
                'device': random.choice(['mobile', 'desktop', 'tablet'])
            })
        elif event_type == 'add_to_cart':
            metadata = json.dumps({
                'product_id': random.randint(1, 200),
                'quantity': random.randint(1, 3)
            })
        else:
            metadata = json.dumps({})

        events.append((
            user_id,
            event_type,
            event_ts,
            metadata
        ))

    query = """
        INSERT INTO events (user_id, event_type, event_ts, metadata)
        VALUES (%s, %s, %s, %s)
    """

    try:
        execute_batch(cursor, query, events, page_size=100)
        conn.commit()
        print(f"{n} événements créés")
    except Exception as e:
        print(f"Erreur lors de la création des événements : {e}")
        conn.rollback()


def generate_crm_contacts(conn, n=500):
    """Génère n contacts CRM"""
    print(f"\nGénération de {n} contacts CRM...")
    cursor = conn.cursor()

    contacts = []
    for i in range(n):
        if i % 100 == 0:
            print(f"   ... {i}/{n} contacts générés")

        converted = random.random() > 0.6
        created_at = fake.date_time_between(start_date='-1y', end_date='now')

        contact = (
            fake.email(),
            fake.first_name(),
            fake.last_name(),
            random.choice(SOURCES),
            f"CAMP_{random.randint(1000, 9999)}",
            created_at,
            converted,
            fake.date_time_between(start_date=created_at, end_date='now') if converted else None
        )
        contacts.append(contact)

    query = """
        INSERT INTO crm_contacts (email, first_name, last_name, source, campaign_id, created_at, converted, converted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        execute_batch(cursor, query, contacts, page_size=100)
        conn.commit()
        print(f"{n} contacts CRM créés")
    except Exception as e:
        print(f"Erreur lors de la création des contacts : {e}")
        conn.rollback()


def main():
    """Point d'entrée principal"""
    print("=" * 70)
    print("GENERATION DE DONNEES SHOPSTREAM")
    print("=" * 70)

    conn = get_connection()

    try:
        # Génération dans l'ordre (à cause des clés étrangères)
        generate_users(conn, n=1000)
        generate_products(conn, n=200)
        generate_orders(conn, n=5000)
        generate_events(conn, n=10000)
        generate_crm_contacts(conn, n=500)

        print("\n" + "=" * 70)
        print("GENERATION TERMINEE AVEC SUCCES")
        print("=" * 70)
        print("\nRécapitulatif :")
        cursor = conn.cursor()
        cursor.execute("SELECT 'users' AS table_name, COUNT(*) FROM users")
        print(f"   - Users : {cursor.fetchone()[1]}")
        cursor.execute("SELECT COUNT(*) FROM products")
        print(f"   - Products : {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM orders")
        print(f"   - Orders : {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM order_items")
        print(f"   - Order Items : {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM events")
        print(f"   - Events : {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM crm_contacts")
        print(f"   - CRM Contacts : {cursor.fetchone()[0]}")

    except Exception as e:
        print(f"\nERREUR GLOBALE : {e}")
        conn.rollback()
    finally:
        conn.close()
        print("\nConnexion fermée.")


if __name__ == "__main__":
    main()