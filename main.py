import psycopg2


def db_connect():
    with psycopg2.connect(database="crm", user="postgres", password="331350") as conn:
        return conn


# Функция, создающая структуру БД (таблицы).
def create_db_tables(cur):
    # удаление таблиц
    cur.execute("""
            DROP TABLE IF EXISTS phone;
            DROP TABLE IF EXISTS client;
        """)

    # создание таблиц
    cur.execute("""
            CREATE TABLE IF NOT EXISTS client (
                client_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
                "name" varchar NOT NULL,
                surname varchar NOT NULL,
                email varchar NULL,
                CONSTRAINT client_pk PRIMARY KEY (client_id)
            );
        """)
    print("Создана таблица client")

    cur.execute("""
            CREATE TABLE IF NOT EXISTS phone (
                phone_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
                "number" varchar NOT NULL,
                client_id int4 NOT NULL,
                CONSTRAINT phone_pk PRIMARY KEY (phone_id)
            );
        """)
    print("Создана таблица phone")

    conn.commit()


# Функция, позволяющая добавить нового клиента.
def add_client(cur, name, surname, email):
    cur.execute("""
            INSERT INTO client(name, surname, email)
            VALUES(%s, %s, %s) RETURNING client_id;
        """, (name, surname, email))
    print("Добавлен новый клиент id=", cur.fetchone()[0])


# Функция, позволяющая добавить телефон для существующего клиента.
def add_client_phone(cur, number, client_id):
    cur.execute("""
            INSERT INTO phone(number, client_id)
            VALUES(%s, %s) RETURNING phone_id;
        """, (number, client_id))
    print("Добавлен новый телефон id=", cur.fetchone()[0], "для клиента id=", client_id)


# Функция, позволяющая вывести все данные клиента
def clients_info(cur):
    print("Данные:")

    cur.execute("""
            SELECT client_id, name, surname, email
            FROM client;
        """)
    clients = cur.fetchall()

    for client in clients:
        cur.execute("""
                        SELECT phone_id, number, client_id
                        FROM phone
                        WHERE client_id = %s;
                    """, (client[0],))
        phones = cur.fetchall()
        print(client, phones)


# Функция, позволяющая изменить данные о клиенте.
def update_client(cur, client_id, name=None, surname=None, email=None):
    cur.execute("""
            SELECT name, surname, email
            FROM client
            WHERE client_id = %s;
        """, (client_id,))
    client = cur.fetchone()

    if name is None:
        name = client[0]

    if surname is None:
        surname = client[1]

    if email is None:
        email = client[2]

    cur.execute("""
            UPDATE client
                SET name=%s,
                    surname=%s,
                    email=%s
            WHERE client_id=%s;
        """, (name, surname, email, client_id))
    print("Изменены данные клиента id=", client_id)


# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(cur, phone_id):
    cur.execute("""
            DELETE FROM phone
            WHERE phone_id=%s;
        """, (phone_id,))
    print("Удалён телефон id=", phone_id)


# Функция, позволяющая удалить существующего клиента.
def delete_client(cur, client_id):
    cur.execute("""
            DELETE FROM phone
            WHERE client_id=%s;
        """, (client_id,))
    print("Удалены телефоны клиента id=", client_id)
    cur.execute("""
            DELETE FROM client
            WHERE client_id=%s;
        """, (client_id,))
    print("Удалён клиент id=", client_id)


# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
def find_client(cur, name=None, surname=None, email=None, phone_number=None):
    print("Поиск клиента", "name=", name, "surname=", surname, "email=", email, "phone_number=", phone_number)

    cur.execute("""
            SELECT DISTINCT c.client_id, c.name, c.surname, c.email
            FROM client c 
                JOIN phone p ON c.client_id = p.client_id
            WHERE c.name = COALESCE(%s, c.name)
                AND c.surname = COALESCE(%s, c.surname)	
                AND c.email = COALESCE(%s, c.email)
                AND p.number = COALESCE(%s, p.number);
        """, (name, surname, email, phone_number))
    print(cur.fetchall())


if __name__ == "__main__":
    conn = db_connect()
    with conn.cursor() as cur:
        create_db_tables(cur)
        conn.commit()
        print("----------")
        clients_info(cur)
        print("----------")
        print("")

        add_client(cur, "Иван", "Иванов", "")
        add_client(cur, "Петр", "Петров", "p.petrov@email.com")
        add_client(cur, "Василий", "Тёркин", "v.terkin@email.su")
        add_client_phone(cur, "+7(911)999-66-55", 1)
        add_client_phone(cur, "88124256372", 2)
        add_client_phone(cur, "88129999999", 2)
        add_client_phone(cur, "89991112233", 3)
        conn.commit()
        print("----------")
        clients_info(cur)
        print("----------")
        print("")

        update_client(cur, 1, "Иванна", "Иванова", "***")
        update_client(cur, 2, email="p.petrov@email.ru")
        conn.commit()
        print("----------")
        clients_info(cur)
        print("----------")
        print("")

        delete_phone(cur, 3)
        delete_client(cur, 3)
        conn.commit()
        print("----------")
        clients_info(cur)
        print("----------")
        print("")

        find_client(cur, phone_number="88124256372")
