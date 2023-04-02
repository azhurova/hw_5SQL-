import psycopg2


def db_connect():
    with psycopg2.connect(database="crm", user="postgres", password="postgres") as conn:
        return conn


# Функция, создающая структуру БД (таблицы).
def create_db_tables(conn):
    with conn.cursor() as cur:
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
def add_client(conn, name, surname, email):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client(name, surname, email)
            VALUES(%s, %s, %s) RETURNING client_id;
        """, (name, surname, email))
        print("Добавлен новый клиент id=", cur.fetchone()[0])
    conn.commit()


# Функция, позволяющая добавить телефон для существующего клиента.
def add_client_phone(conn, number, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phone(number, client_id)
            VALUES(%s, %s) RETURNING phone_id;
        """, (number, client_id))
        print("Добавлен новый телефон id=", cur.fetchone()[0], "для клиента id=", client_id)
    conn.commit()


# Функция, позволяющая вывести все данные клиента
def clients_info(conn):
    print("Данные:")
    with conn.cursor() as cur:
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
def update_client(conn, client_id, name, surname, email):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE client
                SET name=%s,
                    surname=%s,
                    email=%s
            WHERE client_id=%s;
        """, (name, surname, email, client_id))
        print("Изменены данные клиента id=", client_id)
    conn.commit()


# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(conn, phone_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone
            WHERE phone_id=%s;
        """, (phone_id,))
        print("Удалён телефон id=", phone_id)
    conn.commit()


# Функция, позволяющая удалить существующего клиента.
def delete_client(conn, client_id):
    with conn.cursor() as cur:
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
    conn.commit()


# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
def find_client(conn, name, surname, email, phone_number):
    print("Поиск клиента", "name=", name, "surname=", surname, "email=", email, "phone_number=", phone_number)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT c.client_id, c.name, c.surname, c.email
            FROM client c 
                JOIN phone p ON c.client_id = p.client_id
            WHERE c.name = %s 
                OR c.surname = %s 	
                OR c.email = %s
                OR p.number = %s;
        """, (name, surname, email, phone_number))
        print(cur.fetchall())


conn = db_connect()
create_db_tables(conn)
print("----------")
clients_info(conn)
print("----------")
print("")

add_client(conn, "Иван", "Иванов", "")
add_client(conn, "Петр", "Петров", "p.petrov@email.com")
add_client(conn, "Василий", "Тёркин", "v.terkin@email.su")
add_client_phone(conn, "+7(911)999-66-55", 1)
add_client_phone(conn, "88124256372", 2)
add_client_phone(conn, "88129999999", 2)
add_client_phone(conn, "89991112233", 3)
print("----------")
clients_info(conn)
print("----------")
print("")

update_client(conn, 1, "Иванна", "Иванова", "***")
update_client(conn, 2, "Петр", "Петров", "p.petrov@email.ru")
print("----------")
clients_info(conn)
print("----------")
print("")

delete_phone(conn, 3)
delete_client(conn, 3)
print("----------")
clients_info(conn)
print("----------")
print("")

find_client(conn, "", "", "", "88124256372")
