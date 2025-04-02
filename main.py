import mysql.connector
from mysql.connector import Error

class Sql_Beispiele:
    @staticmethod
    def connection():
        try:
            mydb = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="root",
                password="root",
            )
            my_cursor = mydb.cursor()
            return mydb, my_cursor
        except Error as e:
            print(f"Fehler bei der Verbindung zur Datenbank: {e}")
            return None, None

    @staticmethod
    def create_database(my_cursor, db_name):
        try:
            my_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            print(f"Datenbank '{db_name}' wurde erfolgreich erstellt.")
        except Error as e:
            print(f"Fehler beim Erstellen der Datenbank: {e}")

    @staticmethod
    def create_table(my_cursor, db_name, table_name, columns):
        try:
            my_cursor.execute(f"USE {db_name}")
            my_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {columns}")
            print(f"Tabelle '{table_name}' wurde erfolgreich erstellt.")
        except mysql.connector.Error as err:
            print(f"Fehler beim Erstellen der Tabelle: {err}")

    @staticmethod
    def delete_table(my_cursor, db_name, table_name):
        try:
            my_cursor.execute(f"USE {db_name}")
            my_cursor.execute(f"DROP TABLE {table_name}")
            print(f"Tabelle '{table_name}' wurde erfolgreich gelöscht.")
        except Error as e:
            print(f"Fehler beim Löschen der Tabelle: {e}")

    @staticmethod
    def select_table(my_cursor, db_name, table_name, columns="*", condition=""):
        try:
            my_cursor.execute(f"USE {db_name}")
            query = f"SELECT {columns} FROM {table_name}"
            if condition:
                query += f" WHERE {condition}"
            my_cursor.execute(query)
            result = my_cursor.fetchall()
            for row in result:
                print(row)
        except Error as e:
            print(f"Fehler beim Auswählen der Daten: {e}")

    @staticmethod
    def insert(mydb, my_cursor, db_name, table_name, columns, values):
        try:
            my_cursor.execute(f"USE {db_name}")
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            my_cursor.execute(query)
            mydb.commit()
            print("Daten wurden erfolgreich eingefügt.")
            return my_cursor.lastrowid
        except Error as e:
            print(f"Fehler beim Einfügen der Daten: {e}")
            return None

    @staticmethod
    def update_table(mydb ,my_cursor, db_name, table_name, set_values, condition):
        try:
            my_cursor.execute(f"USE {db_name}")
            query = f"UPDATE {table_name} SET {set_values} WHERE {condition}"
            my_cursor.execute(query)
            mydb.commit()
            print("Daten wurden erfolgreich aktualisiert.")
        except Error as e:
            print(f"Fehler beim Aktualisieren der Daten: {e}")

    @staticmethod
    def join(my_cursor, db_name):
        try:
            my_cursor.execute(f"USE {db_name}")

            # INNER JOIN Beispiel
            query = """
            SELECT kunden.name, bestellungen.bestellnummer
            FROM kunden
            INNER JOIN bestellungen ON kunden.id = bestellungen.kunden_id
            """
            my_cursor.execute(query)
            print("\nINNER JOIN Ergebnis:")
            for row in my_cursor.fetchall():
                print(row)

            # LEFT JOIN Beispiel
            query = """
            SELECT kunden.name, bestellungen.bestellnummer
            FROM kunden
            LEFT JOIN bestellungen ON kunden.id = bestellungen.kunden_id
            """
            my_cursor.execute(query)
            print("\nLEFT JOIN Ergebnis:")
            for row in my_cursor.fetchall():
                print(row)

            # RIGHT JOIN Beispiel
            query = """
            SELECT kunden.name, bestellungen.bestellnummer
            FROM kunden
            RIGHT JOIN bestellungen ON kunden.id = bestellungen.kunden_id
            """
            my_cursor.execute(query)
            print("\nRIGHT JOIN Ergebnis:")
            for row in my_cursor.fetchall():
                print(row)

            # FULL OUTER JOIN Beispiel (MySQL unterstützt kein FULL OUTER JOIN, daher UNION von LEFT und RIGHT JOIN)
            query = """
            SELECT kunden.name, bestellungen.bestellnummer
            FROM kunden
            LEFT JOIN bestellungen ON kunden.id = bestellungen.kunden_id
            UNION
            SELECT kunden.name, bestellungen.bestellnummer
            FROM kunden
            RIGHT JOIN bestellungen ON kunden.id = bestellungen.kunden_id
            """
            my_cursor.execute(query)
            print("\nFULL OUTER JOIN (simuliert) Ergebnis:")
            for row in my_cursor.fetchall():
                print(row)

        except Error as e:
            print(f"Fehler bei JOIN-Operationen: {e}")

    @staticmethod
    def order_by(my_cursor, db_name, table_name, order_column):
        try:
            my_cursor.execute(f"USE {db_name}")
            query = f"SELECT * FROM {table_name} ORDER BY {order_column}"
            my_cursor.execute(query)
            print(f"\nORDER BY {order_column} Ergebnis:")
            for row in my_cursor.fetchall():
                print(row)
        except Error as e:
            print(f"Fehler bei ORDER BY: {e}")

    @staticmethod
    def where(my_cursor, db_name, table_name, condition):
        try:
            my_cursor.execute(f"USE {db_name}")
            query = f"SELECT * FROM {table_name} WHERE {condition}"
            my_cursor.execute(query)
            print(f"\nWHERE {condition} Ergebnis:")
            for row in my_cursor.fetchall():
                print(row)
        except Error as e:
            print(f"Fehler bei WHERE: {e}")

    @staticmethod
    def limit(my_cursor, db_name, table_name, limit):
        try:
            my_cursor.execute(f"USE {db_name}")
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            my_cursor.execute(query)
            print(f"\nLIMIT {limit} Ergebnis:")
            for row in my_cursor.fetchall():
                print(row)
        except Error as e:
            print(f"Fehler bei LIMIT: {e}")

    @staticmethod
    def drop_database(my_cursor, db_name):
        try:
            my_cursor.execute(f"DROP DATABASE {db_name}")
            print(f"Datenbank '{db_name}' wurde erfolgreich gelöscht.")
        except Error as e:
            print(f"Fehler beim Löschen der Datenbank: {e}")

    @staticmethod
    @staticmethod
    def main():
        mydb, my_cursor = Sql_Beispiele.connection()
        if mydb and my_cursor:
            Sql_Beispiele.setup_database(mydb, my_cursor)
            Sql_Beispiele.insert_example_data(mydb, my_cursor)
            Sql_Beispiele.update_example_data(mydb, my_cursor)
            Sql_Beispiele.select_examples(my_cursor)
            Sql_Beispiele.advanced_query_examples(my_cursor)
            Sql_Beispiele.cleanup_database(mydb, my_cursor)

    @staticmethod
    def setup_database(my_cursor):
        """Erstellt Datenbank und Tabellen"""
        Sql_Beispiele.create_database(my_cursor, "meine_datenbank")
        Sql_Beispiele.create_table(my_cursor, "meine_datenbank", "kunden",
                                   "(ID INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), adresse VARCHAR(255), age INT)")
        Sql_Beispiele.create_table(my_cursor, "meine_datenbank", "Bestellungen",
                                   "(ID INT AUTO_INCREMENT PRIMARY KEY, bestellnummer VARCHAR(255), kunden_id INT, FOREIGN KEY (kunden_id) REFERENCES Kunden(ID) ON DELETE CASCADE)")

    @staticmethod
    def insert_example_data(mydb, my_cursor):
        """Fügt Beispieldaten ein"""
        kunden_id_1 = Sql_Beispiele.insert(mydb, my_cursor, "meine_datenbank", "kunden", "name, adresse, age",
                                           "'Max Mustermann', 'Musterstraße 1', 33")
        kunden_id_2 = Sql_Beispiele.insert(mydb, my_cursor, "meine_datenbank", "kunden", "name, adresse, age",
                                           "'Erika Mustermann', 'Beispielweg 2', 18")
        if kunden_id_1 and kunden_id_2:
            Sql_Beispiele.insert(mydb, my_cursor, "meine_datenbank", "bestellungen", "bestellnummer, kunden_id",
                                 f"'B1001', {kunden_id_1}")
            Sql_Beispiele.insert(mydb, my_cursor, "meine_datenbank", "bestellungen", "bestellnummer, kunden_id",
                                 f"'B1002', {kunden_id_2}")

    @staticmethod
    def update_example_data(mydb, my_cursor):
        """Aktualisiert Beispieldaten"""
        Sql_Beispiele.update_table(mydb, my_cursor, "meine_datenbank", "kunden", "adresse = 'Neue Musterstraße 2'",
                                   "name = 'Max Mustermann'")

    @staticmethod
    def select_examples(my_cursor):
        """Demonstriert verschiedene SELECT-Operationen"""
        print("\nAlle Einträge in der Tabelle:")
        Sql_Beispiele.select_table(my_cursor, "meine_datenbank", "kunden")

        print("\nNur Name und Alter auswählen:")
        Sql_Beispiele.select_table(my_cursor, "meine_datenbank", "kunden", "name, age")

        print("\nNur Personen mit Alter über 30 auswählen:")
        Sql_Beispiele.select_table(my_cursor, "meine_datenbank", "kunden", "*", "age > 30")

    @staticmethod
    def advanced_query_examples(my_cursor):
        """Demonstriert erweiterte Abfragen"""
        # JOIN-Beispiele
        Sql_Beispiele.join(my_cursor, "meine_datenbank")

        # ORDER BY Beispiel
        Sql_Beispiele.order_by(my_cursor, "meine_datenbank", "kunden", "name")

        # WHERE Beispiel
        Sql_Beispiele.where(my_cursor, "meine_datenbank", "kunden", "name = 'Max Mustermann'")

        # LIMIT Beispiel
        Sql_Beispiele.limit(my_cursor, "meine_datenbank", "kunden", 1)

    @staticmethod
    def cleanup_database(mydb, my_cursor):
        """Räumt die Datenbank auf"""
        Sql_Beispiele.delete_table(my_cursor, "meine_datenbank", "bestellungen")
        Sql_Beispiele.delete_table(my_cursor, "meine_datenbank", "kunden")
        Sql_Beispiele.drop_database(my_cursor, "meine_datenbank")
        my_cursor.close()
        mydb.close()

if __name__ == "__main__":
    Sql_Beispiele.main()