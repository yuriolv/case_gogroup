import json
import psycopg2
import os
import time
from confluent_kafka import Consumer
import logging

logger = logging.getLogger(__name__)


class KabumConsumer:

    def __init__(
        self,
        kafka_broker="kafka:9092",
        topic="smartphones",
        group_id="smartphones-group",
        postgres_conn=None
    ):
        self.consumer = Consumer({
            "bootstrap.servers": kafka_broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest"
        })

        self.consumer.subscribe([topic])

        self.conn = psycopg2.connect(postgres_conn)
        self.cursor = self.conn.cursor()

    def insert_data(self, data):

        query = """
        INSERT INTO raw.smartphones (
            product_id, record_hash,
            title, category, brand,
            price, original_price, discount_pct, price_final,
            stock, rating, reviews_count,
            free_shipping, condition,
            seller_id, seller_name, seller_state,
            collected_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id, record_hash) DO NOTHING;
        """

        values = (
            data.get("product_id"),
            data.get("record_hash"),

            data.get("title"),
            data.get("category"),
            data.get("brand"),

            data.get("price"),
            data.get("original_price"),
            data.get("discount_pct"),
            data.get("price_final"),

            data.get("stock"),
            data.get("rating"),
            data.get("reviews_count"),

            data.get("free_shipping"),
            data.get("condition"),

            data.get("seller_id"),
            data.get("seller_name"),
            data.get("seller_state"),

            data.get("collected_at"),
        )

        self.cursor.execute(query, values)

    def run(self, max_messages=10000, idle_timeout=10):
        total = 0
        last_msg_time = time.time()

        logger.info("Consumer iniciado")

        while total < max_messages:
            msg = self.consumer.poll(1.0)

            if msg is None:
                if time.time() - last_msg_time > idle_timeout:
                    logger.info("Fila vazia, encerrando")
                    break
                continue

            if msg.error():
                logger.error(f"{msg.error()}")
                continue

            last_msg_time = time.time()

            data = json.loads(msg.value().decode("utf-8"))

            try:
                self.insert_data(data)
                total += 1

                if total % 100 == 0:
                    self.conn.commit()
                    logger.info(f"Processados: {total}")

            except Exception as e:
                logger.exception(f"Erro ao inserir: {e}")
                self.conn.rollback()

        self.conn.commit()
        self.consumer.close()

        logger.info(f"Consumidos: {total}")


if __name__ == "__main__":

    conn_str = (
        f"dbname={os.getenv('POSTGRES_DB')} "
        f"user={os.getenv('POSTGRES_USER')} "
        f"password={os.getenv('POSTGRES_PASSWORD')} "
        f"host={os.getenv('POSTGRES_HOST')} "
        f"port={os.getenv('POSTGRES_PORT')}"
    )

    consumer = KabumConsumer(postgres_conn=conn_str)
    consumer.run()