import json
import time
import requests
from datetime import datetime, timedelta
from confluent_kafka import Producer
import logging 
import hashlib

logger = logging.getLogger(__name__)


class KabumSmartphonesProducer:
    BASE_URL = "https://servicespub.prod.api.aws.grupokabum.com.br/catalog/v2/products-by-category/smartphones"

    def __init__(self, kafka_broker="kafka:9092", topic="smartphones", page_size=100):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.page_size = page_size

        self.producer = Producer({
            "bootstrap.servers": self.kafka_broker,
            "linger.ms": 10,
            "retries": 3
        })
        
    def generate_hash(self, record: dict) -> str:
        relevant_fields = {
            "product_id": record.get("product_id"),
            "title": record.get("title"),
            "price": record.get("price"),
            "original_price": record.get("original_price"),
            "stock": record.get("stock"),
            "seller_id": record.get("seller_id"),
            "condition": record.get("condition"),
            "free_shipping": record.get("free_shipping"),
        }

        normalized = json.dumps(relevant_fields, sort_keys=True)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Falha ao enviar: {err}")

    def get_data(self, page_number=1, max_retries=3, **filters):
        params = {
            "page_number": page_number,
            "page_size": self.page_size,
            **filters
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=10)

                if response.status_code == 200:
                    return response.json()

                logger.warning(f"Status {response.status_code} | tentativa {attempt+1}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Request falhou: {e}")

            time.sleep(2 ** attempt)

        raise Exception(f"Falha após {max_retries} tentativas")

    def parse_product(self, item):
        attr = item.get("attributes", {})
        marketplace = attr.get("marketplace", {})
        manufacturer = attr.get("manufacturer", {})

        title = attr.get("title", "").lower()

        return {
            "product_id": item.get("id"),
            "title": attr.get("title"),
            "category": attr.get("menu"),

            "brand": manufacturer.get("name"),

            "price": attr.get("price"),
            "original_price": attr.get("old_price"),
            "discount_pct": attr.get("discount_percentage"),
            "price_final": attr.get("price_with_discount"),

            "stock": attr.get("stock"),

            "rating": attr.get("score_of_ratings"),
            "reviews_count": attr.get("number_of_ratings"),

            "free_shipping": attr.get("has_free_shipping"),

            "condition": "used" if "usado" in title else "new",

            "seller_id": marketplace.get("seller_id"),
            "seller_name": marketplace.get("seller_name"),
            "seller_state": marketplace.get("state"),

            "collected_at": datetime.now().isoformat() 
        }

    def fetch_all_products(self, **filters):
        page = 1

        while True:
            data = self.get_data(page_number=page, **filters)

            products = data.get("data", [])
            meta = data.get("meta", {})
            total = meta.get("total_items_count")
            total_pages = meta.get("total_pages_count")

            if not products:
                logger.info(f"Fim da paginação na página {page}")
                break

            logger.info(f"Página {page}/{total_pages} | Produtos: {len(products)} | Total: {total}")

            yield products

            if total_pages and page >= total_pages:
                break

            page += 1

    def send_to_kafka(self, product):
        self.producer.produce(
            topic=self.topic,
            key=str(product["product_id"]),
            value=json.dumps(product).encode("utf-8"),
            callback=self.delivery_report
        )

    def run(self, **filters):
        total_enviados = 0
        start_time = time.time()

        for products in self.fetch_all_products(**filters):
            for item in products:
                parsed = self.parse_product(item)
                parsed["record_hash"] = self.generate_hash(parsed)

                self.send_to_kafka(parsed)
                total_enviados += 1

        self.producer.flush()

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Total enviados: {total_enviados} | Tempo: {elapsed}s")


if __name__ == "__main__":
    producer = KabumSmartphonesProducer()
    producer.run()