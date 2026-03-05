"""Purchase Order Sink for Tilroy API."""

import logging
from typing import Any, Dict, List, Optional

# import requests
# from singer_sdk import typing as th
# from singer_sdk.sinks import Sink
from datetime import datetime
from target_tilroy.client import TilroySink

logger = logging.getLogger(__name__)


class PurchaseOrderSink(TilroySink):
    """Sink for Purchase Orders to Tilroy API."""

    name = "BuyOrders"
    # Define the endpoint path for this sink
    endpoint = "/purchaseapi/production/import/purchaseorders"
    
    def preprocess_record(self, record: dict, context: dict) -> Optional[dict]:
        """Prepare Tilroy purchase order payload before sending to API."""
    
        # process order_date
        order_date = record.get("transaction_date")
        if isinstance(order_date, datetime):
            order_date = order_date.strftime("%Y-%m-%d")
        elif isinstance(order_date, str):
            # Handle ISO format dates
            try:
                if 'T' in order_date:
                    order_date = order_date.split('T')[0]
                else:
                    order_date = order_date
            except:
                pass
            
        # requestedDeliveryDate from BuyOrder created_at (keep ISO date-time like data.singer)
        requested_delivery_date = record.get("created_at")
        if isinstance(requested_delivery_date, datetime):
            requested_delivery_date = requested_delivery_date.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        # if str, pass through as-is (e.g. "2026-03-01T00:00:00.000000Z")

        payload = {
            "orderDate": order_date,
        }
        if record.get("id") is not None:
            payload["supplierReference"] = str(record["id"])

        if requested_delivery_date:
            payload["requestedDeliveryDate"] = requested_delivery_date

        # supplier.tilroyId from customer_id (ETL renames supplierId -> customer_id)
        if record.get("customer_id"):
            payload["supplier"] = {"tilroyId": record.get("customer_id")}
        else:
            self.logger.info(
                f"Skipping order {record.get('id')} because customer_id is missing"
            )
            return None

        # lines from line_items (fallback to "items" for backward compat)
        items = record.get("line_items", [])
        if not items:
            items = record.get("items", [])
        
        if isinstance(items, str):
            items = self.parse_objs(items)

        if not items:
            self.logger.info(f"Skipping order {record.get('id')} with no line items")
            return None

        payload["lines"] = []
        order_delivery_date = payload.get("requestedDeliveryDate")  # BuyOrder created_at
        for item in items:
            transformed_item = {
                "status": "open",
                "sku": {"tilroyId": str(item.get("product_remoteId"))},
                "qty": {"ordered": item.get("quantity")},
                "warehouse": {"number": int(self.config.get("warehouse_id"))},
            }
            line_delivery = item.get("delivery_date") or order_delivery_date
            if line_delivery:
                transformed_item["requestedDeliveryDate"] = line_delivery

            payload["lines"].append(transformed_item)

        return payload
    
    def process_record(self, record: dict, context: dict) -> None:
        """Process a single record."""
        # First preprocess the record
        processed_record = self.preprocess_record(record, context)
        if processed_record:
            self.upsert_record(processed_record, context)
    
    def upsert_record(self, record: dict, context: dict) -> None:
        """Send purchase order to Tilroy API."""
        import requests
        
        if record:
            # Construct and log the full URL
            full_url = f"{self.base_url}{self.endpoint}"
            self.logger.info(f"Making API request to: {full_url}")
            self.logger.info(f"Request method: POST")
            self.logger.info(f"Request payload: {record}")

            try:
                response = requests.post(
                    full_url,
                    json=record,
                    headers=self.http_headers,
                    timeout=30
                )
                
                # Log response details for debugging
                self.logger.info(f"Response status: {response.status_code}")
                
                if response.status_code in (200, 201):
                    res_json_id = response.json().get("supplierReference")
                    self.logger.info(f"{self.name} created in Tilroy with ID: {res_json_id}")
                elif response.status_code == 500:
                    # Handle specific 500 errors
                    try:
                        error_data = response.json()
                        if "supplier" in error_data.get("message", "").lower() and "not found" in error_data.get("message", "").lower():
                            self.logger.warning(f"Skipping order - supplier not found in Tilroy: {error_data.get('message')}")
                            return  # Skip this order instead of failing
                        else:
                            self.logger.error(f"API error response: {response.text}")
                            response.raise_for_status()
                    except:
                        self.logger.error(f"API error response: {response.text}")
                        response.raise_for_status()
                else:
                    self.logger.error(f"API error response: {response.text}")
                    response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"API request failed: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    self.logger.error(f"Error response: {e.response.text}")
                raise
    
    def process_batch(self, context: Dict[str, Any]) -> None:
        """Process any remaining records in the batch."""
        # No batch processing needed for this sink
        pass
    
    def clean_up(self) -> None:
        """Clean up resources."""
        # No cleanup needed for this sink
        pass

