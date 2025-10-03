from .base import BrokerBase
import hashlib
import requests
from urllib.parse import urlencode
import logging
from brokers import symbol_map

try:
    from fyers_apiv3 import fyersModel
except ImportError:
    fyersModel = None

logger = logging.getLogger(__name__)

class FyersBroker(BrokerBase):
    BROKER = "fyers"
    
    def _normalize_product_type(self, product_type):
        """Normalize product type for Fyers API.
        
        Fyers expects: INTRADAY, CNC, MARGIN, BO, CO
        """
        if not product_type:
            return "INTRADAY"  # Default to INTRADAY
            
        pt = str(product_type).upper()
        
        # Direct mappings for Fyers product types
        if pt in {"INTRADAY", "CNC", "MARGIN", "BO", "CO"}:
            return pt
            
        # Map common product types to Fyers format
        mapping = {
            "MIS": "INTRADAY",
            "INTRA": "INTRADAY",
            "I": "INTRADAY",
            "NRML": "MARGIN",
            "NORMAL": "MARGIN",
            "H": "MARGIN",
            "DELIVERY": "CNC",
            "C": "CNC",
            "LONGTERM": "CNC",
            "MTF": "MARGIN",  # MTF maps to MARGIN in Fyers
            "BRACKET": "BO",
            "COVER": "CO",
        }
        
        mapped = mapping.get(pt, pt)  # Return original if no mapping found
        logger.debug(f"Mapped product type {pt} to {mapped}")
        return mapped

    def _normalize_order_type(self, order_type):
        if not order_type:
            return "MARKET"
            
        ot = str(order_type).upper()
        mapping = {
            "MKT": "MARKET",
            "MARKET": "MARKET",
            "L": "LIMIT",
            "LIMIT": "LIMIT",
            "SL": "STOP",
            "SL-M": "STOP_MARKET",
            "STOP": "STOP",
            "STOP_LIMIT": "STOP",
            "STOP_MARKET": "STOP_MARKET",
        }
        return mapping.get(ot, ot)

    def __init__(self, client_id, access_token, **kwargs):
        super().__init__(client_id, access_token, **kwargs)

        # FIXED: Simplified token handling for Fyers
        raw_token = (access_token or "").strip()
        
        if not raw_token:
            raise ValueError("Fyers access_token is required")
        
        # Remove "Bearer " prefix if present (case-insensitive)
        if raw_token.lower().startswith("bearer "):
            raw_token = raw_token[7:].strip()
            logger.debug("Removed 'Bearer' prefix from token")
        
        # Check if token already has client_id prefix
        client_id_lower = str(client_id).lower()
        expected_prefix = f"{client_id_lower}:"
        
        if raw_token.lower().startswith(expected_prefix):
            # Token already correctly formatted with client_id
            combined_token = raw_token
            logger.debug("Token already has correct client_id prefix")
        elif ":" in raw_token:
            # Token has some prefix, check if it matches client_id
            token_parts = raw_token.split(":", 1)
            if token_parts[0].lower() == client_id_lower:
                # Correct client_id, use as-is
                combined_token = raw_token
                logger.debug("Token has matching client_id prefix")
            else:
                # Different prefix or wrong client_id, replace with correct one
                access_part = token_parts[1] if len(token_parts) > 1 else raw_token
                combined_token = f"{client_id}:{access_part}"
                logger.warning(f"Replaced token prefix from '{token_parts[0]}' to '{client_id}'")
        else:
            # No prefix, add client_id
            combined_token = f"{client_id}:{raw_token}"
            logger.debug("Added client_id prefix to token")
        
        # Validate combined token format
        if ":" not in combined_token:
            logger.error("Invalid token format: missing colon separator")
            raise ValueError("Fyers access token must be in format 'client_id:token' or provide just the token part")
        
        token_parts = combined_token.split(":", 1)
        if len(token_parts) < 2 or not token_parts[1]:
            raise ValueError("Invalid Fyers token: missing token part after colon")


        # Guard against duplicated client_id prefixes (e.g. "id:id:token").
        remainder = token_parts[1]
        if remainder.lower().startswith(expected_prefix):
            remainder = remainder.split(":", 1)[1]
            combined_token = f"{client_id}:{remainder}"
            token_parts = [client_id, remainder]
            
        if len(token_parts[1]) < 50:
            logger.warning(f"Token seems too short ({len(token_parts[1])} chars), expected 100-150")
        
        logger.info(f"Fyers token format: {client_id}:[{len(token_parts[1])} chars]")
        

        raw_access_token = token_parts[1]

        # Store both token representations for downstream use
        self.combined_token = combined_token
        self.raw_access_token = raw_access_token


        # Normalized tokens should also be reflected in the base attributes and
        # session headers so any REST fallbacks share the same auth state.
        self.access_token = self.combined_token
        self.session.headers["Authorization"] = self.combined_token
        self.session.headers["access_token"] = self.combined_token
        
        # Initialize Fyers API client
        if fyersModel is not None:
            try:
                # CRITICAL FIX: Fyers API v3 expects specific initialization
                # Parameter order matters: client_id first, then token
                # The token should be client_id:access_token_part (no Bearer prefix)
                
                self.api = fyersModel.FyersModel(
                    client_id=client_id,  # First parameter
                    token=raw_access_token,  # Second parameter: bare token
                    is_async=False,
                    log_path=""
                )
                
                # IMPORTANT: Set the access token explicitly as a property
                # This is required by some versions of the Fyers SDK
                self.api.token = raw_access_token
                
                logger.info("Fyers API client initialized successfully")
                logger.debug(f"API token set to: {raw_access_token[:40]}...{raw_access_token[-20:]}")
                
                # Validate token immediately
                try:
                    test_resp = self.api.get_profile()
                    logger.debug(f"Token validation response: {test_resp}")
                    
                    if test_resp and test_resp.get("s") == "ok":
                        logger.info(f"✓ Fyers token validated successfully for {client_id}")
                    elif test_resp and test_resp.get("s") == "error":
                        error_code = test_resp.get("code")
                        error_msg = test_resp.get("message")
                        logger.error(f"✗ Token validation failed: [{error_code}] {error_msg}")
                        
                        if error_code == -16:
                            logger.error("Authentication error (-16): Token may be expired or invalid format")
                            logger.error(f"Client ID: {client_id}")
                            logger.error(f"Token format: {combined_token[:50]}...")
                            raise ValueError(f"Fyers authentication failed: {error_msg}")
                    else:
                        logger.warning(f"Unexpected validation response: {test_resp}")
                        
                except Exception as test_error:
                    logger.error(f"Token validation error: {test_error}", exc_info=True)
                    # Don't raise here during init, let the actual API call fail with context
                    
            except Exception as e:
                logger.error(f"Failed to initialize Fyers API: {e}", exc_info=True)
                raise
        else:
            logger.warning(
                "fyers_apiv3 library not installed; falling back to raw session"
            )
            self.api = None

    def _format_symbol_for_fyers(self, tradingsymbol, exchange):
        """Format symbol according to Fyers requirements.
        
        Fyers expects format like:
        - NSE:SBIN-EQ for NSE equity
        - BSE:SBIN-EQ for BSE equity  
        - NSE:NIFTY24DEC24000CE for F&O (no -EQ suffix)
        - MCX:GOLD23NOVFUT for commodity derivatives
        """
        if not tradingsymbol:
            return tradingsymbol
            
        # Clean the symbol
        symbol = str(tradingsymbol).upper().strip()
        
        # Handle exchange
        if not exchange:
            exchange = "NSE"
        exchange = str(exchange).upper()

        exchange_aliases = {
            "NSE_EQ": "NSE",
            "BSE_EQ": "BSE",
            "NSE_FNO": "NFO",
            "BSE_FNO": "BFO",
            "NSE_FO": "NFO",
            "BSE_FO": "BFO",
            "NSE_FUT": "NFO",
            "BSE_FUT": "BFO",
        }
        exchange = exchange_aliases.get(exchange, exchange)
        
        # Check if symbol already has exchange prefix
        if ":" in symbol:
            # Symbol already formatted, just ensure proper exchange mapping
            parts = symbol.split(":", 1)
            existing_exchange = exchange_aliases.get(parts[0], parts[0])
            symbol_part = parts[1]
            
            # Map NFO/BFO to NSE/BSE for Fyers
            exchange_map = {
                "NFO": "NSE",
                "BFO": "BSE",
                "CDS": "CDS",
                "MCX": "MCX"
            }
            mapped_exchange = exchange_map.get(existing_exchange, existing_exchange)
            return f"{mapped_exchange}:{symbol_part}"
            
        # Map derivative exchanges for Fyers
        exchange_map = {
            "NFO": "NSE",  # Fyers uses NSE for NFO
            "BFO": "BSE",  # Fyers uses BSE for BFO
            "CDS": "CDS",  # Currency derivatives
            "MCX": "MCX"   # Commodity derivatives
        }
        mapped_exchange = exchange_map.get(exchange, exchange)
        
        # Determine if it's an F&O symbol
        is_fo = any(suffix in symbol for suffix in ["FUT", "CE", "PE"])

        # Attempt to use the precomputed symbol map for equity lookups
        if not is_fo:
            lookup_exchange = exchange
            if lookup_exchange in {"NSE_EQ", "BSE_EQ"}:
                lookup_exchange = lookup_exchange.split("_", 1)[0]
            try:
                mapping = symbol_map.get_symbol_for_broker(
                    symbol, self.BROKER, lookup_exchange
                )
            except Exception:
                mapping = {}
            if (not mapping or not mapping.get("symbol")) and symbol.endswith("-EQ"):
                base_symbol = symbol[:-3]
                try:
                    mapping = symbol_map.get_symbol_for_broker(
                        base_symbol, self.BROKER, lookup_exchange
                    )
                except Exception:
                    mapping = {}
            if mapping and mapping.get("symbol"):
                return mapping["symbol"]
                
        # Remove existing -EQ suffix if present
        if symbol.endswith("-EQ"):
            symbol = symbol[:-3]
        
        if is_fo or exchange in {"NFO", "BFO", "CDS", "MCX"}:
            # F&O or derivatives symbol - don't add -EQ
            logger.debug(f"F&O/Derivative symbol detected: {symbol}, using exchange: {mapped_exchange}")
            return f"{mapped_exchange}:{symbol}"
        else:
            # Equity symbol - add -EQ suffix
            logger.debug(f"Equity symbol detected: {symbol}, adding -EQ suffix for exchange: {mapped_exchange}")
            return f"{mapped_exchange}:{symbol}-EQ"

    def place_order(
        self,
        tradingsymbol=None,
        exchange=None,
        transaction_type=None,
        quantity=None,
        order_type="MARKET",
        product="INTRADAY",
        price=None,
        **kwargs,
    ):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
            
        try:
            # Map generic order fields to Fyers-specific names
            tradingsymbol = tradingsymbol or kwargs.pop("symbol", None)
            transaction_type = transaction_type or kwargs.pop("action", None)
            quantity = quantity or kwargs.pop("qty", None)
            
            # Handle product_type vs product
            product = kwargs.pop("product_type", product) or product or "INTRADAY"
            
            exchange = exchange or kwargs.pop("exchange", None)

            # Normalise common string fields
            if isinstance(transaction_type, str):
                transaction_type = transaction_type.upper()
            if isinstance(order_type, str):
                order_type = order_type.upper()
            if isinstance(product, str):
                product = product.upper()
            if isinstance(exchange, str):
                exchange = exchange.upper()

            # Normalize product and order types
            product = self._normalize_product_type(product)
            order_type = self._normalize_order_type(order_type)
            
            # Map order type to Fyers type code
            order_type_mapping = {
                "MARKET": 2,
                "LIMIT": 1,
                "STOP": 3,
                "STOP_MARKET": 4,
            }
            fy_type = order_type_mapping.get(order_type, 2)  # Default to MARKET

            # Handle price based on order type
            if fy_type in {1, 3}:  # LIMIT or STOP orders
                if price is None:
                    raise ValueError(f"price is required for {order_type} orders")
                limit_price = float(price)
            else:
                limit_price = 0

            # Format symbol for Fyers
            symbol = self._format_symbol_for_fyers(tradingsymbol, exchange)
            
            # Log for debugging
            logger.info(
                f"Fyers order: symbol={symbol}, exchange={exchange}, "
                f"product={product}, order_type={order_type}, "
                f"action={transaction_type}, qty={quantity}, price={price}"
            )

            # Prepare order data
            data = {
                "symbol": symbol,
                "qty": int(quantity),
                "type": fy_type,
                "side": 1 if transaction_type == "BUY" else -1,
                "productType": product,
                "limitPrice": limit_price,
                "disclosedQty": 0,
                "validity": "DAY",
                "offlineOrder": False,
                "stopPrice": 0,
            }
            
            # Handle stop orders
            if order_type in {"STOP", "STOP_MARKET"}:
                trigger_price = kwargs.get("trigger_price") or kwargs.get("stopPrice") or price
                if trigger_price:
                    data["stopPrice"] = float(trigger_price)
            
            result = self.api.place_order(data=data)
            
            # Check result
            if result and result.get("s") == "ok":
                return {
                    "status": "success",
                    "order_id": result.get("id"),
                    "message": result.get("message", "Order placed successfully")
                }
            else:
                error_msg = result.get("message", "Order placement failed") if result else "No response"
                logger.error(f"Fyers order failed: {error_msg}, result: {result}")
                return {
                    "status": "failure",
                    "error": error_msg,
                    "details": result
                }
                
        except Exception as e:
            logger.error(f"Exception in Fyers place_order: {str(e)}", exc_info=True)
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")

        try:
            if hasattr(self.api, "orderbook"):
                orders = self.api.orderbook()
            else:
                return {"status": "failure", "error": "No order book method"}

            if not isinstance(orders, dict):
                return {"status": "failure", "error": "Unexpected response"}

            if str(orders.get("s")).lower() == "error":
                return {
                    "status": "failure",
                    "error": orders.get("message") or "Unknown error",
                    "data": [],
                }
            
            # Get order data
            data = (
                orders.get("orderBook")
                or orders.get("data")
                or orders.get("orders")
                or []
            )

            if isinstance(data, dict):
                data = (
                    data.get("orderBook")
                    or data.get("orders")
                    or []
                )

            # If no orders in orderbook, try tradebook
            if not data and hasattr(self.api, "tradebook"):
                trades = self.api.tradebook()
                if isinstance(trades, dict) and str(trades.get("s")).lower() != "error":
                    data = (
                        trades.get("tradeBook")
                        or trades.get("data")
                        or trades.get("trades")
                        or []
                    )
                    if isinstance(data, dict):
                        data = (
                            data.get("tradeBook")
                            or data.get("trades")
                            or []
                        )

            if not isinstance(data, list):
                data = []

            # Normalize orders
            normalized_orders = []
            for o in data:
                # Extract exchange and symbol
                full_symbol = o.get("symbol", "")
                if ":" in full_symbol:
                    exchange, symbol = full_symbol.split(":", 1)
                else:
                    exchange = "NSE"
                    symbol = full_symbol
                
                # Remove -EQ suffix for display
                if symbol.endswith("-EQ"):
                    symbol = symbol[:-3]
                
                # Map Fyers product types back to standard
                product_type = o.get("productType", "")
                product_map = {
                    "INTRADAY": "MIS",
                    "CNC": "CNC",
                    "MARGIN": "NRML",
                    "BO": "BO",
                    "CO": "CO"
                }
                
                normalized_order = {
                    "action": "BUY" if o.get("side") == 1 else "SELL",
                    "order_type": self._get_order_type_from_code(o.get("type", 2)),
                    "exchange": exchange,
                    "symbol": symbol,
                    "qty": o.get("qty", 0),
                    "price": o.get("limitPrice", 0),
                    "status": o.get("status", ""),
                    "order_id": o.get("id"),
                    "product_type": product_map.get(product_type, product_type),
                }
                normalized_orders.append(normalized_order)

            return {"status": "success", "data": normalized_orders}

        except Exception as e:
            logger.error(f"Exception in get_order_list: {str(e)}", exc_info=True)
            return {"status": "failure", "error": str(e), "data": []}

    def _get_order_type_from_code(self, type_code):
        """Convert Fyers type code back to order type string."""
        type_map = {
            1: "LIMIT",
            2: "MARKET",
            3: "STOP",
            4: "STOP_MARKET"
        }
        return type_map.get(type_code, "MARKET")

    def list_orders(self, **kwargs):
        """Return a list of orders with canonical field names."""
        resp = self.get_order_list(**kwargs)
        if isinstance(resp, dict):
            if resp.get("status") != "success":
                raise RuntimeError(resp.get("error") or "failed to fetch order list")
            orders = resp.get("data", [])
        else:
            orders = resp

        normalized = []
        for o in orders:
            if not isinstance(o, dict):
                continue
            order = dict(o)

            symbol = order.get("symbol") or order.get("tsym")
            if symbol is not None:
                order["symbol"] = str(symbol)

            action = order.get("action") or order.get("trantype")
            if action is not None:
                order["action"] = str(action).upper()

            qty = order.get("qty") or order.get("quantity")
            try:
                order["qty"] = int(qty)
            except (TypeError, ValueError):
                pass

            exchange = order.get("exchange")
            if exchange is not None:
                order["exchange"] = str(exchange).upper()

            order_type = order.get("order_type") or order.get("prctyp") or order.get("type")
            if order_type is not None:
                order["order_type"] = str(order_type).upper()

            status = order.get("status")
            if status is not None:
                order["status"] = str(status).upper()

            order_id = order.get("order_id") or order.get("id") or order.get("orderNumber")
            if order_id is not None:
                order["order_id"] = str(order_id)

            # Ensure product_type is included
            product_type = order.get("product_type") or order.get("productType")
            if product_type is not None:
                order["product_type"] = str(product_type).upper()

            normalized.append(order)

        return normalized

    def get_positions(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            positions = self.api.positions()
            return {"status": "success", "data": positions.get("netPositions", [])}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            result = self.api.cancel_order({"id": order_id})
            if result and result.get("s") == "ok":
                return {"status": "success", "order_id": order_id}
            else:
                return {"status": "failure", "error": result.get("message", "Cancel failed")}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            profile = self.api.get_profile()
            return {"status": "success", "data": profile}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        if self.api is None:
            return False
        try:
            resp = self.api.get_profile()
            if isinstance(resp, dict):
                if str(resp.get("s")).lower() == "ok":
                    code = resp.get("code")
                    if code is None or int(code) >= 0:
                        return True
            return False
        except Exception:
            return False

    def get_opening_balance(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            funds = self.api.funds()
            data = funds.get("fund_limit", funds.get("data", funds))
            
            # Try multiple keys for balance
            for key in ["equityAmount", "cash", "available_balance", "availableCash", "netCash"]:
                if key in data:
                    try:
                        return float(data[key])
                    except (TypeError, ValueError):
                        continue

            # Check in nested structures
            items = funds.get("fund_limit") or funds.get("data") or []
            if isinstance(items, dict):
                items = [items]
            for item in items:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip().lower()
                if title in {"available balance", "clear balance", "cash balance"}:
                    for key in ["equityAmount", "cash", "balance"]:
                        if key in item:
                            try:
                                return float(item[key])
                            except (TypeError, ValueError):
                                continue
            return None
        except Exception:
            return None
            
    # ----- Authentication Helpers -----
    @classmethod
    def login_url(cls, client_id, redirect_uri, state="state123"):
        """Return the Fyers OAuth login URL."""
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state,
        }
        return f"https://api-t1.fyers.in/api/v3/generate-authcode?{urlencode(params)}"

    @classmethod
    def exchange_code_for_token(cls, client_id, secret_key, auth_code):
        """
        Exchange auth code for access and refresh tokens.
        
        The auth_code from Fyers is a JWT token that needs to be exchanged
        for an access_token using the app hash.
        
        Args:
            client_id: Fyers client ID (e.g., WKQ6K175CV-100)
            secret_key: Fyers app secret key
            auth_code: JWT auth code from OAuth redirect
            
        Returns:
            dict: Response with access_token and refresh_token
        """
        # Generate app hash: SHA256(client_id:secret_key)
        app_hash = hashlib.sha256(f"{client_id}:{secret_key}".encode()).hexdigest()
        
        payload = {
            "grant_type": "authorization_code",
            "appIdHash": app_hash,
            "code": auth_code,
        }
        
        logger.info(f"Exchanging auth_code for access_token (client: {client_id})")
        logger.debug(f"App hash: {app_hash[:20]}...")
        logger.debug(f"Auth code length: {len(auth_code)}")
        
        try:
            resp = requests.post(
                "https://api-t1.fyers.in/api/v3/validate-authcode",
                json=payload,
                timeout=10,
            )
            
            logger.debug(f"Exchange response status: {resp.status_code}")
            
            result = resp.json()
            logger.debug(f"Exchange response: {result}")
            
            if result.get("s") == "ok":
                logger.info(f"✓ Successfully exchanged auth_code for access_token")
                # The access_token from Fyers should already be in format client_id:token
                access_token = result.get("access_token", "")
                
                # Verify token format
                if access_token:
                    if ":" in access_token:
                        token_parts = access_token.split(":", 1)
                        logger.debug(f"Token prefix: {token_parts[0]}, length: {len(token_parts[1])}")
                        if token_parts[0].lower() != client_id.lower():
                            logger.warning(f"Token prefix mismatch: expected {client_id}, got {token_parts[0]}")
                            # Fix the prefix
                            access_token = f"{client_id}:{token_parts[1]}"
                            result["access_token"] = access_token
                    else:
                        logger.warning("Token missing client_id prefix, adding it")
                        access_token = f"{client_id}:{access_token}"
                        result["access_token"] = access_token
                else:
                    logger.error("No access_token in successful response")
            else:
                logger.error(f"✗ Token exchange failed: {result.get('message')}")
                logger.error(f"Error code: {result.get('code')}")
                
            return result
            
        except Exception as e:
            logger.error(f"Exception during token exchange: {e}", exc_info=True)
            return {
                "s": "error",
                "code": -1,
                "message": str(e)
            }

    @classmethod
    def refresh_access_token(cls, client_id, secret_key, refresh_token, pin):
        """Refresh the access token using refresh token and pin."""
        app_hash = hashlib.sha256(f"{client_id}:{secret_key}".encode()).hexdigest()
        payload = {
            "grant_type": "refresh_token",
            "appIdHash": app_hash,
            "refresh_token": refresh_token,
            "pin": str(pin),
        }
        resp = requests.post(
            "https://api-t1.fyers.in/api/v3/validate-refresh-token",
            json=payload,
            timeout=10,
        )
        return resp.json()
