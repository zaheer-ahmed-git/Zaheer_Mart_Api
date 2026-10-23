"""End-to-end smoke test against the running docker compose stack.

Calls the services directly on their published ports (Kong is not required).
Usage: python e2e_smoke.py
"""
import sys
import time
import uuid

import httpx

USER = "http://localhost:8007"
ORDER = "http://localhost:8003"
PRODUCT = "http://localhost:8002"
INVENTORY = "http://localhost:8005"
PAYMENT = "http://localhost:8006"

failures = []


def check(name, ok, detail=""):
    print(("PASS" if ok else "FAIL"), name, detail)
    if not ok:
        failures.append(name)


def wait_for(url, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=3).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(3)
    return False


def poll(fn, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(1)
    return None


def main():
    for name, url in [("user", USER), ("order", ORDER), ("product", PRODUCT),
                      ("inventory", INVENTORY), ("payment", PAYMENT)]:
        check(f"{name} service up", wait_for(url + "/"))
    if failures:
        sys.exit(1)

    suffix = uuid.uuid4().hex[:6]
    username = f"tester{suffix}"
    email = f"{username}@example.com"
    password = "P@ssw0rd-" + suffix

    # 1. register (user is created asynchronously through Kafka)
    r = httpx.post(USER + "/user/register", json={
        "username": username, "email": email, "password": password,
        "shipping_address": "1 Test St", "phone": 5555555555,
    }, timeout=60)
    check("register", r.status_code == 200, f"{r.status_code} {r.text[:120]}")
    check("register hides password", "password" not in r.text)

    # 2. login through the user service
    r = httpx.post(USER + "/user/login", data={"username": username, "password": password}, timeout=30)
    check("login", r.status_code == 200 and "access_token" in r.json(), str(r.status_code))
    token = r.json().get("access_token", "")
    auth = {"Authorization": f"Bearer {token}"}

    # 3. login through the order service (request/reply over Kafka)
    r = httpx.post(ORDER + "/user/login", data={"username": username, "password": password}, timeout=30)
    check("login via order service (kafka req/reply)", r.status_code == 200, str(r.status_code))
    auth_order = {"Authorization": f"Bearer {r.json().get('access_token', '')}"}

    # 4. product + stock
    product_id = "prod-" + suffix
    r = httpx.post(PRODUCT + "/products/", json={
        "product_id": product_id, "name": "Shirt", "description": "Blue",
        "price": 10.0, "is_available": True, "category": "clothes",
    }, timeout=30)
    check("create product", r.status_code == 202, str(r.status_code))
    created = poll(lambda: httpx.get(PRODUCT + f"/products/{product_id}/details").status_code == 200)
    check("product stored by consumer", bool(created))

    # inventory row is created from the product event; then we add stock
    poll(lambda: httpx.get(INVENTORY + "/inventory/").json() and any(
        i["product_id"] == product_id for i in httpx.get(INVENTORY + "/inventory/").json()))
    r = httpx.put(INVENTORY + f"/inventory/{product_id}/update_stock?quantity_change=5", timeout=30)
    check("add stock request", r.status_code == 202, str(r.status_code))
    stock_ok = poll(lambda: any(
        i["product_id"] == product_id and i["stock_level"] == 5
        for i in httpx.get(INVENTORY + "/inventory/").json()))
    check("stock is 5", bool(stock_ok))

    # 5. create an order (async) and wait for it to be accepted
    r = httpx.post(ORDER + "/order", json={
        "product_id": product_id, "quantity": 2, "shipping_address": "1 Test St",
    }, headers=auth_order, timeout=30)
    check("create order accepted (202)", r.status_code == 202, str(r.status_code))
    order_id = r.json().get("order_id", "")
    order = poll(lambda: (lambda o: o if o.status_code == 200 else None)(
        httpx.get(ORDER + f"/order/{order_id}", headers=auth_order)), timeout=40)
    check("order stored", order is not None)
    if order is not None:
        check("order In_progress", order.json()["order_status"] == "In_progress", order.json()["order_status"])

    # 6. stock was reserved (5 - 2 = 3)
    reserved = poll(lambda: any(
        i["product_id"] == product_id and i["stock_level"] == 3
        for i in httpx.get(INVENTORY + "/inventory/").json()))
    check("stock reserved (5 -> 3)", bool(reserved))

    # 7. over-order is rejected
    r = httpx.post(ORDER + "/order", json={
        "product_id": product_id, "quantity": 50, "shipping_address": "x",
    }, headers=auth_order, timeout=30)
    over_id = r.json().get("order_id", "")
    over = poll(lambda: (lambda o: o if o.status_code == 200 else None)(
        httpx.get(ORDER + f"/order/{over_id}", headers=auth_order)), timeout=40)
    check("over-order rejected", over is not None and over.json()["order_status"] == "Rejected",
          str(over.json().get("order_status") if over is not None else "not stored"))

    # 8. payment: a foreign / unknown order is refused (no real Stripe call)
    r = httpx.put(PAYMENT + "/payment", json={
        "order_id": "does-not-exist", "payment_method_id": "pm_card_visa", "amount": 2000,
    }, headers=auth, timeout=60)
    check("payment on unknown order is 404", r.status_code == 404, str(r.status_code))

    # 9. protected routes refuse anonymous calls
    check("orders need a token", httpx.get(ORDER + "/orders").status_code == 401)
    check("users list needs a token", httpx.get(USER + "/users/all").status_code == 401)

    print()
    if failures:
        print(f"{len(failures)} FAILED: {failures}")
        sys.exit(1)
    print("ALL E2E CHECKS PASSED")


if __name__ == "__main__":
    main()
