from faker import Faker
import pandas as pd
import random
import os
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
Faker.seed(42)

os.makedirs("data", exist_ok=True)

BASE_DATE = datetime(2024, 1, 1)

def rdate(days=364):
    return (BASE_DATE + timedelta(days=random.randint(0, days))).strftime("%Y-%m-%d")

def generate_healthcare(n=25000):
    MEDICINES = ["Paracetamol", "Amoxicillin", "Metformin", "Ibuprofen", "Omeprazole", "Amlodipine", "Azithromycin", "Cetirizine", "Insulin", "Vitamin D3"]
    CATEGORIES = ["Antibiotic", "Painkiller", "Antidiabetic", "Antihypertensive", "Vitamin"]
    BRANCHES = ["Branch_A", "Branch_B", "Branch_C", "Branch_D"]
    SUPPLIERS = ["MedSupply Co", "PharmaDist Ltd", "HealthBridge Inc", "CureMed Traders"]
    records = []
    for i in range(n):
        stock = random.randint(0, 1000)
        exp_days = random.randint(-90, 730)
        status = "Expired" if exp_days < 0 else ("Low Stock" if stock < 20 else "In Stock")
        records.append({
            "record_id": i + 1,
            "domain": "HEALTHCARE",
            "record_date": rdate(),
            "entity_id": f"MED{str(i+1).zfill(6)}",
            "entity_name": random.choice(MEDICINES),
            "category": random.choice(CATEGORIES),
            "branch": random.choice(BRANCHES),
            "supplier": random.choice(SUPPLIERS),
            "quantity": stock,
            "unit_price": round(random.uniform(5, 800), 2),
            "expiry_date": (BASE_DATE + timedelta(days=exp_days)).strftime("%Y-%m-%d"),
            "status": status,
            "anomaly_flag": 1 if status in ["Expired", "Low Stock"] else 0,
        })
    return pd.DataFrame(records)

def generate_banking(n=25000):
    PURPOSES = ["Home Loan", "Car Loan", "Education Loan", "Personal Loan", "Business Loan"]
    BANKS = ["SBI", "HDFC", "ICICI", "Axis Bank", "Canara Bank", "Kotak Mahindra"]
    STATES = ["Tamil Nadu", "Karnataka", "Maharashtra", "Delhi", "Gujarat", "Rajasthan"]
    records = []
    for i in range(n):
        income = random.randint(120000, 5000000)
        loan_amt = random.randint(50000, 5000000)
        tenure = random.choice([12, 24, 36, 48, 60, 84, 120])
        cscore = random.randint(300, 900)
        missed = random.randint(0, 12)
        emi = round(loan_amt / tenure, 2)
        dti = round((emi * 12) / income, 4)
        rs = 0
        if cscore < 550:
            rs += 3
        elif cscore < 650:
            rs += 2
        elif cscore < 750:
            rs += 1
        if dti > 0.5:
            rs += 3
        elif dti > 0.35:
            rs += 2
        if missed > 6:
            rs += 3
        elif missed > 2:
            rs += 2
        risk = "HIGH" if rs >= 8 else ("MEDIUM" if rs >= 4 else "LOW")
        records.append({
            "record_id": i + 1,
            "domain": "BANKING",
            "record_date": rdate(),
            "entity_id": f"APP{str(i+1).zfill(6)}",
            "entity_name": fake.name(),
            "category": random.choice(PURPOSES),
            "branch": random.choice(BANKS),
            "supplier": random.choice(STATES),
            "quantity": tenure,
            "unit_price": loan_amt,
            "expiry_date": rdate(730),
            "status": risk,
            "anomaly_flag": 1 if risk == "HIGH" else 0,
        })
    return pd.DataFrame(records)

def generate_social(n=25000):
    BRANDS = ["Samsung", "Apple", "Zomato", "Swiggy", "Flipkart", "Amazon"]
    PLATFORMS = ["Twitter", "Instagram", "Facebook", "YouTube", "LinkedIn"]
    LOCATIONS = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Salem"]
    POS = ["Love {b}! Best ever.", "Amazing quality from {b}.", "Fast delivery by {b}!"]
    NEG = ["Disappointed with {b}.", "Worst support from {b}.", "Never buying from {b} again."]
    NEU = ["Checking out {b} offers.", "Anyone tried {b}?", "Comparing {b} with others."]
    records = []
    for i in range(n):
        brand = random.choice(BRANDS)
        sent = random.choices(["POSITIVE", "NEGATIVE", "NEUTRAL"], [0.4, 0.3, 0.3])[0]
        tmpl = random.choice(POS if sent == "POSITIVE" else (NEG if sent == "NEGATIVE" else NEU))
        pol = round(random.uniform(0.3, 1.0) if sent == "POSITIVE" else (random.uniform(-1.0, -0.3) if sent == "NEGATIVE" else random.uniform(-0.2, 0.2)), 3)
        records.append({
            "record_id": i + 1,
            "domain": "SOCIAL_MEDIA",
            "record_date": rdate(),
            "entity_id": f"POST{str(i+1).zfill(7)}",
            "entity_name": brand,
            "category": random.choice(PLATFORMS),
            "branch": random.choice(LOCATIONS),
            "supplier": fake.user_name(),
            "quantity": random.randint(0, 50000),
            "unit_price": pol,
            "expiry_date": rdate(),
            "status": sent,
            "anomaly_flag": 1 if sent == "NEGATIVE" else 0,
        })
    return pd.DataFrame(records)

def generate_billing(n=25000):
    DEPTS = ["Cardiology", "Orthopedics", "Neurology", "Oncology", "Pediatrics", "Emergency"]
    INSURERS = ["StarHealth", "NationalInsurance", "UnitedHealth", "BajajAllianz", "HDFCErgo"]
    HOSPS = ["Apollo Salem", "Kaveri Hospital", "SKS Hospital", "Vinayaga Hospital"]
    records = []
    for i in range(n):
        bill = round(random.uniform(500, 250000), 2)
        roll = random.random()
        claim = round(bill * (random.uniform(0.3, 0.55) if roll < 0.10 else (random.uniform(0.56, 0.74) if roll < 0.30 else random.uniform(0.75, 0.98))), 2)
        anom = "HIGH" if roll < 0.10 else ("MEDIUM" if roll < 0.30 else "NORMAL")
        records.append({
            "record_id": i + 1,
            "domain": "HEALTHCARE_BILLING",
            "record_date": rdate(),
            "entity_id": f"BILL{str(i+1).zfill(6)}",
            "entity_name": random.choice(DEPTS),
            "category": random.choice(INSURERS),
            "branch": random.choice(HOSPS),
            "supplier": fake.name(),
            "quantity": random.randint(1, 30),
            "unit_price": bill,
            "expiry_date": rdate(),
            "status": anom,
            "anomaly_flag": 0 if anom == "NORMAL" else 1,
        })
    return pd.DataFrame(records)

if __name__ == "__main__":
    print("=" * 50)
    print("Unified Enterprise Data Lake Data Generator")
    print("Generating 100,000 records across 4 domains")
    print("=" * 50)

    domains = [
        ("HEALTHCARE", generate_healthcare, "data/healthcare_100k.csv"),
        ("BANKING", generate_banking, "data/banking_100k.csv"),
        ("SOCIAL_MEDIA", generate_social, "data/social_media_100k.csv"),
        ("BILLING", generate_billing, "data/billing_100k.csv"),
    ]

    total = 0
    for name, func, path in domains:
        print(f"\n{name} generating 25,000 records...")
        df = func(25000)
        df.to_csv(path, index=False)
        total += len(df)
        print(f"Done -> {path}")

    print(f"\nTotal records generated: {total}")
    print("All 4 domain files saved in data/ folder")
