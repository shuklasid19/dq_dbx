"""Generate financial test data for DQ Accelerator."""

import argparse
import random
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple

from faker import Faker

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType, BooleanType, LongType
)
import pyspark.sql.functions as F

fake = Faker()


class FinancialDataGenerator:
    """Generate financial test data."""
    
    def __init__(self, spark: SparkSession, catalog: str = "main"):
        """
        Initialize data generator.
        
        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
        """
        self.spark = spark
        self.catalog = catalog
        self.fake = Faker()
        random.seed(42)
        Faker.seed(42)
    
    def create_schemas(self):
        """Create schemas for bronze, silver, and gold layers."""
        for schema in ["bronze", "silver", "gold"]:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema}")
            print(f"✓ Created schema: {self.catalog}.{schema}")
    
    def generate_customers(self, num_customers: int = 1000) -> None:
        """Generate customer data."""
        print(f"\nGenerating {num_customers} customers...")
        
        customers_data = []
        for i in range(1, num_customers + 1):
            first_name = fake.first_name()
            last_name = fake.last_name()
            
            # Introduce some data quality issues (5%)
            if random.random() < 0.05:
                email = f"invalid_{i}"  # Invalid email
            else:
                email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"
            
            if random.random() < 0.03:
                phone = "123"  # Invalid phone
            else:
                phone = fake.phone_number()
            
            customers_data.append((
                i,  # customer_id
                first_name,
                last_name,
                email,
                phone,
                fake.address().replace('\n', ', '),
                fake.city(),
                fake.state_abbr(),
                fake.zipcode(),
                fake.date_of_birth(minimum_age=18, maximum_age=85),
                fake.ssn(),
                random.choice(["PENDING", "VERIFIED", "REJECTED", "EXPIRED"]),
                datetime.now() - timedelta(days=random.randint(1, 1000)),
                datetime.now()
            ))
        
        schema = StructType([
            StructField("customer_id", LongType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("ssn", StringType(), True),
            StructField("kyc_status", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ])
        
        df = self.spark.createDataFrame(customers_data, schema=schema)
        
        # Write to raw table
        df.write.mode("overwrite").saveAsTable(f"{self.catalog}.bronze.customers")
        print(f"✓ Created {self.catalog}.bronze.customers with {df.count()} records")
        
        return customers_data
    
    def generate_accounts(self, customers: List, num_accounts_per_customer: Tuple[int, int] = (1, 3)) -> None:
        """Generate account data."""
        print(f"\nGenerating accounts...")
        
        accounts_data = []
        account_id = 1
        
        for customer in customers:
            customer_id = customer[0]
            num_accounts = random.randint(*num_accounts_per_customer)
            
            for _ in range(num_accounts):
                account_type = random.choice(["SAVINGS", "CHECKING", "CREDIT", "INVESTMENT", "LOAN"])
                status = random.choice(["ACTIVE", "INACTIVE", "CLOSED", "SUSPENDED"])
                
                # Account number - with some quality issues
                if random.random() < 0.02:
                    account_number = f"ACC{random.randint(1, 999)}"  # Too short
                else:
                    account_number = f"ACC{account_id:012d}"
                
                opened_date = datetime.now() - timedelta(days=random.randint(30, 3650))
                
                # Closed date logic
                if status == "CLOSED":
                    closed_date = opened_date + timedelta(days=random.randint(30, 1000))
                else:
                    closed_date = None
                
                # Balance generation
                if account_type == "SAVINGS":
                    balance = Decimal(random.uniform(100, 50000))
                elif account_type == "CHECKING":
                    balance = Decimal(random.uniform(-1000, 10000))
                elif account_type == "CREDIT":
                    balance = Decimal(random.uniform(-5000, 0))
                elif account_type == "INVESTMENT":
                    balance = Decimal(random.uniform(1000, 100000))
                else:  # LOAN
                    balance = Decimal(random.uniform(-50000, -1000))
                
                accounts_data.append((
                    account_id,
                    customer_id,
                    account_number,
                    account_type,
                    status,
                    balance,
                    random.choice(["USD", "EUR", "GBP", "JPY"]),
                    opened_date,
                    closed_date,
                    fake.iban(),
                    datetime.now(),
                    datetime.now()
                ))
                
                account_id += 1
        
        schema = StructType([
            StructField("account_id", LongType(), False),
            StructField("customer_id", LongType(), False),
            StructField("account_number", StringType(), False),
            StructField("account_type", StringType(), False),
            StructField("status", StringType(), False),
            StructField("balance", DecimalType(18, 2), False),
            StructField("currency_code", StringType(), False),
            StructField("opened_date", TimestampType(), False),
            StructField("closed_date", TimestampType(), True),
            StructField("iban", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ])
        
        df = self.spark.createDataFrame(accounts_data, schema=schema)
        df.write.mode("overwrite").saveAsTable(f"{self.catalog}.bronze.accounts")
        print(f"✓ Created {self.catalog}.bronze.accounts with {df.count()} records")
        
        return accounts_data
    
    def generate_transactions(self, accounts: List, days: int = 90, txn_per_day_range: Tuple[int, int] = (5, 50)) -> None:
        """Generate transaction data."""
        print(f"\nGenerating transactions for {days} days...")
        
        transactions_data = []
        transaction_id = 1
        
        start_date = datetime.now() - timedelta(days=days)
        
        for day in range(days):
            current_date = start_date + timedelta(days=day)
            num_txns = random.randint(*txn_per_day_range)
            
            for _ in range(num_txns):
                account = random.choice(accounts)
                account_id = account[0]
                account_type = account[3]
                currency_code = account[6]
                
                # Transaction type based on account type
                if account_type == "SAVINGS":
                    txn_type = random.choice(["DEPOSIT", "WITHDRAWAL", "INTEREST"])
                elif account_type == "CHECKING":
                    txn_type = random.choice(["DEPOSIT", "WITHDRAWAL", "PAYMENT", "FEE", "TRANSFER"])
                elif account_type == "CREDIT":
                    txn_type = random.choice(["PAYMENT", "PURCHASE", "FEE", "INTEREST"])
                else:
                    txn_type = random.choice(["PAYMENT", "DEPOSIT"])
                
                # Amount generation with quality issues
                if random.random() < 0.02:
                    amount = None  # NULL amount
                elif random.random() < 0.01:
                    amount = Decimal(random.uniform(-1000, -100))  # Negative amount
                else:
                    if txn_type in ["DEPOSIT", "PAYMENT"]:
                        amount = Decimal(random.uniform(10, 5000))
                    elif txn_type == "WITHDRAWAL":
                        amount = Decimal(random.uniform(20, 2000))
                    elif txn_type == "FEE":
                        amount = Decimal(random.uniform(1, 50))
                    else:
                        amount = Decimal(random.uniform(5, 1000))
                
                # Transaction timestamp
                txn_timestamp = current_date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                # Currency code - introduce some mismatches
                if random.random() < 0.01:
                    txn_currency = random.choice(["XXX", "ZZZ"])  # Invalid currency
                else:
                    txn_currency = currency_code
                
                transactions_data.append((
                    transaction_id,
                    account_id,
                    txn_type,
                    amount,
                    txn_currency,
                    txn_timestamp,
                    current_date.date(),
                    fake.text(max_nb_chars=50),
                    fake.company() if random.random() < 0.3 else None,
                    random.choice(["COMPLETED", "PENDING", "FAILED"]),
                    datetime.now()
                ))
                
                transaction_id += 1
        
        schema = StructType([
            StructField("transaction_id", LongType(), False),
            StructField("account_id", LongType(), False),
            StructField("transaction_type", StringType(), False),
            StructField("amount", DecimalType(18, 2), True),
            StructField("currency_code", StringType(), False),
            StructField("transaction_timestamp", TimestampType(), False),
            StructField("transaction_date", DateType(), False),
            StructField("description", StringType(), True),
            StructField("merchant", StringType(), True),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), False),
        ])
        
        df = self.spark.createDataFrame(transactions_data, schema=schema)
        df.write.mode("overwrite").saveAsTable(f"{self.catalog}.bronze.transactions")
        print(f"✓ Created {self.catalog}.bronze.transactions with {df.count()} records")
    
    def generate_all(self, num_customers: int = 1000, days: int = 90):
        """Generate all test data."""
        print(f"\n{'='*80}")
        print("FINANCIAL DATA GENERATION")
        print(f"{'='*80}")
        
        self.create_schemas()
        customers = self.generate_customers(num_customers)
        accounts = self.generate_accounts(customers)
        self.generate_transactions(accounts, days=days)
        
        print(f"\n{'='*80}")
        print("✅ DATA GENERATION COMPLETE")
        print(f"{'='*80}")
        print(f"\nGenerated tables in catalog '{self.catalog}':")
        print(f"  - {self.catalog}.bronze.customers")
        print(f"  - {self.catalog}.bronze.accounts")
        print(f"  - {self.catalog}.bronze.transactions")
        print(f"\nNext steps:")
        print(f"  1. Run DQ checks on bronze tables")
        print(f"  2. Transform to silver layer (dimensions and facts)")
        print(f"  3. Create gold layer aggregates")
        print(f"  4. Set up dashboard monitoring")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate financial test data")
    parser.add_argument(
        "--catalog",
        default="main",
        help="Unity Catalog name (default: main)"
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=1000,
        help="Number of customers to generate (default: 1000)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days of transaction history (default: 90)"
    )
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("Financial-Data-Generator")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        generator = FinancialDataGenerator(spark, catalog=args.catalog)
        generator.generate_all(
            num_customers=args.customers,
            days=args.days
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
