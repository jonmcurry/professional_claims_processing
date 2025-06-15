#!/usr/bin/env python3
"""
Sample Claims Data Generator
Generates 100,000 realistic healthcare claims and loads them into PostgreSQL staging database.
Updated to use integer facility IDs to match the original schema.
"""

import asyncio
import logging
import random
import uuid
import json  # Added import for JSON handling
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List

import asyncpg
from faker import Faker

from src.config.config import load_config
from src.db.postgres import PostgresDatabase

# Initialize Faker for realistic data generation
fake = Faker()

# Healthcare-specific data pools - UPDATED: Use integer facility IDs
FACILITY_IDS = list(range(1, 51))  # 50 facilities: 1-50 instead of FAC001-FAC050
FINANCIAL_CLASSES = ["COM", "MED", "MCR", "PPO", "HMO", "WRK", "OTH", "SELF"]
GENDER_CODES = ["M", "F"]
PLACES_OF_SERVICE = ["11", "12", "21", "22", "23", "31", "32", "49", "50", "81"]

# Common procedure codes (CPT codes)
PROCEDURE_CODES = [
    "99213",
    "99214",
    "99215",
    "99203",
    "99204",
    "99205",  # Office visits
    "99282",
    "99283",
    "99284",
    "99285",  # Emergency visits
    "36415",
    "85025",
    "80053",
    "80061",  # Lab procedures
    "71020",
    "71030",
    "73060",
    "73070",  # Radiology
    "29881",
    "29882",
    "64483",
    "64484",  # Surgery
    "90834",
    "90837",
    "90853",
    "96116",  # Mental health
    "G0438",
    "G0439",
    "99490",
    "99491",  # Preventive care
]

# Common diagnosis codes (ICD-10)
DIAGNOSIS_CODES = [
    "Z00.00",
    "Z12.11",
    "I10",
    "E11.9",
    "M79.3",  # Routine/common
    "J44.1",
    "N39.0",
    "K21.9",
    "M25.561",
    "F41.1",  # Chronic conditions
    "S72.001A",
    "M17.11",
    "I25.10",
    "E78.5",
    "Z51.11",  # Acute/treatment
    "G89.29",
    "M54.5",
    "R06.02",
    "R50.9",
    "K59.00",  # Symptoms
]

# Modifiers
MODIFIERS = ["", "25", "59", "76", "77", "78", "79", "LT", "RT", "50"]

# Revenue codes
REVENUE_CODES = ["0450", "0636", "0320", "0370", "0410", "0730", "0942"]


@dataclass
class GeneratedClaim:
    """Container for a generated claim with all related data"""

    claim_data: Dict[str, Any]
    line_items: List[Dict[str, Any]]
    diagnosis_codes: List[Dict[str, Any]]


class SampleDataGenerator:
    """Generates realistic healthcare claims data"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.patient_accounts = set()

    def generate_patient_account_number(self) -> str:
        """Generate a unique patient account number"""
        while True:
            account = f"PAT{random.randint(100000, 999999)}"
            if account not in self.patient_accounts:
                self.patient_accounts.add(account)
                return account

    def generate_claim_data(self) -> Dict[str, Any]:
        """Generate a single claim's base data"""
        claim_id = f"CLM{uuid.uuid4().hex[:8].upper()}"
        facility_id = random.choice(FACILITY_IDS)  # Now returns integer (1-50)
        patient_account = self.generate_patient_account_number()

        # Generate realistic dates
        service_from = fake.date_between(start_date="-730d", end_date="today")
        
        # 80% single-day claims, 20% multi-day (up to 30 days)
        if random.random() < 0.8:
            service_to = service_from
        else:
            service_to = service_from + timedelta(days=random.randint(1, 30))

        # Patient demographics
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=95)
        gender = random.choice(GENDER_CODES)
        first_name = fake.first_name_male() if gender == "M" else fake.first_name_female()
        last_name = fake.last_name()
        patient_name = f"{last_name}, {first_name}"

        return {
            "claim_id": claim_id,
            "facility_id": facility_id,  # Integer value (1-50)
            "department_id": random.randint(100, 999),
            "patient_account_number": patient_account,
            "patient_name": patient_name,
            "first_name": first_name,
            "last_name": last_name,
            "medical_record_number": f"MRN{random.randint(1000000, 9999999)}",
            "date_of_birth": birth_date,
            "gender": gender,
            "service_from_date": service_from,
            "service_to_date": service_to,
            "primary_diagnosis": random.choice(DIAGNOSIS_CODES),
            "financial_class": random.choice(FINANCIAL_CLASSES),
            "secondary_insurance": random.choice(FINANCIAL_CLASSES) if random.random() < 0.3 else None,
            "total_charge_amount": Decimal("0.00"),  # Will be calculated from line items
            "processing_status": "pending",
            "processing_stage": "intake",
            "active": True,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "raw_data": json.dumps({"source": "sample_generator", "version": "1.0"}),
            "validation_results": None,
            "ml_predictions": None,
            "processing_metrics": None,
            "error_details": None,
            "priority": random.choice(["low", "normal", "high"]),
            "submitted_by": "data_generator",
            "correlation_id": str(uuid.uuid4()),
        }

    def generate_line_items(self, claim_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate line items for a claim"""
        # 1-5 line items per claim, weighted toward 1-2
        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
        
        line_items = []
        total_charge = Decimal("0.00")

        for line_num in range(1, num_items + 1):
            procedure_code = random.choice(PROCEDURE_CODES)
            units = random.randint(1, 3)
            
            # Generate realistic charge amounts based on procedure type
            if procedure_code.startswith("99"):  # Office visits
                base_charge = Decimal(random.uniform(200, 600))
            elif procedure_code.startswith("8"):  # Labs
                base_charge = Decimal(random.uniform(50, 300))
            elif procedure_code.startswith("7"):  # Radiology
                base_charge = Decimal(random.uniform(300, 1500))
            elif procedure_code.startswith("2") or procedure_code.startswith("6"):  # Surgery
                base_charge = Decimal(random.uniform(1000, 5000))
            else:  # Other
                base_charge = Decimal(random.uniform(100, 800))

            charge_amount = base_charge * units
            total_charge += charge_amount

            # Calculate RVU and reimbursement
            rvu_value = Decimal(random.uniform(0.5, 5.0))
            reimbursement_amount = rvu_value * units * Decimal("36.04")  # Conversion factor

            line_item = {
                "claim_id": claim_data["claim_id"],
                "line_number": line_num,
                "procedure_code": procedure_code,
                "modifier1": random.choice(MODIFIERS),
                "modifier2": random.choice(MODIFIERS) if random.random() < 0.2 else None,
                "modifier3": random.choice(MODIFIERS) if random.random() < 0.1 else None,
                "modifier4": random.choice(MODIFIERS) if random.random() < 0.05 else None,
                "units": units,
                "charge_amount": charge_amount,
                "service_from_date": claim_data["service_from_date"],
                "service_to_date": claim_data["service_to_date"],
                "diagnosis_pointer": "1",  # Points to primary diagnosis
                "place_of_service": random.choice(PLACES_OF_SERVICE),
                "revenue_code": random.choice(REVENUE_CODES),
                "created_at": datetime.now(),
                "rvu_value": rvu_value,
                "reimbursement_amount": reimbursement_amount,
            }
            line_items.append(line_item)

        # Update total charge amount in claim
        claim_data["total_charge_amount"] = total_charge

        return line_items

    def generate_diagnosis_codes(self, claim_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate diagnosis codes for a claim"""
        # 1-4 diagnoses per claim, weighted toward 1-2
        num_diagnoses = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
        
        diagnosis_codes = []
        used_codes = set()

        for seq in range(1, num_diagnoses + 1):
            # Ensure unique diagnosis codes per claim
            while True:
                diag_code = random.choice(DIAGNOSIS_CODES)
                if diag_code not in used_codes:
                    used_codes.add(diag_code)
                    break

            diagnosis = {
                "claim_id": claim_data["claim_id"],
                "diagnosis_sequence": seq,
                "diagnosis_code": diag_code,
                "diagnosis_description": f"Description for {diag_code}",
                "diagnosis_type": "primary" if seq == 1 else "secondary",
                "created_at": datetime.now(),
                # UPDATED: Add service_to_date for partitioned table compatibility
                "service_to_date": claim_data["service_to_date"],
            }
            diagnosis_codes.append(diagnosis)

        return diagnosis_codes

    def generate_batch(self, batch_size: int) -> List[GeneratedClaim]:
        """Generate a batch of claims"""
        claims = []
        
        for _ in range(batch_size):
            claim_data = self.generate_claim_data()
            line_items = self.generate_line_items(claim_data)
            diagnosis_codes = self.generate_diagnosis_codes(claim_data)
            
            claims.append(GeneratedClaim(
                claim_data=claim_data,
                line_items=line_items,
                diagnosis_codes=diagnosis_codes
            ))
        
        return claims


class DataLoader:
    """Loads generated claims data into PostgreSQL database"""

    def __init__(self, db: PostgresDatabase):
        self.db = db
        self.logger = logging.getLogger(__name__)

    async def load_claims_batch(self, claims: List[GeneratedClaim]) -> int:
        """Load a batch of claims into the database"""
        claims_data = []
        line_items_data = []
        diagnosis_codes_data = []

        # Prepare data for bulk insert
        for claim in claims:
            # Prepare claim data
            claim_tuple = (
                claim.claim_data["claim_id"],
                claim.claim_data["facility_id"],  # Integer value
                claim.claim_data["department_id"],
                claim.claim_data["patient_account_number"],
                claim.claim_data["patient_name"],
                claim.claim_data["first_name"],
                claim.claim_data["last_name"],
                claim.claim_data["medical_record_number"],
                claim.claim_data["date_of_birth"],
                claim.claim_data["gender"],
                claim.claim_data["service_from_date"],
                claim.claim_data["service_to_date"],
                claim.claim_data["primary_diagnosis"],
                claim.claim_data["financial_class"],
                claim.claim_data["secondary_insurance"],
                claim.claim_data["total_charge_amount"],
                claim.claim_data["processing_status"],
                claim.claim_data["processing_stage"],
                claim.claim_data["active"],
                claim.claim_data["created_at"],
                claim.claim_data["updated_at"],
                claim.claim_data["raw_data"],
                claim.claim_data["validation_results"],
                claim.claim_data["ml_predictions"],
                claim.claim_data["processing_metrics"],
                claim.claim_data["error_details"],
                claim.claim_data["priority"],
                claim.claim_data["submitted_by"],
                claim.claim_data["correlation_id"],
            )
            claims_data.append(claim_tuple)

            # Prepare line items
            for item in claim.line_items:
                line_tuple = (
                    item["claim_id"],
                    item["line_number"],
                    item["procedure_code"],
                    item["modifier1"],
                    item["modifier2"],
                    item["modifier3"],
                    item["modifier4"],
                    item["units"],
                    item["charge_amount"],
                    item["service_from_date"],
                    item["service_to_date"],
                    item["diagnosis_pointer"],
                    item["place_of_service"],
                    item["revenue_code"],
                    item["created_at"],
                    item["rvu_value"],
                    item["reimbursement_amount"],
                )
                line_items_data.append(line_tuple)

            # Prepare diagnosis codes
            for diagnosis in claim.diagnosis_codes:
                diag_tuple = (
                    diagnosis["claim_id"],
                    diagnosis["service_to_date"],  # UPDATED: Include service_to_date for partitioned table
                    diagnosis["diagnosis_sequence"],
                    diagnosis["diagnosis_code"],
                    diagnosis["diagnosis_description"],
                    diagnosis["diagnosis_type"],
                    diagnosis["created_at"],
                )
                diagnosis_codes_data.append(diag_tuple)

        # Bulk insert using PostgreSQL COPY
        try:
            # Insert claims
            claims_columns = [
                "claim_id",
                "facility_id",
                "department_id",
                "patient_account_number",
                "patient_name",
                "first_name",
                "last_name",
                "medical_record_number",
                "date_of_birth",
                "gender",
                "service_from_date",
                "service_to_date",
                "primary_diagnosis",
                "financial_class",
                "secondary_insurance",
                "total_charge_amount",
                "processing_status",
                "processing_stage",
                "active",
                "created_at",
                "updated_at",
                "raw_data",
                "validation_results",
                "ml_predictions",
                "processing_metrics",
                "error_details",
                "priority",
                "submitted_by",
                "correlation_id",
            ]

            claims_inserted = await self.db.copy_records(
                "claims", claims_columns, claims_data
            )
            self.logger.info(f"Inserted {claims_inserted} claims")

            # Insert line items
            line_items_columns = [
                "claim_id",
                "line_number",
                "procedure_code",
                "modifier1",
                "modifier2",
                "modifier3",
                "modifier4",
                "units",
                "charge_amount",
                "service_from_date",
                "service_to_date",
                "diagnosis_pointer",
                "place_of_service",
                "revenue_code",
                "created_at",
                "rvu_value",
                "reimbursement_amount",
            ]

            lines_inserted = await self.db.copy_records(
                "claims_line_items", line_items_columns, line_items_data
            )
            self.logger.info(f"Inserted {lines_inserted} line items")

            # Insert diagnosis codes
            diagnosis_columns = [
                "claim_id",
                "service_to_date",  # UPDATED: Include service_to_date column
                "diagnosis_sequence",
                "diagnosis_code",
                "diagnosis_description",
                "diagnosis_type",
                "created_at",
            ]

            diag_inserted = await self.db.copy_records(
                "claims_diagnosis_codes", diagnosis_columns, diagnosis_codes_data
            )
            self.logger.info(f"Inserted {diag_inserted} diagnosis codes")

            return claims_inserted

        except Exception as e:
            self.logger.error(f"Error inserting batch: {e}")
            raise


async def create_batch_metadata(db: PostgresDatabase, total_claims: int) -> str:
    """Create a batch metadata record for tracking"""
    batch_id = f"GEN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    await db.execute(
        """
        INSERT INTO batch_metadata (
            batch_id, submitted_by, submitted_at, total_claims,
            status, priority, processing_options
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        batch_id,
        "data_generator",
        datetime.now(),
        total_claims,
        "completed",
        "normal",
        # FIXED: Use json.dumps instead of asyncpg.types.Json
        json.dumps({"generated": True, "source": "sample_data_generator"}),
    )

    return batch_id


async def generate_and_load_sample_data(
    total_claims: int = 100000, batch_size: int = 1000
):
    """Main function to generate and load sample claims data"""

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info(f"Starting generation of {total_claims:,} sample claims...")

    # Load configuration and connect to database
    config = load_config()
    db = PostgresDatabase(config.postgres)

    try:
        await db.connect()
        logger.info("Connected to PostgreSQL database")

        # Create batch metadata
        batch_id = await create_batch_metadata(db, total_claims)
        logger.info(f"Created batch metadata: {batch_id}")

        # Initialize generator and loader
        generator = SampleDataGenerator()
        loader = DataLoader(db)

        # Process in batches
        total_loaded = 0
        batches = (total_claims + batch_size - 1) // batch_size  # Ceiling division

        for batch_num in range(batches):
            current_batch_size = min(batch_size, total_claims - total_loaded)

            logger.info(
                f"Processing batch {batch_num + 1}/{batches} ({current_batch_size} claims)"
            )

            # Generate batch
            claims_batch = generator.generate_batch(current_batch_size)

            # Load batch
            loaded = await loader.load_claims_batch(claims_batch)
            total_loaded += loaded

            logger.info(
                f"Batch {batch_num + 1} completed. Total loaded: {total_loaded:,}/{total_claims:,}"
            )

        logger.info(
            f"Successfully generated and loaded {total_loaded:,} sample claims!"
        )

        # Update batch metadata
        await db.execute(
            """
            UPDATE batch_metadata 
            SET completed_at = $1, processed_claims = $2, valid_claims = $3
            WHERE batch_id = $4
        """,
            datetime.now(),
            total_loaded,
            total_loaded,
            batch_id,
        )

    except Exception as e:
        logger.error(f"Error during data generation: {e}")
        raise
    finally:
        await db.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate sample claims data")
    parser.add_argument(
        "--claims",
        type=int,
        default=100000,
        help="Number of claims to generate (default: 100000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for database operations (default: 1000)",
    )

    args = parser.parse_args()

    # Run the generator
    asyncio.run(generate_and_load_sample_data(args.claims, args.batch_size))