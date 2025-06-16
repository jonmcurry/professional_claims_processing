# Sample Data Generation Guide

This guide explains how to generate 100,000 sample healthcare claims and load them into your PostgreSQL staging database for testing your claims processing system.

## Overview

The sample data generator creates realistic healthcare claims that include:
- **Claims data**: Patient demographics, service dates, facility information, financial class
- **Line items**: Procedure codes, charges, units, modifiers, RVU values
- **Diagnosis codes**: ICD-10 diagnosis codes with descriptions
- **Reference data**: Facilities, financial classes, RVU lookup data

## Prerequisites

1. **Database Setup**: Ensure both PostgreSQL and SQL Server databases are set up according to your schema
2. **Configuration**: Your `config.yaml` should have valid database connection details
3. **Dependencies**: All Python dependencies should be installed (`pip install -r requirements.txt`)

## Step 1: Setup Reference Data

Before generating claims, you need to create the reference data that claims will validate against:

```bash
# Run the reference data setup script
python scripts/setup_reference_data.py
```

This script creates and populates:
- **50 facilities** (FAC001-FAC050) in both PostgreSQL and SQL Server
- **8 financial classes** (COM, MED, MCR, PPO, HMO, WRK, OTH, SELF)
- **RVU data** for common procedure codes in SQL Server
- **Tables**: `facilities`, `financial_classes`, `rvu_data`

## Step 2: Generate Sample Claims

Run the sample data generator to create and load 100,000 claims:

```bash
# Generate 100,000 claims (default)
python scripts/generate_sample_data.py

# Or specify custom parameters
python scripts/generate_sample_data.py --claims 50000 --batch-size 500
```

### Parameters

- `--claims`: Number of claims to generate (default: 100,000)
- `--batch-size`: Batch size for database operations (default: 1,000)

### What Gets Generated

**Claims Distribution:**
- **Facilities**: Random distribution across 50 facilities
- **Financial Classes**: Weighted toward common insurance types
- **Service Dates**: Within last 2 years
- **Line Items**: 1-5 items per claim (weighted toward 1-2 items)
- **Diagnosis Codes**: 1-4 diagnoses per claim
- **Patient Demographics**: Realistic names, ages 18-95, gender distribution

**Realistic Data Patterns:**
- 80% single-day claims, 20% multi-day
- Procedure codes from common CPT codes (office visits, labs, radiology, surgery)
- Diagnosis codes from common ICD-10 codes
- Proper charge amounts and RVU values
- Valid modifiers and place of service codes

## Performance

The generator processes claims in batches using PostgreSQL's COPY command for optimal performance:

- **Expected Runtime**: ~15-30 minutes for 100,000 claims
- **Database Load**: Uses bulk operations to minimize database overhead
- **Memory Usage**: Processes in configurable batches to control memory
- **Logging**: Detailed progress logging every batch

## Generated Data Structure

### Claims Table
```sql
-- Each claim includes:
claim_id, facility_id, patient_account_number, patient_name,
date_of_birth, gender, service_from_date, service_to_date,
primary_diagnosis, financial_class, total_charge_amount,
processing_status, priority, etc.
```

### Claims Line Items
```sql
-- Each line item includes:
claim_id, line_number, procedure_code, units, charge_amount,
service_from_date, service_to_date, rvu_value,
reimbursement_amount, modifiers, etc.
```

### Claims Diagnosis Codes
```sql
-- Each diagnosis includes:
claim_id, diagnosis_sequence, diagnosis_code,
diagnosis_description, diagnosis_type
```

## Validation Ready

The generated claims are designed to **pass your validation rules**:

✅ **Facility ID exists** in facilities table  
✅ **Patient Account Numbers** are unique and valid  
✅ **Service dates** are properly formatted and logical  
✅ **Financial classes** exist in lookup table  
✅ **Date of birth** is valid (18-95 years old)  
✅ **Line item dates** fall within claim date range  

## Batch Tracking

The generator creates a batch metadata record for tracking:
- **Batch ID**: Format `GEN_YYYYMMDD_HHMMSS`
- **Status**: Tracks completion and counts
- **Metadata**: Source information and processing options

## File Locations

Save these scripts in your project:

```
scripts/
├── generate_sample_data.py      # Main data generator
├── setup_reference_data.py      # Reference data setup
└── README_sample_data.md        # This guide
```

## Testing Your Pipeline

After generating the data, you can test your claims processing pipeline:

```bash
# Run your main processing pipeline
python -m src.processing.main

# Check processing results
python -c "
from src.db.postgres import PostgresDatabase
from src.config.config import load_config
import asyncio

async def check_results():
    config = load_config()
    db = PostgresDatabase(config.postgres)
    await db.connect()
    
    # Check claim counts
    result = await db.fetch('SELECT processing_status, COUNT(*) as count FROM claims GROUP BY processing_status')
    for row in result:
        print(f'{row[\"processing_status\"]}: {row[\"count\"]:,} claims')
    
    await db.close()

asyncio.run(check_results())
"
```

## Troubleshooting

**Connection Errors**: Verify your `config.yaml` database settings

**Performance Issues**: Reduce batch size if you encounter memory issues

**Validation Failures**: Check that reference data setup completed successfully

**Missing Dependencies**: Ensure `faker` package is installed: `pip install faker`

## Cleanup

To remove generated data:

```sql
-- PostgreSQL cleanup
DELETE FROM claims_diagnosis_codes WHERE claim_id LIKE 'CLM%';
DELETE FROM claims_line_items WHERE claim_id LIKE 'CLM%';
DELETE FROM claims WHERE claim_id LIKE 'CLM%';
DELETE FROM batch_metadata WHERE batch_id LIKE 'GEN_%';
```

This sample data provides a robust foundation for testing your high-performance claims processing system with realistic healthcare data at scale.