import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from .validators import (validate_dob, validate_facility,
                         validate_financial_class, validate_line_item_dates,
                         validate_service_dates)


class ClaimValidator:
    def __init__(self, valid_facilities: set[str], valid_financial_classes: set[str]):
        self.valid_facilities = valid_facilities
        self.valid_financial_classes = valid_financial_classes

    async def validate(self, claim: Dict[str, Any]) -> List[str]:
        """Validate a claim using multiple rules concurrently."""
        results = await asyncio.gather(
            validate_facility(claim, self.valid_facilities),
            validate_financial_class(claim, self.valid_financial_classes),
            validate_dob(claim),
            validate_service_dates(claim),
            validate_line_item_dates(claim),
        )
        errors: List[str] = []
        for r in results:
            errors += r
        return errors

    async def validate_batch(
        self, claims: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:
        """Enhanced vectorized batch validation with optimizations."""
        if not claims:
            return {}

        # For large batches, use parallel processing
        if len(claims) > 2000:
            return await self.validate_batch_parallel(claims)

        # Pre-process claims for vectorized validation
        validation_data = self._preprocess_claims_for_validation(claims)

        # Vectorized validation operations
        results = await self._validate_batch_vectorized(claims, validation_data)

        return results

    async def validate_batch_parallel(
        self, claims: List[Dict[str, Any]], max_workers: int = 4
    ) -> Dict[str, List[str]]:
        """Parallel batch validation using thread pool for CPU-intensive operations."""
        if not claims:
            return {}

        # Split claims into chunks
        chunk_size = max(len(claims) // max_workers, 100)
        claim_chunks = [
            claims[i : i + chunk_size] for i in range(0, len(claims), chunk_size)
        ]

        async def process_chunk_async(
            chunk: List[Dict[str, Any]]
        ) -> Dict[str, List[str]]:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._process_chunk_sync, chunk)

        # Process chunks in parallel
        chunk_results = await asyncio.gather(
            *[process_chunk_async(chunk) for chunk in claim_chunks]
        )

        # Merge results
        results: Dict[str, List[str]] = {}
        for chunk_result in chunk_results:
            results.update(chunk_result)

        return results

    def _process_chunk_sync(self, claims: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Synchronous processing of a chunk of claims."""
        results: Dict[str, List[str]] = {}

        for claim in claims:
            claim_id = claim.get("claim_id", str(id(claim)))
            errors = []

            # Validate facility
            if claim.get("facility_id") not in self.valid_facilities:
                errors.append("invalid_facility")

            # Validate financial class
            if claim.get("financial_class") not in self.valid_financial_classes:
                errors.append("invalid_financial_class")

            # Validate date of birth
            dob = claim.get("date_of_birth")
            service_from = claim.get("service_from_date")
            if dob and service_from and dob > service_from:
                errors.append("invalid_dob")

            # Validate service dates
            start = claim.get("service_from_date")
            end = claim.get("service_to_date")
            if start and end and start > end:
                errors.append("invalid_service_dates")

            # Validate line item dates
            if start and end:
                for item in claim.get("line_items", []):
                    item_start = item.get("service_from_date")
                    item_end = item.get("service_to_date")
                    if item_start and item_start < start:
                        errors.append("line_item_date_out_of_range")
                        break
                    if item_end and item_end > end:
                        errors.append("line_item_date_out_of_range")
                        break

            results[claim_id] = errors

        return results

    def _preprocess_claims_for_validation(
        self, claims: List[Dict[str, Any]]
    ) -> Dict[str, List[Any]]:
        """Extract and preprocess claim data for vectorized validation."""
        data = {
            "facility_ids": [claim.get("facility_id") for claim in claims],
            "financial_classes": [claim.get("financial_class") for claim in claims],
            "dobs": [claim.get("date_of_birth") for claim in claims],
            "service_from_dates": [claim.get("service_from_date") for claim in claims],
            "service_to_dates": [claim.get("service_to_date") for claim in claims],
            "line_items": [claim.get("line_items", []) for claim in claims],
            "claim_ids": [
                claim.get("claim_id", str(i)) for i, claim in enumerate(claims)
            ],
        }
        return data

    async def _validate_batch_vectorized(
        self, claims: List[Dict[str, Any]], validation_data: Dict[str, List[Any]]
    ) -> Dict[str, List[str]]:
        """Perform vectorized validation operations."""
        results: Dict[str, List[str]] = {}

        # Initialize results
        for claim_id in validation_data["claim_ids"]:
            results[claim_id] = []

        # Vectorized facility validation
        valid_facilities_set = self.valid_facilities
        facility_errors = [
            "invalid_facility" if fid not in valid_facilities_set else None
            for fid in validation_data["facility_ids"]
        ]

        # Vectorized financial class validation
        valid_classes_set = self.valid_financial_classes
        class_errors = [
            "invalid_financial_class" if fc not in valid_classes_set else None
            for fc in validation_data["financial_classes"]
        ]

        # Vectorized date validation
        dob_errors = [
            "invalid_dob" if (dob and service_from and dob > service_from) else None
            for dob, service_from in zip(
                validation_data["dobs"], validation_data["service_from_dates"]
            )
        ]

        # Vectorized service date validation
        service_date_errors = [
            "invalid_service_dates" if (start and end and start > end) else None
            for start, end in zip(
                validation_data["service_from_dates"],
                validation_data["service_to_dates"],
            )
        ]

        # Line item validation (more complex, done individually but optimized)
        line_item_errors = []
        for i, line_items in enumerate(validation_data["line_items"]):
            start = validation_data["service_from_dates"][i]
            end = validation_data["service_to_dates"][i]
            error = None

            if start and end:
                for item in line_items:
                    item_start = item.get("service_from_date")
                    item_end = item.get("service_to_date")
                    if (item_start and item_start < start) or (
                        item_end and item_end > end
                    ):
                        error = "line_item_date_out_of_range"
                        break

            line_item_errors.append(error)

        # Aggregate errors for each claim
        error_lists = [
            facility_errors,
            class_errors,
            dob_errors,
            service_date_errors,
            line_item_errors,
        ]

        for i, claim_id in enumerate(validation_data["claim_ids"]):
            claim_errors = []
            for error_list in error_lists:
                if i < len(error_list) and error_list[i]:
                    claim_errors.append(error_list[i])
            results[claim_id] = claim_errors

        return results

    async def validate_batch_streaming(
        self, claims: List[Dict[str, Any]], chunk_size: int = 1000
    ) -> Dict[str, List[str]]:
        """Stream processing of large batches to manage memory usage."""
        results: Dict[str, List[str]] = {}

        # Process claims in smaller chunks
        for i in range(0, len(claims), chunk_size):
            chunk = claims[i : i + chunk_size]
            chunk_results = await self.validate_batch(chunk)
            results.update(chunk_results)

        return results

    def get_validation_statistics(self) -> Dict[str, Any]:
        """Get validation statistics."""
        return {
            "valid_facilities_count": len(self.valid_facilities),
            "valid_financial_classes_count": len(self.valid_financial_classes),
        }

    def update_validation_sets(
        self, facilities: set[str], financial_classes: set[str]
    ) -> None:
        """Update validation sets with new data."""
        self.valid_facilities = facilities
        self.valid_financial_classes = financial_classes
