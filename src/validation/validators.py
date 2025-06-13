from typing import Any, Dict, List, Set


def validate_facility(claim: Dict[str, Any], valid_facilities: Set[str]) -> List[str]:
    if claim.get("facility_id") not in valid_facilities:
        return ["invalid_facility"]
    return []


def validate_financial_class(claim: Dict[str, Any], valid_classes: Set[str]) -> List[str]:
    if claim.get("financial_class") not in valid_classes:
        return ["invalid_financial_class"]
    return []


def validate_dob(claim: Dict[str, Any]) -> List[str]:
    dob = claim.get("date_of_birth")
    service_from = claim.get("service_from_date")
    if dob and service_from and dob > service_from:
        return ["invalid_dob"]
    return []


def validate_service_dates(claim: Dict[str, Any]) -> List[str]:
    start = claim.get("service_from_date")
    end = claim.get("service_to_date")
    if start and end and start > end:
        return ["invalid_service_dates"]
    return []
