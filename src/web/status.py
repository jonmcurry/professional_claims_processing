from typing import Dict, Any, Optional

processing_status: Dict[str, int] = {"processed": 0, "failed": 0}

# Details about the current batch being processed
batch_status: Dict[str, Optional[Any]] = {
    "batch_id": None,
    "start_time": None,
    "end_time": None,
    "total": 0,
}

